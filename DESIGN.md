# BlobCache Design Document

## Motivation

### Production Problem Analysis

**Service:** logs-event-store-reader (logs-storage namespace)
**RocksDB Config:** FIFO compaction, 960 MB memtables, 455 L0 files at peak
**Fleet Cost:** 5,200 cores for 10B queries/day across ~10K pods

**Root cause analysis:**
- 52% negative lookups (keys not found)
- Each negative: 455 bloom filter checks × 100ns = 45 µs CPU
- Fleet-wide: 5.2B negative lookups/day × 45 µs = 5,000 cores
- Write path: ~200 cores (memtable + compaction)

**Attempts to optimize RocksDB:**
- ❌ Block cache: Values too large (400 KB avg), no benefit
- ❌ Leveled compaction: 10-15× write amp exceeds disk capacity (1,048 MB/s NVMe)
- ❌ Universal compaction: 5-8× write amp still exceeds disk
- ❌ Aggressive intra-L0: Creates larger files that never compact again (worse read amp)
- ✅ RocksDB FIFO is already optimal for these constraints

**Conclusion:** Need different architecture, not RocksDB tuning.

---

## Design Goals

**Target: 75% fleet CPU reduction** (5,200 → 1,300 cores)

Primary optimization: Single bloom filter check (1 ns) vs 455 checks (45 µs)

**What it is:** High-performance blob cache with bloom filter optimization
**What it is NOT:** General-purpose storage engine

---

## High-Level Design

### Core Architecture

**Three components working together:**

1. **Global Bloom Filter (Lock-Free)**
   - Single filter for all keys (not per-file)
   - Lock-free atomic operations (1 ns reads, 12 ns writes)
   - Rebuilt periodically from index (configurable intervals)
   - Persisted to bloom.dat file

2. **Bitcask Index (Default) or Skipmap (Fast)**
   - Key → Record{SegmentID, Pos, Size, CTime, Checksum, HasChecksum}
   - Bitcask: Embedded persistent storage with in-memory hash table
   - Skipmap: Pure in-memory (fastest) or durable Skipmap backed by Bitcask
   - Scan() method for flexible iteration (bloom rebuild, eviction, size tracking)

3. **Segment-Based Blob Storage (NVMe-Optimized)**
   - Large segment files (2GB default)
   - Multiple blobs packed sequentially in each segment
   - Designed for high-performance NVMe (1+ GB/s sustained)
   - Batched writes via MemTable reduce syscall overhead
   - DirectIO support for bypassing OS cache

**Data flow:**

```
Put:  Add to write buffer → When full, flush segment → Batch update index & bloom
Get:  Bloom check (1 ns) → Query index for segment+offset → pread() from segment
```

### Design Decisions Optimized for Hot Path

**Prioritize read speed:**
- ✅ Lock-free bloom (no mutex contention on reads)
- ✅ Single bloom check (not 455 like RocksDB)
- ✅ pread() for concurrent reads from segments (no locks)
- ✅ Zero-alloc Get() (caller-provided Entry)

**Accept trade-offs for simplicity:**
- ✅ Deletes don't update bloom (accept FPs, rebuild periodically)
- ✅ Segment-level eviction (evict entire segments, not individual blobs)
- ✅ No LRU tracking (simple FIFO by segment creation time)
- ✅ Daily orphan cleanup (not on startup)

**Hardware assumptions:**
- High-performance NVMe (1+ GB/s write, <1 ms latency)
- Sequential writes perform better than random small files
- Batching reduces syscall overhead


## Architecture

### Directory Structure

```
/data/blobcache/
├── bitcask-index/            # Bitcask index files (default)
│   ├── datafile              # Key-value pairs
│   └── hint                  # Index file for fast startup
├── bloom.dat                 # Serialized bloom filter
└── segments/
    ├── 1733183456789123456-00.seg    # timestamp-workerID.seg
    ├── 1733183456812456789-01.seg    # Multiple blobs per segment
    ├── 1733183456834567890-02.seg    # Large files (2GB default)
    └── ...                           # Flat directory (no sharding needed)
```

**Index options:**
- Bitcask (default): Persistent embedded storage
- Skipmap (fast): In-memory only or backed by Bitcask

### Why Segment-Based Storage?

**Benchmark Analysis (2025-12-01):**
- Initial implementation: one blob per file
- CPU profiling showed `os.WriteFile` consuming 64-70% CPU time
- This is syscall overhead (open/write/close for each blob)
- 256K blobs = 768K syscalls (3× per file)

**Segment-based solution:**
- Batch 1,000-10,000 blobs per segment file
- Reduces file count by 1000× (256K files → 256 segments)
- Reduces syscalls by 99.9% (768K → 768 syscalls)
- Sequential writes (better I/O performance)

---

## Index Storage

**Bitcask encoding (default index):**
```
Key: raw key bytes
Value: SegmentID(8) + Pos(8) + Size(4) + CTime(8) + Flags(1) + [Checksum(4) if HasChecksum]
       = 29 bytes (no checksum) or 33 bytes (with checksum)
```

**Flags byte:**
- Bit 0: HasChecksum (checksum field valid)
- Bits 1-7: Reserved for future use

**Record structure:**
```go
type Record struct {
    Key         []byte // Raw key bytes
    SegmentID   int64  // Segment ID (timestamp-workerID)
    Pos         int64  // Byte offset within segment
    Size        int    // Blob size in bytes
    CTime       int64  // Creation time (for FIFO eviction)
    Checksum    uint32 // CRC32 checksum (if HasChecksum=true)
    HasChecksum bool   // True if checksum field is valid
}
```

**Indexer interface (minimal):**
- Put(key, record) - Insert/update record
- Get(key, record) - Lookup record
- Delete(key) - Remove record
- Scan(fn) - Iterate all records (for bloom rebuild, eviction, size tracking)
- PutBatch(records) - Atomic batch insert
- Close() - Shutdown

**File mapping:** `segments/{timestamp}-{worker_id}.seg`
- SegmentID: `{unix_nanos}-{worker_id}` (e.g., `1733183456789123456-03`)
- Pos: Byte offset within segment file
- Size: Length of blob data
- Checksum: Optional CRC32 for data integrity

**No directory scan on restart:** Timestamp-based IDs eliminate need to find max ID

---

## Bloom Filter Implementation

**Lock-free design** (NOT mutex-based):
```go
type Filter struct {
    data []uint32  // Bit vector
}

// Lock-free read (atomic.Load)
func Test(key) bool {
    word := atomic.LoadUint32(&data[index])
    return word & mask != 0
}

// Lock-free write (atomic.CompareAndSwap)
func Add(key) {
    for {
        orig := atomic.LoadUint32(&data[index])
        if atomic.CompareAndSwapUint32(&data[index], orig, orig|mask) {
            break
        }
    }
}
```

**Performance (benchmarked):**
- Test: 1 ns/op (1000× faster than RocksDB's 100 ns!)
- Add: 12 ns/op
- Concurrent test: 0.95 ns/op (perfect scaling)
- **4× faster than bits-and-blooms library**

**Combines:**
- fastbloom: Atomic operations for lock-free
- RocksDB: Golden ratio probing (h *= 0x9e3779b9)
- xxHash: 7 GB/s hashing

**Storage:** Serialized and stored in DuckDB bloom_filter table (not separate files)

**Rebuild:** Query all keys from DuckDB (40 ms for 4M keys), build new filter, atomic swap

---

## API Design

```go
// Create cache with functional options
func New(path string, opts ...Option) (*Cache, error)

// Start background workers (eviction, bloom refresh)
func (c *Cache) Start() (closer func() error, error)

// Core operations
Put(key, value []byte)                                    // Safe (copies data)
UnsafePut(key, value []byte)                             // Zero-copy (caller owns)
PutChecksummed(key, value []byte, checksum uint32)       // With explicit checksum
UnsafePutChecksummed(key, value []byte, checksum uint32) // Zero-copy with checksum
Get(key []byte) (io.Reader, bool)                        // Returns reader, not []byte
Close() error
Drain()  // Wait for memtable flush

// Options (functional pattern)
WithMaxSize(int64) Option
WithSegmentSize(int64) Option       // Default: 0 (one blob per file), use 2GB for segments
WithChecksum() Option               // Enable CRC32 checksums
WithChecksumHash(func() hash.Hash32) Option  // Custom hash (e.g., xxhash)
WithVerifyOnRead(bool) Option       // Enable checksum verification on reads
WithFsync(bool) Option              // Enable fdatasync (default: false)
WithDirectIOWrites() Option         // Use DirectIO for writes
WithSkipmapIndex() Option          // Use in-memory skipmap index
WithWriteBufferSize(int64) Option  // Memtable size (default: 100MB)
WithMaxInflightBatches(int) Option // Concurrent flush workers (default: 6)
```

**Defaults:**
- MaxSize: 0 (unlimited, set explicitly for production)
- SegmentSize: 0 (per-blob mode, use 2GB for production)
- Checksums: Disabled (opt-in via WithChecksum)
- Fsync: Disabled (cache semantics, uses fdatasync when enabled)
- VerifyOnRead: Disabled (opt-in)
- DirectIO: Disabled (opt-in for production)
- WriteBufferSize: 100MB (use 1GB for production)

---

## Write Path

```go
// Concurrency model: 1 batcher + N I/O workers (N = MaxInflightBatches, default 6)
type MemTable struct {
    entries chan Entry           // Batcher accumulates entries
    batches chan []Entry         // I/O workers process batches in parallel
}

Put(key, value):
1. Enqueue to memtable (non-blocking)
   - Batcher goroutine accumulates into batches
   - When batch reaches maxSize (1GB), sends to I/O worker

2. I/O worker processes batch:
   a. Generate segment ID: {unix_nanos}-{worker_id}
   b. Create segment file: segments/{segment_id}.seg
   c. Write all blobs sequentially to segment
   d. Batch insert to DuckDB (Appender API)
   e. Batch update bloom filter

3. Multiple I/O workers run concurrently (6 by default)
   - Each worker generates its own timestamp-based segment IDs
   - No coordination needed between workers
```

**Batching benefits:**
- 1,000-10,000 blobs per segment
- 1 file open/close per segment (not per blob)
- Sequential writes (better disk performance)
- Batch DuckDB inserts (avoid primary key violations)

**Crash scenarios:**
- Buffer not flushed: Data lost (acceptable for cache)
- During segment write: Partial segment (detected on startup, deleted)
- After segment, before DB: Orphaned segment (cleaned daily)
- After DB: Complete ✓

---

## Read Path

```go
Get(key):
1. Bloom filter check (~1 ns, lock-free load)
   → If no: return ErrNotFound (fast rejection!)

2. Query DuckDB index (~50 µs)
   SELECT segment_id, offset, size FROM entries WHERE key = ?
   → If not found: return ErrNotFound (bloom FP)

3. Open segment file (or use cached file handle)

4. pread(segment_file, offset, size) (~2-3 ms for 400 KB)
   - pread() is thread-safe (no locking)
   - Can read from same segment concurrently

5. Verify checksum if opted in
```

**Performance:**
- Negative: ~1 ns (vs RocksDB's 45 µs) - **45,000× faster**
- Positive: ~2-3 ms (same as RocksDB, disk I/O limited)

**File handle caching:**
- Keep N most recent segment files open (e.g., 100 handles)
- LRU eviction of file handles
- Reduces open() syscalls on hot segments

---

## Eviction & Cleanup

### Eviction (Background Worker)

**Segment-Level FIFO** - simpler than per-blob eviction:

```go
evictIfNeeded():
1. Query DuckDB for total size (fast aggregate)
   SELECT SUM(size) FROM entries

2. If over limit, query oldest segments:
   SELECT segment_id, SUM(size) as segment_size
   FROM entries
   GROUP BY segment_id
   ORDER BY MIN(ctime) ASC

3. Delete entire segments:
   a. DELETE FROM entries WHERE segment_id IN (...)
   b. rm segments/{segment_id}.seg
   c. Bloom filter: NOT updated (accept FPs, rebuild periodically)

4. Repeat until under size limit
```

**Advantages:**
- ✅ Simple FIFO by segment creation time
- ✅ One file delete per segment (not per blob)
- ✅ Batch DuckDB deletes (efficient)
- ✅ No LRU complexity (no access time tracking needed)

**Trade-offs:**
- ❌ Cannot evict individual blobs
- ❌ May evict some newer blobs in old segments
- ✅ Acceptable for cache semantics (simplicity > precision)

**Order:** DB first, then filesystem
- Get() returns ErrNotFound immediately after DB delete
- Orphaned segments if crash (cleaned daily)
- Acceptable for cache semantics

### Orphan Cleanup (Daily, Not Startup)

Runs in background once per day:
- Compare filesystem to DB
- Delete orphaned segments
- Verify segment integrity (check file sizes)

**Startup is fast (<100 ms)** - no filesystem scan!

---

## Performance Projections

**Conservative estimate: 75% fleet reduction**

| Component | RocksDB | BlobCache | Reduction |
|-----------|---------|-----------|-----------|
| Reads (negative) | 3,000 cores | 60 cores | 98% |
| Reads (positive) | 2,000 cores | 1,000 cores | 50% |
| Writes | 200 cores | 240 cores | -20% (worse) |
| **Total** | **5,200 cores** | **1,300 cores** | **75%** |

**Why conservative:**
- Index queries may be slower than expected
- Filesystem overhead may be higher
- DuckDB insert latency TBD

**Realistic target: 3,900 cores saved**

---

## Implementation Status

**✅ Completed (Production-Ready):**

**Core Features:**
- ✅ Segment-based storage with DirectIO support
- ✅ MemTable with async flush workers (RocksDB-style batching)
- ✅ Lock-free bloom filter (4× faster than alternatives)
- ✅ Bitcask index (default) + Skipmap index (optional)
- ✅ Streaming checksum verification (hash.Hash32 interface)
- ✅ Storage strategies for eviction (BlobStorage, SegmentStorage)
- ✅ Reactive size tracking with eviction triggers
- ✅ Centralized path generation (BlobPaths, SegmentPaths)
- ✅ Platform-optimized fdatasync (Linux: fdatasync, Darwin: F_FULLFSYNC)

**API:**
- ✅ Clean interface: Get(key) → Reader, Put(key, value)
- ✅ PutChecksummed for explicit checksums
- ✅ WithChecksum(), WithChecksumHash() for data integrity
- ✅ WithSegmentSize(), WithDirectIOWrites(), etc.

**Testing:**
- ✅ Comprehensive unit tests (all packages)
- ✅ Integration tests (segment mode, DirectIO, skipmap)
- ✅ Checksum reader tests (9 edge cases including 1-byte reads)
- ✅ Production benchmark vs RocksDB on Graviton

**Performance validated:**
- ✅ 2.77× faster than RocksDB (1.04 vs 0.38 GB/s)
- ✅ 45× less memory (2.2GB vs 100GB)
- ✅ 3.75× less CPU (2 cores vs 7.5 cores)
- ✅ No compaction overhead (append-only architecture)

---

## Decision Log

**Major decisions:**
1. ✅ Segment-based storage (multiple blobs per file) - 99.9% fewer syscalls
2. ✅ 2GB segments (optimal for NVMe throughput)
3. ✅ Lock-free bloom (atomic CAS, not mutex) - 4× faster
4. ✅ Bitcask index (removed DuckDB) - simpler, sufficient performance
5. ✅ Skipmap option (in-memory + optional Bitcask backing) - fastest reads
6. ✅ Segment-level FIFO eviction via storage strategies
7. ✅ Streaming checksums (hash.Hash32) - error on final Read(), not Close
8. ✅ Get() returns io.Reader (not []byte) - caller controls reading
9. ✅ Storage strategies (BlobStorage, SegmentStorage) - clean architecture
10. ✅ Centralized paths (BlobPaths, SegmentPaths) - zero-allocation string types
11. ✅ Fdatasync (not fsync) - 2-5× faster, sufficient for immutable blobs
12. ✅ Record.HasChecksum flag - mid-lifecycle restart safe

**Architecture evolution:**
- ❌ DuckDB → ✅ Bitcask/Skipmap (simpler, removed dependency)
- ❌ One blob per file → ✅ Segment-based (70% CPU in syscalls → <20%)
- ❌ Entry type → ✅ Record type (single consistent naming)
- ❌ Scatter path generation → ✅ Centralized in paths.go
- ❌ Eviction in Indexer → ✅ Storage strategies (clean boundaries)

**Checksum design:**
- ✅ hash.Hash32 interface (stdlib compatibility)
- ✅ Streaming verification (incremental hashing as data flows)
- ✅ Error on final Read() (more resilient than Close errors)
- ✅ HasChecksum flag for backwards compatibility

**Out of scope:**
- ❌ Range queries
- ❌ Sorted iteration
- ❌ Compression (caller-controlled)
- ❌ Compaction (append-only architecture)
- ❌ Per-blob eviction (segment-level only)
- ❌ LRU tracking (FIFO only)

**Design validated through:**
- Production RocksDB analysis (455 L0 files causing 45µs CPU per negative lookup)
- Graviton benchmark: **2.77× faster, 45× less memory, 3.75× less CPU than RocksDB**
- CPU profiling: Identified and fixed syscall overhead
- Bloom filter: 4× faster than alternatives, lock-free design

---

## Benchmark Results

### Production Benchmark: AWS Graviton m7gd.8xlarge (2025-12-08)

**Hardware:**
- 32 vCPU (Graviton 3)
- 128 GB RAM
- 1.7TB NVMe instance storage
- Disk capacity: ~2GB/s read, ~1GB/s write (independent channels)

**Test configuration:**
- 2.56M operations (256K writes × 1MB = 256GB written, 2.3M reads)
- Mixed workload: 10% writes, 45% read hits, 45% read misses
- Matches production workload pattern
- RocksDB config: 1GB memtable, 6 inflight, FIFO compaction, DirectIO

| Metric | RocksDB v6.20.3 | Blobcache | Improvement |
|--------|-----------------|-----------|-------------|
| **Latency** | 259,316 ns/op | 93,448 ns/op | **2.77x faster** |
| **Write Throughput** | 0.376 GB/s | 1.042 GB/s | **2.77x faster** |
| **Duration** | 18+ minutes | 4.2 minutes | **4.3x faster** |
| **Memory** | 100.6 GB | 2.2 GB | **45x less** |
| **CPU** | 7.5 cores | 2 cores | **3.75x less** |
| **I/O Pattern** | Variable (compaction storms) | Steady | Predictable |
| **Disk Utilization** | 95-100% (variable) | 100% (saturated) | Consistent |

**RocksDB overhead observed:**
- Created 11GB compaction files (12×960MB → 1×11GB)
- Memory grew from 86GB → 100GB during run
- Massive kernel I/O merging (94% write merges, 61% read merges)
- Variable throughput (0.8-1.1 GB/s writes during compaction)

**Blobcache advantages:**
- ✅ **No compaction overhead** - all 1.04 GB/s is useful work
- ✅ **Predictable performance** - no compaction storms
- ✅ **Minimal resources** - 2 cores, 2GB RAM
- ✅ **Simple operations** - append-only, no background merges

### Early Benchmark: Apple M4 Max (2025-12-01)

**Test configuration:**
- 2.56M operations (256K writes, 2.3M reads)
- 1MB blobs
- 10% write, 90% read workload
- Apple M4 Max

| Metric | RocksDB | Blobcache (initial) | Difference |
|--------|---------|---------------------|------------|
| Duration | 71.5s | 55.4s | **29% faster** |
| Write Throughput | 3.50 GB/s | 4.51 GB/s | **29% faster** |
| CPU in syscalls | ~30% | **70%** | Problem identified |
| I/O Pattern | Large transactions (563-704 KB/t) | Small transactions (250-407 KB/t) | More overhead |
| File Count | ~455 SST files | 256K files | 1000× more files |

**Key finding:** Blobcache was faster overall, but 70% CPU in syscalls led to segment-based architecture.

### Segment-Based Architecture Benefits

**Expected improvements:**
- Syscall reduction: 768K → 768 (99.9% fewer)
- File count: 256K → 256 (1000× fewer)
- CPU in syscalls: 70% → 20-30%
- Maintain 29% speed advantage
- Sequential writes (better I/O)

**Comparison:**

| Feature | RocksDB | Blobcache (One-File) | Blobcache (Segment) |
|---------|---------|----------------------|---------------------|
| Bloom checks | 455 | 1 | 1 |
| Files created | ~455 | 256,000 | 256 |
| Syscall overhead | Low | High (70% CPU) | Low |
| Write batching | Yes | No | Yes |
| Eviction | FIFO by file | Per-blob | Per-segment |

**Best of both worlds:**
- ✅ Single bloom filter (45,000× CPU savings)
- ✅ Batched writes (like RocksDB)
- ✅ Low syscall overhead
- ✅ No compaction overhead
- ✅ Simpler than RocksDB
