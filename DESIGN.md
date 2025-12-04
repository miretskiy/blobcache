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
   - Rebuilt periodically from index (configurable: 5-10min or on FP rate threshold) (40 ms for 4M keys)
   - Stored in DuckDB (not separate files)

2. **DuckDB Index (Analytical)**
   - Key → (segment_id, offset, size, ctime)
   - Exceptional query performance (0.1-0.5s for FIFO scans)
   - ACID transactions, crash recovery via WAL
   - Zero-alloc Get() API (caller-provided Entry)

3. **Segment-Based Blob Storage (NVMe-Optimized)**
   - Large segment files (1GB - 10GB each)
   - Multiple blobs packed sequentially in each segment
   - Designed for high-performance NVMe (1+ GB/s)
   - Batched writes reduce syscall overhead

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
├── db/
│   ├── index.duckdb          # DuckDB index + bloom filter storage
│   └── wal/                  # DuckDB WAL (crash recovery)
└── segments/
    ├── 1733183456789123456-00.seg    # timestamp-workerID.seg
    ├── 1733183456812456789-01.seg    # Multiple blobs per segment
    ├── 1733183456834567890-02.seg    # Large files (1GB - 10GB each)
    └── ...                           # Hundreds to thousands of files
                                      # Flat directory (no sharding needed)
```

**Note:** Bloom filter stored IN DuckDB (as BLOB column), not separate files.

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

## Database Schema

```sql
CREATE TABLE entries (
    key BLOB PRIMARY KEY,
    segment_id BIGINT NOT NULL,   -- Which segment file
    offset BIGINT NOT NULL,       -- Byte offset in segment
    size INTEGER NOT NULL,        -- Blob size in bytes
    ctime BIGINT NOT NULL         -- Creation time (for FIFO)
);

CREATE INDEX idx_segment_ctime ON entries(segment_id, ctime);
CREATE INDEX idx_ctime ON entries(ctime);  -- For FIFO eviction

-- Bloom filter storage (single global filter)
CREATE TABLE bloom_filter (
    version INTEGER PRIMARY KEY,  -- For versioning
    data BLOB NOT NULL,           -- Serialized bloom filter
    created_at BIGINT NOT NULL
);

-- Segment metadata (optional, for stats)
CREATE TABLE segments (
    segment_id BIGINT PRIMARY KEY,
    created_at BIGINT NOT NULL,
    size BIGINT NOT NULL,         -- Total bytes in segment
    blob_count INTEGER NOT NULL   -- Number of blobs
);
```

**Why DuckDB:**
- Embedded (no separate process)
- Exceptional analytical performance:
  - Scans 4M rows in ~40 ms
  - `SELECT segment_id, SUM(size) ... GROUP BY segment_id ORDER BY MIN(ctime)` in 0.1-0.5s
  - Perfect for segment-level FIFO eviction
  - Batch inserts via Appender API (no primary key violations)
- ACID transactions
- Built-in WAL (crash recovery)
- Can store bloom filter (BLOB column)

**File mapping:** `segments/{timestamp}-{worker_id}.seg`
- segment_id: `{unix_nanos}-{worker_id}` (e.g., `1733183456789123456-03`)
- timestamp: Matches ctime in DuckDB (easy debugging/correlation)
- worker_id: I/O worker number (0-5 for 6 concurrent writers)
- offset: Byte position within segment file
- size: Length of blob data

**No directory scan on restart:** timestamp-based IDs eliminate need to find max ID

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
// Required argument (not optional)
func NewBlobCache(path string, opts ...Option) (*BlobCache, error)

// Core operations
Put(ctx context.Context, key, value []byte) error
Get(ctx context.Context, key []byte) ([]byte, error)
Delete(ctx context.Context, key []byte) error
Close() error
Drain() error  // Flush write buffer before close

// Options (functional pattern)
WithMaxSize(int64) Option
WithSegmentSize(int64) Option  // Default: 1GB
WithChecksums(bool) Option     // Default: true
WithFsync(bool) Option          // Default: false
WithVerifyOnRead(bool) Option  // Default: false
```

**Defaults:**
- MaxSize: 80% of disk capacity (auto-detected)
- SegmentSize: 1GB (like RocksDB memtable)
- Eviction: Segment-level FIFO (by creation time)
- Checksums: Enabled (detect corruption)
- Fsync: Disabled (cache semantics, fast)
- VerifyOnRead: Disabled (opt-in)

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

**Completed (~40% done, 4-5 hours):**
- ✅ Design doc
- ✅ Options pattern
- ✅ Key (hash-based deterministic fileID)
- ✅ Index layer (DuckDB, zero-alloc, tested)
- ✅ Bloom filter (lock-free, 4× faster than alternatives, benchmarked)

**Next (Segment-based rewrite, ~2-3 weeks):**
- [ ] Phase 1: Write buffer and segment writer
- [ ] Phase 2: Segment reader with file handle caching
- [ ] Phase 3: Segment-level FIFO eviction
- [ ] Phase 4: Update benchmarks for higher hit rate
- [ ] End-to-end tests
- [ ] CPU profiling comparison

**Final (~1 week):**
- [ ] Metrics integration
- [ ] Stress tests (4M keys, 1 TB)
- [ ] Production validation plan
- [ ] Performance comparison with RocksDB

**Total estimate: 3-4 weeks**

---

## Decision Log

**Key decisions:**
1. ✅ Segment-based storage (multiple blobs per file)
2. ✅ 1GB segments (like RocksDB memtable size)
3. ✅ Lock-free bloom (atomic CAS, not mutex)
4. ✅ Bloom stored in DuckDB (not separate files)
5. ✅ Segment-level FIFO eviction (no LRU)
6. ✅ DB-first deletion order (then filesystem)
7. ✅ Daily orphan cleanup (not on startup)
8. ✅ Zero-alloc Get() API (caller-provided Entry)
9. ✅ xxHash (7 GB/s, proven)
10. ✅ pread() for concurrent segment reads

**Pivoted from one-blob-per-file:**
- ❌ Initial design: one file per blob
- ❌ Problem: 70% CPU in syscalls (os.WriteFile overhead)
- ✅ Solution: Batch blobs into segments (99.9% fewer syscalls)

**Out of scope:**
- ❌ Range queries
- ❌ Sorted iteration
- ❌ Compression (caller-controlled)
- ❌ Compaction (FIFO only)
- ❌ Per-blob eviction (segment-level only)
- ❌ LRU tracking (segment FIFO only)

**Design validated through:**
- Production RocksDB analysis (455 L0 files, 1M keys, 450 GB)
- Benchmark comparison: Blobcache 29% faster than RocksDB
- CPU profiling: 70% time in syscalls led to segment design
- Bloom filter benchmarks (4× faster than alternatives)

---

## Benchmark Results

### RocksDB vs Blobcache Comparison (2025-12-01)

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

**Key finding:** Blobcache was faster overall, but 70% CPU in syscalls indicates room for improvement.

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
