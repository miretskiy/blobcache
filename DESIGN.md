# BlobCache Design Document

## Executive Summary

BlobCache is a high-performance blob caching system optimized for append-heavy workloads where RocksDB's compaction overhead becomes prohibitive. In production scenarios with high miss rates (52% negative lookups), RocksDB's FIFO compaction creates hundreds of SST files, forcing every lookup to cascade through multiple bloom filter checks - wasting significant CPU on the most common operation.

**Primary use case:** Local NVMe caching layer for large blobs (100KB-10MB) where unified bloom filters eliminate multi-file lookup cascades.

**Key innovations:**
1. **Unified bloom filter** - Single filter for all keys eliminates cascading checks across hundreds of SST files
2. **Immutable blob storage** - Write-once architecture eliminates compaction (all disk bandwidth available for ingestion)
3. **Streaming checksums** - Incremental verification as data flows, errors surfaced immediately on Read()

**Production benchmark (AWS Graviton m7gd.8xlarge, 256GB workload):**
- **2.77× faster** than RocksDB (1.04 GB/s vs 0.38 GB/s)
- **45× less memory** (2.2GB vs 100GB)
- **3.75× less CPU** (2 cores vs 7.5 cores)
- **No compaction storms** - predictable, consistent performance

**Trade-offs:** Brand new system (not battle-tested), segment mode has coarse-grained eviction (can't delete individual blobs within segments), FIFO only (no LRU), append-only (no updates).

---

## Motivation

### Production Problem

**Service:** logs-event-store-reader (Datadog logs storage infrastructure)

**Workload characteristics:**
- Large blob storage (100KB-10MB per blob, 900KB average)
- Append-heavy (write-once, read-many, evict-by-age)
- High miss rate (52% of requests for keys not in cache)

**RocksDB configuration (FIFO compaction):**
- 960MB memtables
- 6 memtables in-flight
- FIFO compaction with 2× write amplification
- Disabled block cache (values too large)
- 455 L0 SST files observed (800+ files at peak load)

### The Performance Problem

**Read amplification kills us:**

RocksDB's FIFO compaction trades low write amplification (2×) for **extremely high read amplification**. Each Get() checks bloom filters sequentially (all loaded in memory) until a match is found. Example with 455 files (busy node, peak is 800+):

```
Negative lookup (52% of requests):
  Must check ALL bloom filters (key not found)
  = 455 bloom filter checks × 100ns each
  = 45,500ns = 45µs CPU per request

Positive lookup (48% of requests):
  Statistical average: ~50% of files checked before match
  = 227 bloom filter checks × 100ns each
  = 22,700ns = 22µs CPU per request
```

**This is inherent to FIFO compaction.** The number varies (200-800+ files) based on ingestion rate and eviction timing. At peak (800+ files), negative lookups cost 80µs+. The fundamental problem: bloom filters must be checked sequentially, and for negative lookups (most common), every single file must be checked.

### Why RocksDB Features Don't Help

RocksDB provides powerful capabilities that our workload **doesn't use** - they're pure overhead:

**Unused features (pure overhead):**
- **Range scans**: We only do point lookups (Get by key)
- **Range deletions**: We evict entire files, not key ranges
- **Iterators (forward/backward)**: No iteration needed
- **Block cache**: Values too large (900KB avg) - cache would need 100GB+
- **SSTable metadata**: Index blocks, filter blocks, data blocks - all overhead for large values
- **Compaction strategies**: Leveled/Universal need 5-15× write amp (exceeds disk capacity)
- **Point updates/deletes**: Append-only workload, no updates

**What we actually need:**
- ✅ Fast negative lookups (bloom filter)
- ✅ Fast positive lookups (index + disk read)
- ✅ Batched writes (memtable)
- ✅ FIFO eviction

**Root cause:** RocksDB is a general-purpose LSM storage engine. For specialized workloads (append-only caching), its generality becomes overhead.

### Compaction Tax

**FIFO compaction is optimal** for our disk constraints (1GB/s write capacity), but comes with costs:

**During our benchmark (256GB written):**
- RocksDB created 12 SST files (960MB each = 11.5GB)
- FIFO triggered: Compact 12 files → 1 merged file (11GB)
- This requires:
  - **Read**: 11.5GB from disk (streamed in blocks)
  - **Merge-sort**: CPU intensive, sorting blocks from 12 files
  - **Write**: 11GB merged output back to disk
- All while handling new writes!

**Result:**
- Only ~0.4 GB/s available for new data ingestion
- Remaining ~0.6 GB/s consumed by compaction I/O
- 7.5 cores for merge operations
- 100GB memory usage (78% of available RAM)

**For logs caching, compaction provides zero value** - we never read old data once it's evicted. The compaction is pure maintenance overhead.

---

## Solution Overview

### Baseline Performance

**For caching large blobs on local NVMe, a simple file-per-blob strategy already performs well** - often matching or exceeding RocksDB in our early benchmarks. The combination of large blob sizes (900KB average) and fast local NVMe storage means that simple approaches are competitive.

**RocksDB's blob mode:** RocksDB offers a "blob" feature designed for large values, storing them separately from the LSM tree. We have not evaluated this mode recently. Historical attempts were problematic, though the specific issues are no longer documented. This design takes a different approach: purpose-built for immutable blob caching rather than adapting a general LSM engine.

### Core Insights

**What we eliminate:**
1. **Compaction** - Immutable blobs never merge, no compaction needed
2. **Multi-file bloom checks** - Single global bloom filter
3. **SST file overhead** - Simple segment files (no index blocks, filter blocks, etc.)

**What we keep from RocksDB:**
1. **MemTable design** - Batch writes, async flush with multiple workers
2. **Write buffer sizing** - 1GB memtables, 6 inflight (matches production)
3. **DirectIO** - Bypass OS cache for predictable write latency

**What we add:**
1. **Unified bloom filter** - Lock-free, atomic operations
2. **Streaming checksums** - hash.Hash32 interface, verify on Read()
3. **Storage strategies** - Clean architecture for eviction logic

### System Design Philosophy

**Optimize for:**
- ✅ Negative lookups (52% of requests) - 1ns bloom check
- ✅ Write throughput - No compaction stealing bandwidth
- ✅ Predictability - No compaction storms
- ✅ Simplicity - Append and evict, that's it

**Accept trade-offs:**
- ❌ Segment mode: No point deletes (segment-level eviction only)
- ❌ No updates (append-only)
- ❌ No range queries (not needed)
- ❌ Coarser eviction granularity in segment mode

**Cache semantics:** Recent write loss acceptable (no fsync by default), corruption detected (optional checksums), simple FIFO eviction.

---

## Architecture

### Component Overview

```
┌───────────────────────────────────────────────────────────┐
│                         Cache                             │
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │ Bloom Filter │  │   MemTable   │  │ Storage Strategy│  │
│  │  (Lock-Free) │  │  (Batching)  │  │   (Eviction)    │  │
│  │              │  │              │  │                 │  │
│  │  • 1ns Test  │  │• Async flush │  │• BlobStorage    │  │
│  │  • Single    │  │• Configurable│  │• SegmentStorage │  │
│  │  • Atomic    │  │  workers     │  │                 │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬────────┘  │
│         │                 │                   │           │
│         └────────┬────────┴───────────────────┘           │
│                  │                                        │
│         ┌────────▼────────────────────┐                   │
│         │   Index (Bitcask/Skipmap)   │                   │
│         │                             │                   │
│         │  Scan() → eviction, bloom   │                   │
│         │  Get() → segment lookup     │                   │
│         │  PutBatch() → flush         │                   │
│         └────────┬────────────────────┘                   │
│                  │                                        │
│    ┌─────────────┴──────────────┐                         │
│    │                            │                         │
│  ┌─▼────────────┐      ┌────────▼─────────┐               │
│  │ BlobReader   │      │   BlobWriter     │               │
│  │              │      │                  │               │
│  │• Buffered    │      │ • Buffered       │               │
│  │• Segment     │      │ • DirectIO       │               │
│  │• Checksum    │      │ • Segment        │               │
│  └──────────────┘      └──────────────────┘               │
└───────────────────────────────────────────────────────────┘
```

The system has four main components working together:

1. **Bloom Filter** - Fast negative lookup rejection (1ns)
2. **MemTable** - Batched write buffering with async flush
3. **Index** - Key → segment position lookup (Bitcask or Skipmap)
4. **Storage Strategy** - Eviction logic (blob-specific or segment-specific)

Plus supporting components:
- **BlobReader/Writer** - Abstraction for file I/O strategies
- **Path managers** - Centralized filename generation
- **Checksum verifier** - Streaming data integrity

### Write Path

```
Application
    ↓ Put(key, value)
MemTable (lock-free skipmap)
    ↓ [accumulate until 1GB]
Rotation (atomic pointer swap)
    ↓ Send to flush channel
Flush Worker (1 of 6)
    ↓ Range over entries
BlobWriter.Write(key, value)
    ↓ Append to segment file
    ↓ Compute checksum
Collect Records[]
    ↓ After all written
Index.PutBatch(records)
    ↓ Batch insert metadata
Update approxSize
    ↓ If > MaxSize
Reactive Eviction (immediate)
```

**Key design:**
- Lock-free puts (skipmap has no global lock)
- Rotation only when buffer full (configurable, production uses 1GB)
- Multiple flush workers process batches concurrently (default 6, configurable)
- Batched index updates (efficient)
- Immediate eviction response to spikes

### Read Path

```
Application
    ↓ Get(key)
Bloom Filter Test (1ns)
    ↓ false? → return (nil, false)
    ↓ true (or might exist)
MemTable.Get(key)
    ↓ found? → return bytes.Reader(value)
    ↓ not in memtable
Index.Get(key) → Record
    ↓ Lookup SegmentID, Pos, Size
BlobReader.Get(key)
    ↓ Open segment file (cached)
    ↓ ReadAt(pos, size)
    ↓ Wrap with checksum verifier if enabled
Return io.Reader
```

**Performance:**
- Negative: **1ns** (bloom rejects)
- Positive (cached): **~1µs** (memtable hit)
- Positive (disk): **~2-3ms** (index lookup + disk read)

Compare to RocksDB negative: **45µs** (455 bloom checks)

### Eviction (Dual Mode)

**Reactive (immediate):**
- Triggered by memtable flush when size > MaxSize
- Prevents cache bloat during traffic spikes
- Atomic flag prevents concurrent evictions (multiple flush workers could trigger simultaneously)

**Background (safety net):**
- Runs every 60 seconds
- Catches gradual growth
- Ensures size stays under limit

**Process:**
1. Compute total size (Scan index, sum sizes)
2. If > MaxSize: Calculate toEvict with hysteresis (10% extra)
3. Delegate to StorageStrategy.Evict(toEvict)
4. Update approxSize tracking

---

## Component Deep Dive

### Bloom Filter - Unified Lock-Free Design

**The problem blobcache solves:**

RocksDB's FIFO compaction creates many L0 files (200-600 depending on ingestion rate). Each file has its own bloom filter. For negative lookups, RocksDB must check EVERY file's bloom filter sequentially:

```
File 001: bloom.Test(key) → false
File 002: bloom.Test(key) → false
File 003: bloom.Test(key) → false
...
File 455: bloom.Test(key) → false
Total: 455 × 100ns = 45µs
```

**Our solution:** Single global bloom filter
```
Global bloom.Test(key) → false
Total: 1ns
```

**Size estimation for production:**

For 1M keys with 1% false positive rate:
- Bits needed: -ln(0.01) / (ln(2))² × 1M ≈ 9.6 bits/key
- Total: 9.6M bits = 1.2MB
- With uint32 array: 300K words × 4 bytes = 1.2MB

For 4M keys:
- 4.8MB bloom filter
- Rebuild time: 40ms (scan 4M keys from index)
- Memory overhead: Negligible (<5MB)

**Atomic operations trade-off:**

**Why atomic (not mutex):**
- Test(): Single atomic load = 1ns
- Add(): CAS loop = 12ns (retry on contention)
- Concurrent Test(): 0.95ns (perfect scaling, no lock)

**Why not mutex:**
- Mutex acquire/release: 10-20ns even uncontended
- Under contention: 50-100ns+
- Would make Test() 10-20× slower

**Precision trade-off:**
- Atomic Add() uses CAS loop (may retry on concurrent writes)
- Rare: Only when same word written concurrently
- Acceptable: Retry overhead (1-2ns) << mutex overhead (10-20ns)

**Rebuild strategy:**
- Deletes don't update bloom (would need atomic bit clear - complex)
- Instead: Rebuild periodically (10min default)
- Scan index (fast), create new filter, atomic pointer swap
- Accept temporary false positives after deletes
- Simpler, cleaner design

### MemTable - Batched Async Flush

**Design goal:** Match RocksDB's write efficiency while staying simpler.

**Why multiple frozen memtables (like RocksDB):**

Imagine single memtable with synchronous flush:
```
Write 1GB → Flush blocks → Can't accept new writes → 1-2 second stall
```

With multiple memtables (production config: 6):
```
Active memtable: Accepting writes (lock-free)
Frozen #1-N: Flushing concurrently (N I/O workers)
```

**Benefits:**
- ✅ **Avoids blocking writes**: Active memtable usually available
- ✅ **Saturates disk**: Multiple workers × concurrent I/O = full bandwidth
- ✅ **Bounded memory**: Max N×bufferSize (production: 6×1GB = 6GB)

**Configuration trade-offs:**

| Setting | Memory | I/O Depth | Throughput | When to use |
|---------|--------|-----------|------------|-------------|
| 1GB × 6 workers | 6GB | High | 1.04 GB/s | Production (match RocksDB) |
| 512MB × 6 workers | 3GB | High | 1.04 GB/s | Memory-constrained |
| 1GB × 3 workers | 3GB | Medium | 0.8 GB/s | Lower memory + throughput |
| 100MB × 6 workers | 600MB | Medium | 0.6 GB/s | Development/testing |

**More workers ≠ better (for disk-bound workloads):**
- Production (6 workers) already saturates 1GB/s disk
- Doubling to 12 workers would increase memory (12GB) without improving throughput
- Disk-bound, not CPU-bound (in our benchmark scenario)

**Backpressure mechanism:**
- When MaxInflightBatches full (production: 6), next Put() blocks
- Currently implemented with mutex + condition variable
- Prevents unbounded memory growth during traffic spikes

### Index - Bitcask vs DuckDB Decision

**Why we chose Bitcask over DuckDB:**

**DuckDB (original design):**
- Analytical queries: `SELECT segment_id, SUM(size) FROM entries GROUP BY segment_id ORDER BY MIN(ctime)`
- Fast aggregations: `SELECT SUM(size) FROM entries` in 40ms for 4M rows
- Nice to have, but not essential

**Bitcask (current):**
- Simple embedded key-value store
- In-memory hash table for O(1) lookups
- Persistent (append-only log + hint file)
- No CGO, no SQL, smaller binary

**Decision rationale:**
1. **Scan() is sufficient**: Eviction can scan and sort in memory (fast enough)
2. **Simpler**: No SQL engine, easier to debug/maintain
3. **Proven**: Battle-tested in Riak and other systems
4. **Lighter**: Fewer dependencies, smaller attack surface

**Skipmap option for speed:**
- Pure in-memory for fastest reads (lock-free skipmap)
- Optional Bitcask backing for durability
- Best for segment mode (rebuilt from segments on restart)
- Trade-off: Requires segment mode (can't rebuild per-blob)

**Durability model:**

Both indexes are eventually durable:
- **Bitcask**: Append-only log flushed to disk, hint file for fast restart
- **Skipmap**: Backed by Bitcask writes (durable) + in-memory reads (fast)

**Consistency:**
- Index updated AFTER blob written to disk
- Crash before index update: Orphaned segment (acceptable for cache)
- Crash after index update: Complete, blob readable
- Checksums detect corruption (optional)

### Storage Strategies - Clean Eviction Architecture

**The problem:** Eviction logic fundamentally differs between storage modes.

**Per-blob mode:**
- Need to delete individual files
- Must sort ALL records by CTime (oldest first)
- Path generation: `blobs/shard-XXX/YYY.blob`

**Segment mode:**
- Delete entire segment files
- Group records by SegmentID, sort segments by age
- Path generation: `segments/TIMESTAMP-WORKER.seg`

**Original design:**
```go
func runEviction() {
    if segmentMode {
        // ... 50 lines of segment logic ...
    } else {
        // ... 50 lines of blob logic ...
    }
}
```

**Current design (good):**
```go
type StorageStrategy interface {
    Evict(ctx, targetBytes) (evictedBytes, error)
}

type BlobStorage struct {
    paths BlobPaths  // Encapsulates path generation
    index Indexer
}

type SegmentStorage struct {
    paths SegmentPaths  // Different path structure
    index Indexer
}
```

**Benefits:**
1. **Single Responsibility** - Each strategy owns its eviction logic
2. **Testable** - Unit test BlobStorage.Evict() in isolation
3. **Path encapsulation** - No scattered `fmt.Sprintf("shard-%03d")` everywhere
4. **Clean Indexer** - Only Scan() method (generic, usable by both strategies)

**Path managers (zero-allocation):**
```go
type BlobPaths string      // Just the basePath
type SegmentPaths string   // Just the basePath

// Methods return constructed paths
func (p BlobPaths) BlobPath(key) string
func (p SegmentPaths) SegmentPath(id, worker) string
```

Benefits: Value types (not pointers), no allocations, type-safe (can't mix blob/segment paths).

### Checksums - Streaming Verification Design

**Design philosophy:**

Traditional approach: Read entire blob, compute checksum, compare, return data.
- ❌ Forces eager reading (even if caller only needs 1KB)
- ❌ Duplicates code (every reader must implement)
- ❌ Close errors ignored (`defer file.Close()` swallows errors)

**Our approach:** Streaming verification with stdlib interface.

**Use hash.Hash32 (not custom Checksummer):**
- ✅ Standard library interface (hash/crc32, hash/xxhash compatibility)
- ✅ Proven, well-understood semantics
- ✅ Incremental hashing (Write() method)
- ✅ Existing implementations (crc32.NewIEEE(), etc.)

**Why this works:**
1. **Lazy**: Checksums only computed for data caller actually reads
2. **Resilient**: Error returned where caller will see it (on Read)
3. **Efficient**: No buffering, hash computed incrementally
4. **Reusable**: Same wrapper for all readers (no duplication)

**Mid-lifecycle restart support:**

`Record.HasChecksum` flag enables safe restarts:
- Old data: `HasChecksum=false` (skip verification)
- New data: `HasChecksum=true` (verify if VerifyOnRead enabled)
- Avoids `checksum=0` ambiguity (0 is a valid CRC32 value)
- Flags byte encoding for future extensions

**Test coverage:**
- 9 comprehensive tests including edge cases
- 1-byte-at-a-time streaming (extreme case)
- Checksum mismatch detection and error caching
- Empty data, large data (10MB), varying chunk sizes

### Readers and Writers - DirectIO Analysis

**BlobReader interface:**
```go
type BlobReader interface {
    Get(key base.Key) (io.Reader, bool)
    Close() error
}
```

**Implementations:**
- **BufferedReader**: Per-blob mode, standard I/O
- **SegmentReader**: Segment mode, file handle caching

**BlobWriter interface:**
```go
type BlobWriter interface {
    Write(key base.Key, value []byte) error
    Pos() WritePosition  // Returns SegmentID, Pos for index
    Close() error
}
```

**Implementations:**
- **BufferedWriter**: Standard I/O with atomic temp+rename
- **DirectIOWriter**: O_DIRECT with aligned writes
- **SegmentWriter**: Append to large files (buffered or DirectIO)

**DirectIO: The Controversial Choice**

**When DirectIO helps (production Linux):**
- ✅ Predictable latency (no kernel writeback interference)
- ✅ Consistent throughput (1.04 GB/s, no variance; if you can saturate)
- ✅ No dirty page writeback storms
- ✅ Better for 24/7 production (no surprises)

**When DirectIO hurts (and might hurt blobcache):**
- ❌ **Synchronous writes**: Each write waits for disk acknowledgment
  - Buffered I/O: Write returns immediately (kernel buffers in page cache)
  - DirectIO: Write blocks until disk confirms (~1ms per 1MB)
- ❌ **Higher memory pressure**: More inflight batches needed during writes
  - Buffered: Kernel absorbs spikes in page cache
  - DirectIO: Must buffer in userspace (more frozen memtables)
- ❌ **Defeats page cache for recent reads**:
  - 45% of benchmark reads are "hits" (recent writes from same worker)
  - Buffered: Likely in page cache (RAM speed, ~1ns)
  - DirectIO: Must read from disk (1-3ms)
- ❌ **Alignment overhead**: Padding to 4KB, then truncate

**Why RocksDB uses DirectIO:**
- Compaction reads OLD data (not in page cache anyway)
- DirectIO helps compaction avoid polluting cache
- Different access pattern than blobcache

**Why blobcache might NOT want DirectIO:**
- Workload: Write data, shortly after read it (benchmark pattern)
- Page cache hit rate could be very high (recent data cached)
- DirectIO bypasses this benefit

**Current status:**
- DirectIO is optional (WithDirectIOWrites)
- Benchmark tested with DirectIO enabled (for RocksDB parity)
- Open question: Would buffered I/O be faster for blobcache's workload?

**Recommendation:** Benchmark both modes on production hardware. Page cache might provide significant benefit for recently-written data.

### Readers/Writers - Fdatasync Strategy

**Per-blob writes (atomic temp+rename):**
```
1. Write to temp file (.tmp-{fileID}-{timestamp}.blob)
2. Fdatasync (if enabled)
3. Close temp file
4. Atomic rename to final path
```

**Segment writes (append to long-lived file):**
```
Buffered mode:
1. Append blob to segment
2. Fdatasync after EACH blob (durability for long-lived file)

DirectIO mode:
1. Append blob (already bypasses cache)
2. Fdatasync only on segment close (redundant but defensive)
```

**Why fdatasync (not fsync):**
- **2-5× faster** (Linux: data-only, skips metadata sync)
- **Sufficient**: Blob size stored in index (not filesystem metadata)
- **Immutable**: Don't care about mtime
- **Platform**: Linux uses fdatasync(2), Darwin uses F_FULLFSYNC (no fdatasync exists)

**When enabled (WithFsync):**
- ❌ **Not recommended** for caching (defeats purpose of speed)
- ✅ **Use for**: Durable storage where data loss unacceptable
- Trade-off: Lower throughput for durability

---

## Performance Analysis

### Benchmark Results Interpretation

**AWS Graviton m7gd.8xlarge (production-class hardware):**
- 32 cores, 128GB RAM, 1.7TB NVMe
- Disk: 536K read IOPS, 268K write IOPS (independent channels)
- **No SLC cache** - consistent sustained performance

**Test:** 2.56M operations (256K writes = 256GB, 2.3M reads)
- Workload: 10% writes, 45% hits (worker's own recent writes), 45% misses
- Both systems: 1GB memtable, 6 inflight, DirectIO, no compression

**Results:**

| Metric | RocksDB | Blobcache | Ratio |
|--------|---------|-----------|-------|
| Latency | 259µs | 93µs | 2.77× faster |
| Throughput | 0.38 GB/s | 1.04 GB/s | 2.77× faster |
| Duration | 18.7 min | 4.2 min | 4.45× faster |
| Memory | 100.6GB | 2.2GB | 45.7× less |
| CPU | 7.5 cores | 2 cores | 3.75× less |

**Disk bandwidth breakdown:**

| System | User Writes | Compaction Read | Compaction Write | Total I/O | % Useful |
|--------|-------------|-----------------|------------------|-----------|----------|
| Blobcache | 1.04 GB/s | 0 | 0 | 1.04 GB/s | **100%** |
| RocksDB | ~0.4 GB/s | ~0.3 GB/s | ~0.3 GB/s | ~1.0 GB/s | **40%** |

**Key insight:** RocksDB reports 0.38 GB/s "write throughput" but uses ~1 GB/s total disk I/O. Only 40% is new data ingestion - the rest is compaction maintenance. Blobcache's 1.04 GB/s is 100% useful work.

**Memory usage:**

| System | Memory Used | % of 128GB RAM |
|--------|-------------|----------------|
| Blobcache | 2.2GB | 1.7% |
| RocksDB | 100.6GB | 78% |

**Observation:** RocksDB benchmark used 78% of available memory (100.6GB on 128GB machine), while blobcache used 1.7% (2.2GB).

This difference could stem from various factors - bloom filters loaded in memory for 455 files, SST metadata structures, CGO boundary overhead, or features we haven't implemented (that may be valuable). It's also possible blobcache has simplifications or bugs that will require more memory as the implementation matures.

The current observed state: one benchmark uses 2-5% of RAM, the other uses 70-80%.

**CPU usage:**
- RocksDB: 7.5 cores (merge-sorting 11GB files)
- Blobcache: 2 cores (lock-free writes, simple appends)

**Consistency:**
- RocksDB: Variable (0.8-1.1 GB/s swings during compaction)
- Blobcache: Steady 1.04 GB/s (no interference)

### Platform Differences

**Mac M4 vs Linux Graviton:**

| Characteristic | Mac M4 Max | AWS Graviton |
|----------------|------------|--------------|
| **SLC cache** | ~6GB (burst 3-4 GB/s) | None (consistent 1 GB/s) |
| **Sustained throughput** | 1-1.5 GB/s (after SLC) | 1.04 GB/s (always) |
| **DirectIO benefit** | ❌ Slower (SLC faster) | ✅ Essential (no page cache games) |
| **Benchmark value** | Misleading (burst) | Production-realistic |
| **Fdatasync** | F_FULLFSYNC (fcntl) | syscall.Fdatasync() |

**Lesson:** Always validate on production hardware. Mac's SLC cache makes it look faster than reality, and DirectIO shows opposite performance characteristics.

**Page cache considerations:**

Benchmark workload: 45% reads are "hits" (worker reads its own recent writes).

**With page cache (buffered I/O):**
- Recent writes likely cached in RAM
- Reads served at memory speed (~1ns)
- Lower apparent disk read load

**With DirectIO:**
- Bypasses page cache
- All reads hit disk (1-3ms)
- Higher disk utilization

**Open question:** Is DirectIO actually beneficial for blobcache?
- RocksDB compaction reads OLD data (not cached anyway)
- Blobcache reads RECENT data (likely cached)
- Further benchmarking needed to measure page cache hit benefit

---

## Configuration Guide

### Production Settings (Linux)

```go
cache, _ := New("/data/blobcache",
    // Capacity
    WithMaxSize(1<<40),              // 1TB total
    WithSegmentSize(2<<30),          // 2GB segments
    WithEvictionHysteresis(0.1),     // Evict 10% extra

    // Write buffering (match RocksDB)
    WithWriteBufferSize(1<<30),      // 1GB memtable
    WithMaxInflightBatches(6),       // 6 concurrent flushes

    // Performance
    WithDirectIOWrites(),            // Debatable - test both!
    WithSkipmapIndex(),              // Fastest reads (in-memory)

    // Integrity
    WithChecksum(),                  // CRC32 on writes
    WithVerifyOnRead(false),         // Don't verify (trust checksums exist)

    // Durability
    WithFsync(false),                // Cache can lose recent writes
)
```

**Why these settings:**
- **1GB memtable**: Matches RocksDB production (960MB)
- **6 inflight**: Saturates disk, matches RocksDB max_write_buffer_number
- **2GB segments**: Sweet spot (not too many files, not too large)
- **DirectIO**: Currently used, but should test without (page cache might help)
- **Skipmap**: Fastest index (trade: requires segment mode)
- **Checksums ON, Verify OFF**: Detect corruption, don't pay read penalty
- **No fsync**: Cache semantics (speed > durability)

### Development Settings (Mac)

```go
cache, _ := New("/tmp/blobcache",
    WithMaxSize(10<<30),             // 10GB (smaller for quick iteration)
    WithSegmentSize(1<<30),          // 1GB segments
    WithWriteBufferSize(100<<20),    // 100MB (faster test cycles)
    WithMaxInflightBatches(6),       // Same concurrency
    // No DirectIO (slower on Mac, SLC cache is faster)
    // No checksums (faster development iteration)
)
```

---

## Trade-offs & Limitations

### 1. NOT Battle-Tested

**Critical caveat:** This is a brand new system (December 2025).

**RocksDB has:**
- 10+ years in production
- Millions of deployments
- Every edge case discovered and fixed
- Mature ecosystem

**Blobcache has:**
- Comprehensive unit tests
- Integration tests
- One production benchmark
- **No production runtime yet**

**What this means:**
- ❌ Unknown failure modes exist
- ❌ Edge cases not yet discovered
- ❌ Performance under extreme load untested (though verified through benchmarks)
- ✅ Simple design reduces surface area (fewer things can go wrong)

### 2. Segment Mode: Coarse-Grained Eviction

**Limitation (segment mode only):** Can't delete individual blobs within a segment.

**Consequences:**
- Evicting a 2GB segment might delete 10,000 blobs
- Some blobs in that segment might be newer/hotter; but that's exactly what RocksDB FIO strategy does anyway.
- Wasted space until segment evicted

**Per-blob mode:** Supports fine-grained deletion (one file per blob).

**Why acceptable:**
- ✅ Cache semantics (imprecise eviction OK)
- ✅ Simplicity (no per-blob tracking)
- ✅ Performance (one file delete, not 10,000)


### 3. FIFO Only (No LRU)

**Limitation:** Eviction by age, not access frequency.
* Possible follow up: file per blob strategy enables LRU based eviction.

**Consequences:**
- Can't keep frequently-accessed data and evict cold data
- FIFO might evict popular segment before unpopular one

**Why acceptable:**
- ✅ Simpler (no access time tracking)
- ✅ Fast eviction (no LRU bookkeeping)
- ✅ Good enough for many workloads

**Not acceptable for:** Workloads with strong access patterns (20% of data gets 80% of requests).

### 4. Append-Only (No Updates/Deletes)

**Limitation:** Can't update or delete individual blobs.

**Consequences:**
- Space not reclaimed until segment evicted
- Can't correct corrupted data in-place

**Why acceptable:**
- ✅ Write-once-read-many workload
- ✅ Cache can be rebuilt from source (S3)

**Not acceptable for:** Mutable data stores.

### 5. Memory During Eviction

**Limitation:** Eviction loads all records into memory.

**Current behavior:**
- BlobStorage: Scan all records, sort by CTime in memory
- SegmentStorage: Scan all records, group by SegmentID in memory

**Memory overhead:**
- 1M records × ~50 bytes each = 50MB
- 10M records = 500MB
- 100M records = 5GB

**Acceptable** for expected scale (1-10M records).
**Not acceptable** for 100M+ records (would need streaming sort).

---

## When To Use Blobcache

### ✅ Perfect For:
- Append-heavy workloads (logs, metrics, events, immutable artifacts)
- Large blobs (100KB-10MB) where bloom filters provide value
- High miss rates (bloom filter rejection is critical path)
- FIFO eviction acceptable (age-based, not access-based)
- Cache semantics (data loss acceptable, rebuild from source)
- Need predictable performance (no compaction surprises)

### ❌ NOT Suitable For:
- Point updates or deletes (append-only architecture)
- Range queries or sorted iteration (hash-based index)
- Small values (<10KB) where RocksDB's block cache helps
- LRU eviction required (only FIFO supported)
- Strong durability requirements (cache can lose recent writes)
- Battle-tested requirement (this is new, not proven at scale)

---

## Appendix

### A. Implementation Details

**Bloom filter implementation:**
```go
// Lock-free atomic operations
func Test(key) bool {
    word := atomic.LoadUint32(&data[index])
    return word & mask != 0
}

func Add(key) {
    for {
        old := atomic.LoadUint32(&data[index])
        if atomic.CompareAndSwapUint32(&data[index], old, old|mask) {
            break
        }
    }
}
```

**Record encoding (Bitcask):**
```
SegmentID(8) + Pos(8) + Size(4) + CTime(8) + Flags(1) + [Checksum(4)]
= 29-33 bytes
```

**Key hashing:**
```go
h := xxhash.Sum64(key)
shardID := h % numShards
fileID := h >> 32
```

### B. Directory Structure

**Segment mode:**
```
/data/blobcache/
├── bitcask-index/
│   ├── 000000.data
│   └── 000000.hint
├── bloom.dat
└── segments/
    ├── 1733183456789123456-00.seg  (2GB, worker 0)
    ├── 1733183456812456789-01.seg  (2GB, worker 1)
    └── ...
```

**Per-blob mode:**
```
/data/blobcache/
├── bitcask-index/
├── bloom.dat
└── blobs/
    ├── shard-000/
    │   ├── 12345.blob
    │   └── 67890.blob
    ├── shard-001/
    └── ... (256 shards)
```

### C. Error Handling Philosophy

**Cache semantics:**
- Recent write loss acceptable (no fsync by default)
- Corruption detected (optional checksums)
- Eviction can be imprecise (segment-level granularity)

**Warnings (not errors):**
- Failed blob delete → log, continue
- Index error → log, continue
- Eviction error → log, retry later

**Crash scenarios:**
- During segment write: Orphaned partial segment (ignore on startup)
- After segment, before index: Orphaned complete segment (no index entry)
- After index update: Success (blob readable)

### D. Comparison to Alternatives

**vs RocksDB:**
- Blobcache: 2.77× faster, 45× less memory, no compaction
- Use when: Append-only, FIFO eviction OK, cache semantics
- Avoid when: Need updates/deletes, range queries, battle-tested

**vs BadgerDB:** Similar LSM design, would have same compaction issues

**vs BoltDB:** B+tree, single-writer, not suitable for high-throughput

**vs Filesystem:** Tried (70% CPU in syscalls), led to segments

---

## Conclusion

BlobCache demonstrates that **specialized design beats general-purpose** for append-heavy caching:
- Eliminates RocksDB's compaction overhead (11GB merge files during 256GB write!)
- Single bloom filter (45,000× faster negative lookups)
- Simple architecture (append, evict, done)

**Production readiness:**
- ✅ Benchmark validated (2.77× faster)
- ✅ Comprehensive tests
- ❌ **Not battle-tested** (new system, gradual rollout required)
- ❌ **Unknown failure modes** exist (expect surprises)

**Recommended approach:**
1. Shadow production traffic (validate bloom FP rate, memory usage)
2. Gradual rollout with extensive monitoring
3. Keep RocksDB as fallback during migration
4. Test both with/without DirectIO (page cache might help)

**Bottom line:** For Datadog's logs caching use case (avoid S3 re-downloads), blobcache eliminates RocksDB's compaction tax. Worth the risk of a new system for 2.77× performance gain, but proceed carefully.
