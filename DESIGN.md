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
   - Single filter for all keys (not per-shard)
   - Lock-free atomic operations (1 ns reads, 12 ns writes)
   - Rebuilt periodically from index (configurable: 5-10min or on FP rate threshold) (40 ms for 4M keys)
   - Stored in DuckDB (not separate files)

2. **DuckDB Index (Analytical)**
   - Key → (shardID, fileID, size, ctime, mtime)
   - Exceptional query performance (0.1-0.5s for FIFO scans)
   - ACID transactions, crash recovery via WAL
   - Zero-alloc Get() API (caller-provided Entry)

3. **Sharded Blob Storage (NVMe-Optimized)**
   - Sharded directories (default 256, configurable) (filesystem performance)
   - One blob per file (immutable, deterministic filename from hash)
   - Designed for high-performance NVMe (1+ GB/s)
   - Temp + rename for atomic writes

**Data flow:**

```
Put:  Hash key → Write blob file → Update index → Update bloom (lock-free, immediate)
Get:  Bloom check (1 ns) → Read file directly (2-3 ms) - No index lookup needed!
```

### Design Decisions Optimized for Hot Path

**Prioritize read speed:**
- ✅ Lock-free bloom (no mutex contention on reads)
- ✅ Single bloom check (not 455 like RocksDB)
- ✅ Deterministic fileID (no counter, no locking)
- ✅ Zero-alloc Get() (caller-provided Entry)

**Accept trade-offs for simplicity:**
- ✅ Deletes don't update bloom (accept FPs, rebuild periodically)
- ✅ One blob per file (simple, no offsets, no packing)
- ✅ Daily orphan cleanup (not on startup)
- ✅ Crash between DB/FS delete (orphans cleaned later)

**Hardware assumptions:**
- High-performance NVMe (1+ GB/s write, <1 ms latency)
- Filesystem handles millions of small files well
- Sharded directories (default 256, configurable) keep each directory manageable


## Architecture

### Directory Structure

```
/data/blobcache/
├── db/
│   ├── index.duckdb          # DuckDB index + bloom filter storage
│   └── wal/                  # DuckDB WAL (crash recovery)
└── blobs/
    ├── shard-000/
    │   ├── 123456789.blob    # One blob per file (deterministic from key hash)
    │   └── ...
    ├── shard-001/
    └── ...                   # 256 shards (filesystem sharding)
```

**Note:** Bloom filter stored IN DuckDB (as BLOB column), not separate files.

---

## Database Schema

```sql
CREATE TABLE entries (
    key BLOB PRIMARY KEY,
    shard_id INTEGER NOT NULL,
    file_id BIGINT NOT NULL,     -- Deterministic from key hash
    size INTEGER NOT NULL,
    ctime BIGINT NOT NULL,        -- Creation time
    mtime BIGINT NOT NULL         -- Last modification
);

CREATE INDEX idx_ctime ON entries(ctime);  -- For FIFO by creation
CREATE INDEX idx_mtime ON entries(mtime);  -- For FIFO by LRU

-- Bloom filter storage (single global filter)
CREATE TABLE bloom_filter (
    version INTEGER PRIMARY KEY,  -- For versioning
    data BLOB NOT NULL,           -- Serialized bloom filter
    created_at BIGINT NOT NULL
);
```

**Why DuckDB:**
- Embedded (no separate process)
- Exceptional analytical performance:
  - Scans 4M rows in ~40 ms
  - `SELECT MIN(ctime) ... GROUP BY shard_id, file_id` in 0.1-0.5s
  - Perfect for FIFO eviction queries
- ACID transactions
- Built-in WAL (crash recovery)
- Can store bloom filter (BLOB column)

**File mapping:** `blobs/shard-{shardID}/{fileID}.blob`
- shardID: `hash % 256`
- fileID: `hash` itself (deterministic, no counter needed)

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

// Options (functional pattern)
WithMaxSize(int64) Option
WithShards(int) Option
WithEvictionStrategy(EvictionStrategy) Option
WithChecksums(bool) Option  // Default: true
WithFsync(bool) Option       // Default: false
WithVerifyOnRead(bool) Option // Default: false
```

**Defaults:**
- MaxSize: 80% of disk capacity (auto-detected)
- Shards: 256
- Eviction: EvictByCTime (oldest first)
- Checksums: Enabled (detect corruption)
- Fsync: Disabled (cache semantics, fast)
- VerifyOnRead: Disabled (opt-in)

---

## Write Path

```go
Put(key, value):
1. Hash key → shardID, fileID (deterministic)
2. Write to temp file
3. Atomic rename (crash-safe)
4. Insert into DuckDB index
5. Add to bloom filter (lock-free CAS)
```

**Atomicity:** Temp + rename ensures file is complete or doesn't exist

**Crash scenarios:**
- Before rename: Temp file orphaned (cleaned daily)
- After rename, before DB: Blob orphaned (cleaned daily)
- After DB: Complete ✓

---

## Read Path

```go
Get(key):
1. Bloom filter check (~1 ns, lock-free load)
   → If no: return ErrNotFound (fast rejection!)
2. Query DuckDB index (~50 µs)
   → If not found: return ErrNotFound (bloom FP)
3. Read blob file (~2-3 ms for 400 KB)
4. Verify checksum if opted in
```

**Performance:**
- Negative: ~1 ns (vs RocksDB's 45 µs) - **45× faster**
- Positive: ~2-3 ms (same as RocksDB, disk I/O limited)

---

## Eviction & Cleanup

### Eviction (Background Worker)

**NOT atomic** - manages race conditions with trade-offs:

```go
evictIfNeeded():
1. Query DuckDB for total size (fast aggregate)
2. If over limit, query oldest files:
   SELECT shard_id, file_id, SUM(size)
   FROM entries
   GROUP BY shard_id, file_id
   ORDER BY MIN(ctime) ASC  -- or mtime for LRU
   
3. Delete entries from DB (transaction)
4. Delete blob files from filesystem
5. Bloom filter: NOT updated (accept FPs, rebuild periodically)
```

**Order:** DB first, then filesystem
- Get() returns ErrNotFound immediately after DB delete
- Orphaned blobs if crash (cleaned daily)
- Acceptable for cache semantics

**Race condition:** Read between DB delete and FS delete → ErrNotFound (acceptable)

### Orphan Cleanup (Daily, Not Startup)

Runs in background once per day:
- Compare filesystem to DB
- Delete orphaned temp files
- Delete orphaned blobs

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

**Next (~40% remaining, ~6-8 hours):**
- [ ] Complete Put/Get/Delete (blob file operations)
- [ ] Eviction worker
- [ ] File iterator
- [ ] Crash recovery
- [ ] End-to-end tests

**Final (~20%, ~2-3 hours):**
- [ ] Metrics integration
- [ ] Stress tests (4M keys, 1 TB)
- [ ] Documentation
- [ ] Production validation plan

**Total realistic estimate: 15-20 hours over 2-3 weeks**
(Not 2-3 months - that was wildly pessimistic!)

---

## Decision Log

**Key decisions:**
1. ✅ One blob per file (no offsets/packing)
2. ✅ Deterministic fileID from hash (no counter)
3. ✅ Lock-free bloom (atomic CAS, not mutex)
4. ✅ Bloom stored in DuckDB (not separate files)
5. ✅ DB-first deletion order (then filesystem)
6. ✅ Daily orphan cleanup (not on startup)
7. ✅ Zero-alloc Get() API (caller-provided Entry)
8. ✅ xxHash (7 GB/s, proven)

**Out of scope:**
- ❌ Range queries
- ❌ Sorted iteration
- ❌ Compression (caller-controlled)
- ❌ Compaction (FIFO only)
- ❌ Resharding existing data

**Design validated through:**
- Production RocksDB analysis (455 L0 files, 1M keys, 450 GB)
- Simulator validation (matched production behavior)
- Bloom filter benchmarks (4× faster than alternatives)

---

## Optional: Access Time Tracking (atime)

**For LRU eviction by access time:**

### Schema Extension

```sql
ALTER TABLE entries ADD COLUMN atime BIGINT;
CREATE INDEX idx_atime ON entries(atime);
```

### Get() with atime

```go
// If WithTrackAccessTime(true):
Get(key):
1. Bloom check (1 ns)
2. Read file directly (2-3 ms)
3. Update index: UPDATE entries SET atime = ? WHERE key = ?  (50 µs)
   → Only if tracking enabled
```

**Trade-off:**
- ✓ Enables FIFO by access time (true LRU)
- ✗ Adds 50 µs to Get() (2% overhead on positive lookups)
- ✗ Write overhead on every successful read

### Eviction Strategies

```go
type EvictionStrategy int

const (
    EvictByCTime  // Oldest created (no atime needed)
    EvictByMTime  // Least recently modified (updated on Put)
    EvictByATime  // Least recently accessed (requires WithTrackAccessTime)
)
```

**Options:**
```go
blobcache.WithEvictionStrategy(EvictByATime)
blobcache.WithTrackAccessTime(true)  // Enables atime updates
```

**Default:** EvictByCTime (no atime tracking, fastest Get())

**Use case:** When access patterns matter more than insertion order
