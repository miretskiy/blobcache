# BlobCache Design Document

## Executive Summary

BlobCache is a high-performance blob caching system optimized for append-heavy workloads where RocksDB's compaction overhead becomes prohibitive. In production scenarios with high miss rates (52% negative lookups), RocksDB's FIFO compaction creates hundreds of SST files, forcing every lookup to cascade through multiple bloom filter checks.

**Primary use case:** Local NVMe caching layer for large blobs (100KB-10MB) where unified bloom filters eliminate multi-file lookup cascades.

**Key innovations:**
1. **Unified bloom filter** - Single filter for all keys eliminates cascading checks across hundreds of files
2. **Immutable blob storage** - Write-once architecture eliminates compaction entirely
3. **Degraded mode** - Background I/O errors trigger memory-only mode (cache remains operational)

**Production benchmark (AWS Graviton m7gd.8xlarge, 256GB workload):**
- **2.77× faster** than RocksDB (1.04 GB/s vs 0.38 GB/s)
- **45× less memory** (2.2GB vs 100GB)
- **3.75× less CPU** (2 cores vs 7.5 cores)
- **No compaction storms** - predictable, consistent performance

**Trade-offs:** Brand new system (not battle-tested), segment-level eviction only (no individual blob deletion), FIFO only (no LRU), append-only (no updates).

---

## Motivation

### Production Problem

**Service:** logs-event-store-reader (Datadog logs storage infrastructure)

**Workload characteristics:**
- Large blob storage (100KB-10MB per blob, 900KB average)
- Append-heavy (write-once, read-many, evict-by-age)
- High miss rate (52% of requests for keys not in cache)

**RocksDB configuration (FIFO compaction):**
- 960MB memtables, 6 inflight
- FIFO compaction with 2× write amplification
- Disabled block cache (values too large)
- 455 L0 SST files observed (800+ at peak load)

### The Performance Problem

**Read amplification kills us:**

RocksDB's FIFO compaction trades low write amplification for **extremely high read amplification**. Each Get() checks bloom filters sequentially until a match is found:

```
Negative lookup (52% of requests):
  Must check ALL bloom filters
  = 455 bloom checks × 100ns each = 45µs CPU per request

Positive lookup (48% of requests):
  Statistical average: ~50% checked before match
  = 227 bloom checks × 100ns each = 22µs CPU per request
```

At peak (800+ files), negative lookups cost 80µs+. **This is inherent to FIFO compaction** - the number of files varies with ingestion rate.

### Why RocksDB Features Don't Help

RocksDB provides powerful capabilities our workload **doesn't use**:

**Unused features (pure overhead):**
- Range scans, iterators (we only do point lookups)
- Block cache (900KB values too large)
- Leveled/Universal compaction (5-15× write amplification exceeds disk capacity)
- Point updates/deletes (append-only workload)

**What we actually need:**
- Fast negative lookups (bloom filter)
- Batched writes (memtable)
- FIFO eviction

**Root cause:** RocksDB is a general-purpose LSM storage engine. For specialized workloads (append-only caching), its generality becomes overhead.

### Compaction Tax

During our benchmark (256GB written), RocksDB's FIFO compaction:
- Created 12 SST files (960MB each = 11.5GB)
- Compacted: Read 11.5GB, merge-sort, write 11GB back
- Result: Only 0.4 GB/s available for new data (0.6 GB/s consumed by compaction)

**For logs caching, compaction provides zero value** - we never read old data once it's evicted. Pure maintenance overhead.

---

## Solution Overview

### Core Design Decisions

**1. Unified Bloom Filter (vs Per-File)**

**Why:** Eliminate cascading bloom checks for negative lookups.

RocksDB with 455 files requires 455 sequential bloom checks. We use one global filter - single atomic load.

**Trade-off:** Must rebuild on eviction (can't selectively remove keys). Rebuild is fast (40ms for 4M keys) and infrequent.

**2. Segment-Centric Architecture (vs Per-Blob Files)**

**Why:** File count explosion kills filesystem performance.

Early experiments with one-file-per-blob hit 70% CPU in syscalls. Segments reduce file count 1000×.

**Trade-off:** Coarse-grained eviction (delete entire segment, not individual blobs). Acceptable for cache semantics.

**3. Hash-Based Keys (vs Original Keys)**

**Why:** Decouple key identity from storage sharding.

Keys are hashed to uint64, used consistently throughout (index, bloom, memtable). Simplifies architecture - no key/hash conversions at boundaries.

**Trade-off:** Can't iterate by original key order. Don't need it (point lookups only).

**4. Always Durable Skipmap Index**

**Why:** Fast reads (lock-free in-memory) + crash safety.

Skipmap for O(1) reads, Bitcask backing for persistence. No configuration needed - one optimal solution.

**Trade-off:** Requires segment mode (index rebuilt from segment footers on restart).

**5. Degraded Mode (RocksDB-Inspired)**

**Why:** Graceful degradation better than hard failure.

When background I/O fails (disk full, corruption), workers stop but cache remains operational in memory-only mode. Inspired by RocksDB's read-only mode, but better for caching (memory-only mode still useful).

**Trade-off:** Bloom filter accumulates false positives for dropped memtables. Acceptable (just extra disk lookups in emergency state).

**6. Segment Footers with Metadata**

**Why:** Self-describing segments enable crash recovery.

Each segment file ends with footer containing all blob records (hash, offset, size, checksum). Index can be rebuilt by scanning segment footers.

**Trade-off:** Small space overhead (32 bytes per blob). Enables durability without separate manifest files.

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────┐
│                   Cache                         │
│                                                 │
│  ┌──────────────┐  ┌──────────────┐            │
│  │ Bloom Filter │  │   MemTable   │            │
│  │  (Lock-Free) │  │  (Batching)  │            │
│  │              │  │              │            │
│  │  • 1ns Test  │  │• Lock-free   │            │
│  │  • Atomic    │  │• Async flush │            │
│  │  • Unified   │  │• 6 workers   │            │
│  └──────┬───────┘  └──────┬───────┘            │
│         │                 │                    │
│         └────────┬────────┘                    │
│                  │                             │
│         ┌────────▼──────────────┐              │
│         │   Index (Skipmap)     │              │
│         │   + Bitcask Backing   │              │
│         │                       │              │
│         │  • Lock-free reads    │              │
│         │  • Durable writes     │              │
│         └───────────────────────┘              │
│                  │                             │
│         ┌────────▼──────────────┐              │
│         │  Segment Storage      │              │
│         │                       │              │
│         │  • Append-only files  │              │
│         │  • Self-describing    │              │
│         │  • Optional DirectIO  │              │
│         └───────────────────────┘              │
└─────────────────────────────────────────────────┘
```

### Write Path Philosophy

**Goal:** Never block Put() operations.

**Design:**
1. Put() writes to active memtable (lock-free skipmap)
2. When memtable full (1GB), atomic pointer swap creates new active
3. Frozen memtable sent to flush workers via buffered channel
4. Multiple workers (6) process flushes concurrently
5. Index updated after successful disk write

**Why multiple frozen memtables:**
- Single memtable: 1-2 second flush blocks all writes
- Multiple (6): Active accepts writes while others flush
- Bounded memory: 6 × 1GB = 6GB maximum

**Why batched index updates:**
- Reduce Bitcask write overhead (single transaction for entire memtable)
- Atomic: All records visible or none

### Read Path Philosophy

**Goal:** Fast rejection of non-existent keys (52% of requests).

**Design:**
1. Bloom filter test (1ns) - reject if definitely not present
2. Check memtable (active + frozen) - recent writes
3. Index lookup (lock-free skipmap) - get segment position
4. Disk read (cached file handles) - retrieve blob

**Why this order:**
- Bloom first: Fastest, eliminates most work
- Memtable second: Recent writes (likely)
- Disk last: Expensive, only when necessary

**Cache semantics:** I/O errors treated as cache miss. Simplifies API - Get() returns (io.Reader, bool).

### Eviction Philosophy

**Why FIFO (not LRU):**
- Simpler (no access tracking)
- Faster (no bookkeeping on reads)
- Predictable (age-based, deterministic)
- Good enough for many workloads

**Why segment-level (not blob-level):**
- One file delete vs thousands
- Simpler index updates
- Matches RocksDB FIFO behavior (deletes entire SST)

**Why dual-mode (reactive + background):**
- Reactive: Immediate response to traffic spikes
- Background: Safety net for gradual growth
- Together: Robust against all patterns

**Eviction triggers degraded mode on error:**
- Index delete failure is fatal (consistency critical)
- File delete failure is logged (best-effort cleanup)

---

## Error Handling Design

### Degraded Mode (RocksDB-Inspired)

**Philosophy:** Graceful degradation better than hard failure.

**RocksDB approach:** Background errors → read-only mode, writes fail, requires Resume().

**Our approach:** Background errors → **memory-only mode**.

**Why better for caching:**
- Cache still useful even without persistence
- Writes continue (in-memory)
- Bounded memory (FIFO eviction of unflushed memtables)
- No recovery needed - cache works as RAM cache

**One-way door:** Once degraded, workers stopped permanently until cache restart. Simpler than recovery logic.

### Background Error Propagation

**Design principle:** Errors flow from workers → atomic state, not to callers.

**Why:**
- Put() is async - caller already returned when flush happens
- Can't surface async errors to synchronous API
- Degraded mode observable via BGError() for monitoring

**Worker behavior:**
- Flush worker: First error → set degraded, stop permanently
- Eviction worker: First error → set degraded, stop permanently
- All other workers: See degraded state, stop sending work

**Error categories:**

| Error Type | Severity | Action | Rationale |
|------------|----------|--------|-----------|
| Blob write failure | Fatal | Degraded mode | Can't persist data |
| Index update failure | Fatal | Degraded mode | Consistency critical |
| Eviction file delete | Logged | Continue | Index consistency more important |
| Eviction index delete | Fatal | Degraded mode | Index corruption unacceptable |
| Segment writer close | Logged | Continue | Footer write best-effort |

**Why segment writer close is non-fatal:**
- Segments are multi-GB, memfiles are smaller
- Worker keeps writer open across flushes (efficiency)
- Footer write failure → reads fail with I/O error (cache miss semantics)
- Acceptable for cache (not a database)

### Drain vs Close Semantics

**Design:** Explicit is better than implicit.

**Close() does NOT drain** - caller must call Drain() first if persistence needed.

**Why:**
- Matches RocksDB philosophy ("caller must first call SyncWAL()")
- Caller controls trade-off: persist vs fast shutdown
- Simpler worker logic (stop immediately on signal)

**Usage:**
```go
// Persist everything
cache.Drain()
cache.Close()

// Fast shutdown (abandon unflushed)
cache.Close()
```

**Degraded mode:** Drain() returns immediately (workers stopped, nothing to flush).

### Logging Strategy

**Why slog:** Standard library, structured logging, easy Datadog/OTEL integration later.

**Global logger:** Package-level `log` variable, configurable via SetLogger().

**Why global:**
- Avoids config dependency throughout code
- Single configuration point
- Can swap implementations (testing, production)

---

## Current Architecture Details

### Directory Structure

**Segment mode (current, only mode):**
```
/data/blobcache/
├── db/                    # Bitcask backing store
│   ├── 000000.data
│   └── 000000.hint
├── segments/
│   ├── 0000/              # Shard 0 (optional, default no sharding)
│   │   ├── 12345.seg      # Segment files with footers
│   │   └── 67890.seg
│   └── ...
└── .initialized           # Marker file
```

**Why no bloom.dat:** Bloom rebuilt on startup from index (fast enough).

**Why sharding optional:** Reduces directory size for filesystems with limits. Default: no sharding (simpler).

**Why Bitcask in db/:** Durable backing for in-memory skipmap index.

### Metadata Package

**Why separate package:** Type safety and encapsulation.

Segment footers contain structured metadata (BlobRecord, SegmentRecord). Encoding/decoding isolated in metadata package.

**Why important:** Enables format evolution without touching I/O code.

### Hash-Based Keys

**Why hash everywhere:** Simplifies architecture.

Keys hashed once (xxhash) on entry, uint64 used throughout:
- Bloom filter (hash-to-bit-positions)
- Index (skipmap key)
- MemTable (skipmap key)

**Why xxhash:** Fast, good distribution, production-proven.

**Trade-off:** Can't iterate by original key. Don't need it (point lookups only).

---

## Performance Analysis

### Benchmark Results

**AWS Graviton m7gd.8xlarge:**
- 32 cores, 128GB RAM, 1.7TB NVMe
- Disk: 536K read IOPS, 268K write IOPS
- No SLC cache - consistent sustained performance

**Test:** 2.56M operations (256K writes = 256GB, 2.3M reads)

| Metric | RocksDB | Blobcache | Improvement |
|--------|---------|-----------|-------------|
| Latency | 259µs | 93µs | 2.77× faster |
| Throughput | 0.38 GB/s | 1.04 GB/s | 2.77× faster |
| Memory | 100.6GB | 2.2GB | 45.7× less |
| CPU | 7.5 cores | 2 cores | 3.75× less |

**Disk bandwidth breakdown:**

| System | User Writes | Compaction | Total I/O | % Useful |
|--------|-------------|------------|-----------|----------|
| Blobcache | 1.04 GB/s | 0 | 1.04 GB/s | **100%** |
| RocksDB | ~0.4 GB/s | ~0.6 GB/s | ~1.0 GB/s | **40%** |

**Key insight:** RocksDB uses ~1 GB/s total I/O, but only 40% is new data - rest is compaction maintenance.

### Why Blobcache is Faster

**1. No compaction overhead** - All disk bandwidth for ingestion
**2. Single bloom filter** - 45,000× faster negative lookups (1ns vs 45µs)
**3. Simple appends** - No merge-sort operations
**4. Lock-free design** - Skipmap, atomic bloom filter

### Memory Usage

RocksDB used 100.6GB (78% of 128GB RAM) during benchmark.

**Possible reasons:**
- Bloom filters for 455 files loaded in memory
- SST metadata structures
- CGO overhead
- Features we haven't implemented

Blobcache used 2.2GB (1.7% of RAM).

**Why less:**
- Single bloom filter (not 455)
- No SST metadata overhead
- Simpler data structures

---

## Trade-offs & Limitations

### 1. NOT Battle-Tested

**Critical caveat:** Brand new system (December 2024).

RocksDB: 10+ years production, millions of deployments, all edge cases discovered.

Blobcache: Comprehensive tests, one production benchmark, **no production runtime yet**.

**Recommendation:** Gradual rollout with extensive monitoring, keep RocksDB as fallback.

### 2. Segment-Level Eviction Only

**Limitation:** Can't delete individual blobs within segment.

**Why:** Architectural choice for simplicity and performance.

**Consequence:**
- Evicting 2GB segment deletes all blobs in it (~10,000)
- Some might be newer/hotter
- Space not reclaimed until entire segment evicted

**Why acceptable:**
- RocksDB FIFO does the same (evicts entire SST)
- Cache semantics (imprecise eviction OK)
- Performance (one file delete, not 10,000)

### 3. FIFO Only (No LRU)

**Limitation:** Eviction by age, not access frequency.

**Why:** Simpler design, no access time tracking.

**Consequence:** Can't keep hot data and evict cold data.

**Not acceptable for:** Workloads with strong access patterns (80/20 rule).

**Acceptable for:** Temporal workloads where new = hot, old = cold.

### 4. Append-Only (No Updates)

**Limitation:** Can't update or delete individual blobs.

**Why:** Write-once architecture eliminates compaction.

**Consequence:** Space not reclaimed until segment evicted.

**Acceptable for:** Immutable data (logs, events, artifacts).

### 5. Degraded Mode Bloom False Positives

**Limitation:** Dropped memtables don't update bloom filter.

**Why:** Rebuilding bloom on every memtable drop is expensive (scan entire index).

**Consequence:** Bloom filter accumulates false positives in degraded mode.

**Why acceptable:**
- Degraded mode is emergency state
- False positives just cause extra disk lookups (cache miss)
- Alternative would slow down memtable eviction

---

## Configuration Philosophy

### Minimal Configuration

**Design goal:** Sensible defaults, few knobs.

**Core settings:**
- `WithMaxSize(bytes)` - Cache capacity limit
- `WithWriteBufferSize(bytes)` - Memtable size (default: 100MB)
- `WithSegmentSize(bytes)` - Target segment file size (default: 32MB)

**Advanced settings:**
- `WithShards(n)` - Filesystem directory sharding (default: 0 = none)
- `WithDirectIOWrites()` - Bypass OS cache (default: false)
- `WithChecksum()` - Enable CRC32 checksums (default: disabled)
- `WithVerifyOnRead(bool)` - Verify checksums on reads (default: false)
- `WithFDataSync(bool)` - Sync after writes (default: false, cache semantics)

**Why minimal:**
- Fewer ways to misconfigure
- Easier to understand
- Optimal defaults for most cases

**Removed configurations:**
- Storage strategy selection (always segments)
- Index type (always skipmap+bitcask)
- Eviction hysteresis (no longer needed)
- Bloom refresh interval (removed feature)

### Production Recommendations

**Linux production (logs caching):**
```go
cache, _ := New("/data/blobcache",
    WithMaxSize(1<<40),           // 1TB total
    WithSegmentSize(2<<30),       // 2GB segments
    WithWriteBufferSize(1<<30),   // 1GB memtable (match RocksDB)
    WithChecksum(),               // Detect corruption
    // No DirectIO - test both (page cache might help)
    // No fsync - cache can lose recent writes
)
```

**Why these choices:**
- 1GB memtable: Matches RocksDB production (960MB), balances memory and flush frequency
- 6 workers (default): Saturates disk bandwidth, bounded memory
- 2GB segments: Sweet spot (not too many files, not too coarse eviction)
- Checksums ON, Verify OFF: Detect corruption without read penalty
- No fsync: Speed over durability (cache semantics)

**DirectIO decision pending:** Need to benchmark with/without. Page cache might accelerate recent reads (45% of workload).

### Development Settings

**Mac development:**
```go
cache, _ := New("/tmp/blobcache",
    WithMaxSize(10<<30),          // 10GB (faster iteration)
    WithWriteBufferSize(100<<20), // 100MB (faster test cycles)
    WithSegmentSize(1<<30),       // 1GB segments
    // No DirectIO (Mac SLC cache is faster)
    // No checksums (faster development)
)
```

**Why different:**
- Smaller sizes for quick iteration
- No DirectIO (Mac has SLC cache, DirectIO actually slower)
- Trade accuracy for speed

---

## When To Use Blobcache

### ✅ Perfect For

- **Append-heavy workloads**: Logs, metrics, events, immutable artifacts
- **Large blobs**: 100KB-10MB where bloom filters provide value
- **High miss rates**: Bloom filter rejection is critical path
- **FIFO eviction acceptable**: Age-based, not access-based
- **Cache semantics**: Data loss acceptable, rebuild from source
- **Predictable performance**: No compaction surprises

### ❌ NOT Suitable For

- **Point updates or deletes**: Append-only architecture
- **Range queries**: Hash-based index, no ordering
- **Small values** (<10KB): RocksDB's block cache helps
- **LRU eviction required**: Only FIFO supported
- **Strong durability**: Cache can lose recent writes
- **Battle-tested requirement**: New system, not proven at scale

### 🤔 Consider Carefully

**vs RocksDB:**
- Use blobcache: Append-only, FIFO OK, 2-3× performance gain worth risk
- Use RocksDB: Need updates/deletes, range queries, battle-tested requirement

**Gradual migration recommended:**
1. Shadow production traffic (validate bloom FP rate, memory)
2. Gradual rollout with extensive monitoring
3. Keep RocksDB as fallback
4. Test both with/without DirectIO

---

## Open Questions & Future Work

### 1. DirectIO: Help or Hurt?

**Current:** Used in benchmark (matching RocksDB config).

**Question:** Does page cache help our workload?
- 45% reads are recent writes (likely in page cache)
- Buffered I/O might serve these from RAM
- Need benchmark: buffered vs DirectIO

### 2. Bloom Rebuild in Degraded Mode

**Current:** Dropped memtables don't update bloom (false positives accumulate).

**Alternative:** Rebuild bloom from memory on memtable drop.
- Pro: Accurate bloom filter
- Con: Expensive (scan all remaining memfiles)

**Decision:** Keep simple for now (emergency state, false positives acceptable).

### 3. Observability Integration

**Current:** slog logger with Error/Warn/Info levels.

**Future:** Integrate Datadog metrics/tracing.
- Count: degraded mode entries
- Count: memtable drops
- Histogram: flush latencies
- Histogram: eviction latencies

### 4. Sharding Strategy

**Current:** Optional, modulo-based (hash % shards).

**Question:** Is sharding still needed with segments?
- Per-blob mode: Yes (millions of files)
- Segment mode: Maybe not (hundreds of files)

**Decision:** Keep as option, default to none (simpler).

---

## Conclusion

BlobCache demonstrates that **specialized design beats general-purpose** for append-heavy caching:
- Eliminates RocksDB's compaction overhead entirely
- Single bloom filter (1ns vs 45µs negative lookups)
- Graceful degradation (memory-only mode on I/O errors)

**Production readiness:**
- ✅ Benchmark validated (2.77× faster)
- ✅ Comprehensive tests (including error scenarios)
- ✅ Degraded mode design (RocksDB-inspired)
- ❌ **Not battle-tested** (gradual rollout required)

**Bottom line:** For Datadog's logs caching use case, blobcache eliminates RocksDB's compaction tax. Worth the risk for 2.77× performance gain, but proceed carefully with monitoring and fallback plan.
