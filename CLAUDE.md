# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BlobCache is a **dual-purpose storage system** that operates in two distinct modes:

1. **Cache Mode**: High-performance disk-first cache with SIEVE eviction (default)
2. **CAS Mode**: Durable Content Addressable Storage via high-performance Write-Ahead Log

Both modes share the same unified log-structured architecture (Project Ferrum) where segments ARE the log, achieving write amplification of 1.00 by writing data exactly once.

## Core Principles

**Performance First:**
- Saturate NVMe bandwidth: 1.1-1.2 GB/s sustained (99% of hardware ceiling)
- Zero-copy abstractions: `ZeroCopyView` for read-after-write optimization
- Zero GC impact: Arena-backed structures invisible to garbage collector
- Lock-free hot paths: CAS reservations, atomic bloom filter, wait-free Librarian reads

**Correctness:**
- Sequence IDs prevent time-travel and check-then-act bugs
- Reserve-First pattern prevents WAL spillover bugs
- Header CRC prevents corrupt-size allocation panics
- Crash-only initialization for deterministic recovery
- Comprehensive testing: unit, integration, degraded mode, recovery, sequence ordering

**Code Quality:**
- Exceptional API design with clear configuration options
- Code clarity over cleverness
- Static analysis: `go vet`, `staticcheck`, `go fmt` on ALL files before commit
- **ZERO WARNINGS TOLERATED** - This means exactly what it says: NOT A SINGLE WARNING. No distinction between "new" vs "pre-existing" warnings. No excuses. If `go vet` or `staticcheck` produces ANY warning in ANY file, fix it before committing. The codebase must be warning-free at all times.

**Resource Management:**
- Minimal GC impact via `MmapPool` arena allocations
- Explicit reference counting with Go 1.24+ `runtime.AddCleanup`
- Bounded backpressure through channel-based flow control
- Direct I/O for writes (predictable memory), buffered I/O for reads (leverage kernel page cache)

**Error Handling (CRITICAL):**
- **NEVER ignore I/O errors** - not a single one, especially not `Close()`
- If error is benign (e.g., `PunchHole` failure), handle by logging at appropriate level
- Use `errors.Join` to preserve context when combining multiple error conditions
- All I/O paths must have explicit error handling with actionable outcomes

**Documentation Maintenance (REQUIRED):**
- **DESIGN.md is the authoritative architectural document**
- MUST be updated when making significant design changes:
  - Changes to data structures or on-disk formats
  - New algorithms or eviction strategies
  - Performance-critical code paths
  - Persistence layer changes (index, WAL, segments)
  - Concurrency protocols or correctness guarantees
- Document the "why" not just the "what" - explain trade-offs and alternatives considered
- Include performance implications and memory/disk overhead analysis where relevant
- Update section numbers and cross-references when adding new sections
- Mark outdated sections with "Note: This section may be outdated as of [date]" if uncertain

**When to update DESIGN.md:**
```
Examples requiring documentation:
✅ Changing segment size strategy (2GB → MemTable-sized)
✅ Adding tombstone incremental log with new key structure
✅ Modifying Relocate() to prevent ghost resurrection
✅ Changing I/O strategy (Direct vs Buffered)

Examples NOT requiring documentation:
❌ Fixing typos or formatting
❌ Adding logging statements
❌ Small refactors that don't change semantics
❌ Test additions (unless they validate new behavior)
```

## Build and Test Commands

### Running Tests

```bash
# Run all tests
go test ./...

# Run specific package tests
go test ./internal/index
go test ./internal/wal

# Run a single test
go test -run TestName
go test -run TestBlobCache_BasicOperations

# Run with race detector (ALWAYS run before commits for concurrent code)
go test -race ./...

# Run with verbose output
go test -v ./...

# Test for flaky tests (run same test N times)
go test -run TestName -count=100
go test -run TestCache_Compression_Zstd -count=10
```

*ALL* test results must be validated on a remote
linumx machine: workspace-yevgeniy-miretskiy-m7gd-8xlarge

Use ssh commands to execute tests and benchmark on linux.

### Code Quality Checks (REQUIRED before commits)

```bash
# Format all code
go fmt ./...

# Run vet (MUST pass with zero warnings)
go vet ./...

# Run staticcheck (install: go install honnef.co/go/tools/cmd/staticcheck@latest)
staticcheck ./...

# Full pre-commit check
go fmt ./... && go vet ./... && staticcheck ./... && go test -race ./...
```

### Primary Benchmark: `BenchmarkBlobCache`

This is the **most critical benchmark** for validating system behavior under realistic production load.

**What it does:**
- Each benchmark iteration (`-benchtime=XXXx`) represents **one write operation** of approximately 1MB
- Interspersed with each write are multiple read operations following a realistic access pattern
- Distribution: 10% writes, 40% hot reads (Zipfian), 25% cold reads (sequential), 25% misses

**Key characteristics:**
- Uses Zipfian distribution (s=1.1, v=1.0) where top 10-15% of keys account for 60-70% of accesses
- Blob sizes: 100KB to 2MB (randomized)
- Tests full system: writes, WAL, flushes, SIEVE eviction, hole punching, bloom filter performance
- Runs with WAL enabled in durable CAS mode
- Configured with `DegradedPanic` to crash on errors (no silent failures)

**Typical workloads:**

```bash
# Small test: ~10GB logical writes
go test -bench=BenchmarkBlobCache -benchtime=10000x | tee bench-10k.log

# Medium test: ~100GB logical writes (exercises eviction)
go test -bench=BenchmarkBlobCache -benchtime=100000x | tee bench-100k.log

# Large test: ~256GB (extended eviction + hole punching)
go test -bench=BenchmarkBlobCache -benchtime=256000x | tee bench-256k.log

# Full stress test: ~1TB (validates hole punching, stability, leak detection)
go test -bench=BenchmarkBlobCache -benchtime=1000000x | tee bench-1m.log
```

**IMPORTANT: Use `tee` to capture output**
Benchmark runs for extended periods (15-60+ minutes depending on iterations). The output contains:
- Progress updates roughly once per minute (heartbeat)
- Final latency histograms (p50/p99/p999 for GET and PUT)
- System metrics (RSS, disk utilization, throughput, sparse file ratios)

**Monitoring During Benchmark (CRITICAL):**

Open separate terminal windows to observe real-time system behavior:

```bash
# Terminal 1: Disk I/O utilization (updates every 5 seconds)
iostat -x 5

# Terminal 2: System statistics (updates every 5 seconds)
vmstat 5
```

**What to look for:**

**iostat expectations:**
- `%util` column: Should be consistently 95-100% (saturated NVMe)
- `r/s` + `w/s`: Total IOPS (operations per second)
- `rkB/s` + `wkB/s`: Actual hardware throughput
- `await`: Average I/O wait time (should be consistent, not spiking)

**vmstat expectations (Direct I/O writes, buffered reads):**
- `b` column (blocked processes): **Should be 0** - no kernel blocking with Direct I/O writes
  - If `b > 0` (e.g., 7-22): System hitting dirty page limits, thrashing
- `cs` column (context switches): 7k-15k/sec is normal, 40k+ indicates thrashing
- `free` column: Memory may decrease as kernel builds page cache for reads (this is expected and beneficial)
- `si`/`so` columns (swap): Should be 0 (no swap activity)

**Benchmark heartbeat output:**
The benchmark prints progress updates approximately once per minute with:
- `RSS`: Current memory usage (should stabilize around 5-6GB)
- `IO Depth`: Current I/O queue depth from iostat
- `Phys-Write`: Physical write throughput (GB/s)
- `Log-TP`: Logical write throughput (GB/s) - what application sees
- `Ratio`: Physical/Logical size ratio (measures hole-punch effectiveness)
- `Free`: Remaining disk space (safety check)

**Other Benchmarks:**

```bash
# SIEVE eviction performance (CPU-bound victim selection)
go test -bench=BenchmarkEviction_SieveVictimSelection -benchtime=1000000x
```

### Recovery Tool

```bash
# Build the recovery tool
go build ./cmd/blobcache-recover

# Run recovery (rebuilds index from segment files)
./blobcache-recover --recover --path=/path/to/cache
```

## Testing Methodology

### Principle: Real Components Over Mocks

**Prefer real implementations:**
- Avoid mocks whenever possible
- Use actual components with real behavior
- Integration tests provide more value than heavily mocked unit tests

**When abstraction is needed:**
- Introduce **small, focused interfaces** (e.g., `poolProvider`) only where they provide clear testing value
- Ensure abstractions don't impact performance (measure if uncertain)
- Wrap real components to override specific behaviors for tests (e.g., error injection)

**Example: `poolProvider` interface**

```go
type poolProvider interface {
    Acquire() (*MmapBuffer, error)
    Release(*MmapBuffer)
}
```

This allows testing memory pool edge cases without mocking the entire `MmapPool` implementation.

### Using TestingKnobs for Targeted Injection

`knobs.go` provides **TestingKnobs** for explicit failure injection and behavioral overrides:

```go
type TestingKnobs struct {
    OnFlushStart     func(slabID int)        // Hook before flush begins
    OnFlushFinish    func(slabID int)        // Hook after flush completes
    OnEvictBatch     func(n int)             // Monitor eviction
    ErrorOnFlushStart func() error           // Inject I/O failures
    BeforeIndexUpdate func()                 // Inject delays for concurrency testing
}
```

**Use TestingKnobs when:**
- Testing error paths (e.g., degraded mode on I/O failure)
- Validating concurrency behavior (e.g., sequence ordering)
- Observing internal state transitions (e.g., flush lifecycle)

**Do NOT use TestingKnobs for:**
- Mocking entire subsystems (wrap real components instead)
- Replacing core logic (defeats the purpose of integration testing)

### Test Categories (ALL must pass before commits)

1. **Unit Tests**: Component isolation (`internal/index/index_test.go`, `internal/wal/wal_test.go`)
2. **Integration Tests**: Full system flows (`segment_integration_test.go`)
3. **Degraded Mode**: Resilience validation (`degraded_mode_test.go`)
4. **Recovery**: Crash consistency (`recovery_test.go`)
5. **Sequence Order**: Concurrency correctness (`sequence_test.go`)
6. **Race Detection**: `go test -race ./...` (REQUIRED for all concurrent code)

### Formal Verification with TLA+

The `model/` directory contains TLA+ specifications for formally verifying critical protocols.

**Why TLA+?**
- Exhaustively explores ALL interleavings of concurrent operations
- Finds bugs that testing cannot (race conditions, ordering violations)
- Used by Amazon (DynamoDB, S3), Microsoft (Cosmos DB), MongoDB (Raft)

**Current Models:**
- `model/wal/WAL.tla` - WAL Group Commit protocol verification

**What the WAL model verifies:**
1. **Durability**: If `Write()` returns success, data is on disk
2. **Single Leader**: At most one goroutine flushes at a time
3. **No Lost Writes**: Acknowledged writes survive crashes
4. **Liveness**: Pending writes eventually complete (no deadlock)

**Running the model:**
```bash
# Install TLA+ tools
brew install tla-plus/tap/tla-plus  # macOS
# Or download from: https://github.com/tlaplus/tlaplus/releases

# Run model checker
cd model/wal
java -jar tla2tools.jar -config WAL.cfg WAL.tla
```

**When to update/create TLA+ models:**
- New concurrent protocol (leader election, group commit, etc.)
- Changes to existing protocol semantics
- Bug found in production → reproduce in model, verify fix

**When NOT needed:**
- Refactoring that doesn't change protocol behavior
- Adding logging or metrics
- Performance optimizations with same semantics

See `model/README.md` for a comprehensive beginner's guide to TLA+.

## High-Level Architecture

BlobCache implements a tiered storage pipeline optimized for zero-copy reads and minimal GC pressure:

### 1. Memory Tier (Hot Path - Zero GC)

**MemTable** (`memtable.go`):
- Orchestrates lock-free slab reservations using CAS operations
- Manages hardware-aligned slabs (`MmapBuffer`) sized to match `WriteBufferSize` (default: 128MB)
- Allocated via `mmap` (GC-invisible arenas)
- Backpressure via bounded `MmapPool` channels (self-regulating)
- Virtual interleaving for XL writes (>WriteBufferSize) without double-writes
- Reserve-First pattern for WAL integration (prevents spillover bug)

**Librarian** (`librarian.go`):
- Lock-free read-after-write cache maintaining ~1GB of recently written data
- Uses atomic slice pointers for wait-free reads during concurrent writes
- Zero-copy access via `ZeroCopyView` with reference counting
- Reference management via Go 1.24+ `runtime.AddCleanup` (safe munmap)

### 2. Index Tier (Control Plane - Minimal GC)

**BlobIndex** (`internal/index/`):
- 256-sharded arena-backed hash table for O(1) lookups
- Arena design: nodes use `uint32` indices instead of pointers → zero GC scan overhead
- SIEVE eviction algorithm (cache-conscious, faster than LRU for high-volume blobs)
- Bitcask-style persistence for crash recovery
- Memory overhead: ~76 bytes per item (predictable, scales with item count not storage size)

**Key Design Insight:**
- RAM constraint = item count, not total storage size
- For 1TB @ 128KB/blob: ~8M items = ~608MB RAM (disk-bound)
- For 1TB @ 4KB/blob: ~250M items = ~19GB RAM (RAM-bound)

### 3. Disk Tier (Cold Storage - Hybrid I/O)

**Segments** (`segmentio.go`, `internal/record/`):
- MemTable-sized files (~128MB, matching `WriteBufferSize`)
- Smaller segments enable: WAL-rename efficiency (zero-copy), fast recovery, effective compaction
- Unified binary format (v2) with **Header CRC** (prevents corrupt-size allocation panics)
- Self-describing footers + companion `.iseg` files (snapshot index for disaster recovery)
- **Write path**: Direct I/O (`O_DIRECT` on Linux, `F_NOCACHE` on Darwin) bypasses kernel page cache
- **Read path**: Buffered I/O (leverage kernel page cache - "it'd be the height of hubris to assume we can do better")
- Hole punching (`FALLOC_FL_PUNCH_HOLE`) for fine-grained space reclamation

**Archivist** (`archivist.go`):
- Read-only access to persisted segments
- Index items point to exact byte offsets (no scanning, no magic)
- **Safety Protocol**: Verify Header CRC BEFORE allocating buffer based on PhysSize

**Segment Index Files (`.iseg`)**:
- Companion files for each segment containing snapshot of index data
- Same format as Bitcask durable index
- Enable disaster recovery if Bitcask index becomes corrupted
- Written atomically during segment flush

### 4. Durability Layer (CAS Mode)

**Write-Ahead Log** (`internal/wal/`):
- Transforms cache into durable Content Addressable Storage
- **Group commit architecture**: Amortizes fsync across concurrent writers (~10-15% overhead vs 50%+ naive)
- **WAL-rename strategy**: Active WAL becomes segment file (zero double-writes, 1.00× write amp)
- Leader election via single `writerBusy` atomic bool
- Crash-only initialization: Two-attempt loop with automatic recovery

### 5. Fast Rejection Layer

**Unified Bloom Filter** (`bloom/`):
- Single filter for ALL keys (O(1) miss rejection vs O(N) for LSM per-SST filters)
- Full 128-bit XXH3 entropy prevents "32-bit funnel" collisions at scale
- Lock-free atomic operations for concurrent updates
- Proactive rebuilds when ghost entries exceed 10% or FPR spikes
- 45,000× faster negative lookups than RocksDB's distributed filters

### 6. Space Management

**Eviction** (`internal/index/index.go` - SIEVE implementation):
- **Hybrid strategy**: Random Greedy (<64KB targets) or Proportional Fair (≥64KB targets)
- Three-phase lifecycle: Selection (SIEVE scan) → Commit (Bitcask) → Reclamation (hole punch)
- Bounded lock hold times: Max 64 items per shard (~13µs) prevents priority inversion

**Compaction** (`compaction.go` - IN PROGRESS):
- **Current**: Hole punching reclaims space within segments without rewriting
- **Planned**: Merge sparse segments when fullness drops below threshold (~25%)
  - Take ~4 sparse segments and merge into 1 dense segment
  - Must bypass page cache for efficiency (likely Direct I/O)
  - Heuristic should account for explicit deletions in CAS mode
  - Tombstone cleanup to prevent indefinite accumulation
- **Strict contiguity rule**: Only merge adjacent segment ranges (prevents leapfrog hazard)
- **Tail GC**: Tombstones only dropped when compacting oldest (tail) segment
- Atomic `Relocate` with CAS: Concurrent writes to newer segments safely skip relocation

## Missing/In-Progress Features

1. **Efficient Delete Support**:
   - With and without WAL
   - Tombstone tracking for CAS mode
   - Compaction must garbage-collect tombstones

2. **Segment Merging**:
   - Trigger when segment is mostly empty (e.g., <25% full due to hole punching)
   - Merge ~4 sparse segments into 1 dense segment
   - Must be extremely efficient and bypass page cache (Direct I/O likely)

3. **Compaction Heuristics**:
   - Account for explicit deletions in CAS mode
   - Prevent indefinite tombstone accumulation
   - Balance space reclamation vs I/O overhead

4. **Prefetch API** (under consideration):
   - Allow caller to advertise `Prefetch([]blob_ids)`
   - Use `fadvise` to start pre-fetching needed pages
   - Middle ground between "trust Linux fully" and "implement custom page cache"

## Critical Implementation Details

### Sequence IDs and Write Ordering (Correctness Guarantees)

Every write gets a monotonic `SeqID` (initialized from `time.Now().UnixNano()` at startup). Two guards prevent stale writes from corrupting the cache:

1. **Lifecycle Guard**: Tracks `maxSealedSeq` → rejects writes that started before rotation but land after
2. **Concurrency Guard**: 256 sharded locks serialize same-key updates with SeqID comparison

**Cost**: ~100ns per write (one atomic increment + sharded lock). **Benefit**: Zero read-path overhead while guaranteeing latest-write-wins semantics.

### Reserve-First Pattern (WAL Integration)

WAL writes use Reserve-First to prevent the **Spillover Bug**:

```text
CORRECT (Reserve-First):
1. LOCK: Reserve slab position, increment seqID, check rotation
2. UNLOCK
3. WAL write (I/O outside lock)
4. Fill reserved buffer
5. LOCK (sharded): Index update with SeqID check

WRONG (WAL-First):
Writer A writes to WAL N → slab rotates → lands in new slab →
WAL N flushed and deleted → DATA LOSS
```

Reserve-First guarantees WAL file and slab are always paired for 1:1 recovery.

### XL Write Handling (Virtual Interleaving)

Blobs exceeding `WriteBufferSize` use Virtual Interleaving to avoid double-writes:

1. **Zero-width reservation**: Reserve position at page boundary, increment `xlSize`, NO memory allocation yet
2. **Unlocked allocation**: Allocate standalone `XLBuf` (mmap with header padding) outside lock
3. **Timeline embedding**: Attach to slab's index entry as "virtual" insertion point
4. **Physical merge**: During flush, XL payloads interleaved at reserved positions
5. **Rotation threshold**: Trigger rotation when cumulative `xlSize` exceeds 2× buffer size

**Result**: XL data inline in segment (not sidecar files), RSS returns to baseline after flush, zero double-writes.

### Deletion Model (Tombstones)

BlobCache uses **soft deletes** to prevent the "leapfrog hazard":

```text
Timeline: T1: K in Seg A | T2: Delete K (tombstone) | T3: Re-write K in Seg C

WRONG (no tombstones): Compact A+C skipping B → tombstone "wins" → K deleted!
CORRECT (tombstones): Preserve tombstone in non-tail compactions → newer write supersedes
```

Lifecycle: `Delete(key)` → WAL append (if enabled) → Hole punch (immediate space reclaim) → Mark tombstone → Compaction drops (tail only)

### Hybrid I/O Strategy

**Current approach: Direct I/O writes, buffered reads**

**Write path (Direct I/O):**
- Predictable memory: Fixed arena, not kernel monopoly
- System isolation: No OOM, no swap activation, no kswapd cliffs
- Deterministic flow control: Go channels, not kernel blocking
- Sustained saturation: 100% disk utilization without GC-induced pauses

**Read path (Buffered I/O):**
- Leverage kernel page cache expertise ("height of hubris to assume we can do better")
- Kernel handles LRU, read-ahead, prefetching
- Memory usage grows as kernel caches hot data (expected and beneficial)

**Future consideration:**
- Optional `Prefetch([]blob_ids)` API using `fadvise` to hint kernel
- Allows application-level knowledge without reimplementing page cache

## Error Handling Standards

### NEVER Ignore I/O Errors

**Every I/O operation must have explicit error handling:**

```go
// WRONG - silently ignoring Close error
defer file.Close()

// CORRECT - handle Close errors
defer func() {
    if err := file.Close(); err != nil {
        // At minimum, log the error
        slog.Error("failed to close file", "path", path, "error", err)
        // If in a function that returns error, preserve it
        if retErr == nil {
            retErr = err
        } else {
            retErr = errors.Join(retErr, err)
        }
    }
}()
```

### Benign vs Critical Errors

**Benign errors** (operation can continue, but should be logged):
- `PunchHole` failure (space not reclaimed, but data still accessible)
- `Fadvise` failure (hint ignored by kernel, performance impact only)

```go
if err := sys.PunchHole(fd, offset, length); err != nil {
    slog.Warn("hole punch failed, space not reclaimed",
        "segment", segID, "offset", offset, "error", err)
    // Continue - index still updated, blob logically deleted
}
```

**Critical errors** (must propagate or trigger degraded mode):
- Write failures (data loss risk)
- Index persistence failures (crash recovery compromised)
- Segment file corruption (Header CRC mismatch)

### Preserving Error Context with errors.Join

When multiple errors can occur in cleanup paths, use `errors.Join`:

```go
func (c *BlobCache) Close() error {
    var errs []error

    if err := c.memtable.Drain(); err != nil {
        errs = append(errs, fmt.Errorf("drain memtable: %w", err))
    }

    if err := c.index.Close(); err != nil {
        errs = append(errs, fmt.Errorf("close index: %w", err))
    }

    return errors.Join(errs...)
}
```

## Configuration Patterns

### Cache Mode (High Throughput, SIEVE Eviction)
```go
cache, _ := blobcache.New("/data/cache",
    blobcache.WithMaxSize(1<<40),           // 1TB capacity
    blobcache.WithWriteBufferSize(1<<30),   // 1GB slabs
    blobcache.WithMaxInflightSlabs(8),      // 8GB total buffer
    blobcache.WithMaxCachedSlabs(8),        // ~8GB read-after-write cache
    blobcache.WithFlushConcurrency(4),      // 4 parallel I/O workers
)
```

### CAS Mode (Durable Content Addressable Storage)
```go
cache, _ := blobcache.New("/data/cas",
    blobcache.WithWAL(),                    // Enable durability
    blobcache.WithChecksum(),               // CRC32 validation
    blobcache.WithMaxSize(1<<40),
    blobcache.WithWALFlags(sys.FlDirectIO | sys.SyncData), // fdatasync (default)
)
```

### High-Compression CAS
```go
cache, _ := blobcache.New("/data/cas",
    blobcache.WithWAL(),
    blobcache.WithCompression(compression.CodexZstd),
    blobcache.WithCompressionLevel(compression.CompressionSpeed),
    blobcache.WithCompressionMinSize(4096), // Skip small blobs
)
```

## Common Gotchas and Best Practices

1. **Index Memory Calculation**:
   - RAM scales with **item count**, not storage size
   - Formula: `ram_mb = (capacity_mb / avg_blob_size_mb) * 0.000076`
   - Example: 4TB @ 128KB/blob = 31.25M items = ~2.4GB RAM

2. **Header CRC Validation**:
   - **ALWAYS** verify `HeaderCRC` before trusting `PhysSize` for allocation
   - Prevents corrupt-size panic attacks
   - See `Archivist.ReadBlob` safety protocol

3. **XL Rotation Threshold**:
   - Workloads with frequent XL writes need `xlSize >= 2× WriteBufferSize` check
   - Prevents never-rotating slabs

4. **Compaction Contiguity**:
   - **ONLY** merge contiguous segment ranges `[A, B]` or `[A, B, C]`
   - Never skip segments (prevents leapfrog hazard with tombstones)

5. **Segment Size Rationale**:
   - Segments match MemTable size (~128MB), NOT large 2GB+ files
   - Enables WAL-rename efficiency (zero-copy promotion)
   - Faster recovery (smaller files to scan)
   - More effective compaction (easier to find 4 sparse segments to merge)

6. **WAL Recovery**:
   - Uses crash-only initialization with 2-attempt loop
   - DO NOT bypass this pattern or manually delete WAL files

7. **Reference Counting**:
   - Slabs use Go 1.24+ `runtime.AddCleanup` for safe munmap
   - DO NOT manually `munmap` buffers with active I/O or user handles

8. **Lock Hold Times**:
   - Eviction limits work to 64 items per lock hold (~13µs)
   - Never hold global MemTable lock during I/O (use Reserve-First)

9. **Static Analysis**:
   - Run `go vet` and `staticcheck` before EVERY commit
   - Address ALL warnings (zero tolerance)

10. **Error Handling**:
    - Never ignore I/O errors, especially `Close()`
    - Use `errors.Join` to preserve context in cleanup paths
    - Log benign errors, propagate critical errors

11. **NEVER run `go clean -cache`**:
    - Rebuilding the cache is extremely expensive, especially on remote machines
    - If you suspect stale cache, ask the user first

## Zero-Copy Abstractions

**ZeroCopyView** (returned by `Get()` in Librarian hits):
- Direct pointer into mmap'd slab buffer
- Reference counted via `runtime.AddCleanup`
- Caller must call `Close()` to release reference
- No memory copies between storage and application

**Performance Impact**: Read-after-write hits served at ~7.8µs p50 (pure RAM pointer arithmetic)

## Package Structure

```
blobcache/
├── blobcache.go           # Main cache/CAS implementation
├── memtable.go            # Write buffering, slab management, Reserve-First
├── archivist.go           # Segment file reading with Header CRC validation
├── librarian.go           # Lock-free read-after-write cache (ZeroCopyView)
├── compaction.go          # Space reclamation (hole punch + merge - IN PROGRESS)
├── recovery.go            # Index reconstruction from segment footers
├── segmentio.go           # Segment I/O (Direct write, buffered read)
├── options.go             # Configuration API (dual-mode support)
├── knobs.go               # Testing hooks for error injection
├── internal/
│   ├── index/             # Sharded arena index (GC-invisible) + SIEVE
│   ├── record/            # Unified binary format v2 (Header CRC)
│   ├── wal/               # WAL with group commit and leader election
│   ├── sys/               # Platform-specific I/O (Linux/Darwin)
│   └── xmap/              # Memory alignment utilities
├── bloom/                 # Unified lock-free bloom filter
├── compression/           # Compression codecs (Zstd, LZ4, S2)
├── base/                  # Error types and constants
└── model/                 # TLA+ formal verification specs
    └── wal/               # WAL Group Commit model
```

## Performance Expectations

**Benchmark: 1M ops on AWS m7gd.8xlarge (32 vCPUs, NVMe)**

| Metric | Cache Mode | CAS Mode (WAL) | Notes |
|--------|------------|----------------|-------|
| Logical Throughput | 1.2 GB/s | 1.1 GB/s | 99% of hardware ceiling |
| Physical Write | 1.2 GB/s | 3.4 GB/s | CAS: WAL + segments pre-eviction |
| Write Amplification | 1.00× | 1.00× | Zero double-writes (rename strategy) |
| GET p50/p99/p999 | 7.8µs / 127µs / 554µs | Same | Librarian zero-copy |
| PUT p50/p99/p999 | 28ms / 54ms / 62ms | Same | Network-bound |
| Peak RSS | 5.76 GB (stable) | Same | GC-invisible arenas |
| GC Overhead | 0% | 0% | Arena design |

**GC Impact**: With 5.79 GB RSS, GC sees only ~112 MB of live heap. The ~5.5 GB difference is `MmapPool` slabs—completely invisible to Go's runtime.

## Design Documentation

See `DESIGN.md` for comprehensive details on:
- Memory hierarchy rationale and Direct I/O empirical comparison
- Bloom filter scaling analysis and full 128-bit entropy benefits
- Write amplification verification (WAL rename vs naive double-write)
- SIEVE eviction algorithm implementation
- Comparison with RocksDB FIFO (3.2× faster) and Foyer (20% faster)
- GC invisibility validation with iostat correlation

**Note**: Some sections of DESIGN.md may be outdated regarding segment sizes (references to 2GB+ segments). The current implementation uses MemTable-sized segments (~128MB) for the reasons outlined above.
