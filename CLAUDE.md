# CLAUDE.md

This file provides guidance for work in this repository.

## Architecture and invariants

BlobCache is a cache of large immutable blobs. Cache failures cost misses,
never unverified data. The two layers have distinct ownership:

- `blobstore`: record I/O and verification, segment files, rotation, sealing,
  descriptor caching, disk accounting and FIFO segment eviction. It has no
  key lookup index and knows nothing about the caller's index shards.
- `blobcache`: a sharded key index and lazy, bounded mmap chunks divided into
  memory blocks. Alloc bumps contiguous page-aligned records into an active
  block; Put transfers its pin to the write, and Get pins a block during its
  immutable-value callback. Full blocks are evicted oldest first regardless of
  pins. One atomic reference count per block includes the cache's reference;
  eviction sets its sign bit to refuse new pins. Index cleanup drops the cache
  reference; the last outstanding pin releases storage synchronously. No
  repeated search for unpinned victims. Descriptors are never reused. Per-block key
  lists drive conditional index cleanup, grouped by shard outside allocator
  locks, before storage reuse. No payload copies or memory eviction worker.
  Oversized records use dedicated mappings under the same budget. Empty backing
  chunks can be unmapped to make room. Normally blocks are 4 MiB, chunks 16 MiB;
  small budgets scale both down. New mappings are prefaulted outside the lock.
  Mapped/reserved backing bytes include free slots; metadata is additional.
  Concurrent cold reads share one buffer and one pin until all callbacks return;
  the leader performs the read without spawning a goroutine.

The record is `[value | padding | key | trailer]`. The trailer contains both
metadata CRC32C and a mandatory value CRC32C. The initial format uses uint64 segment IDs,
16-digit hex filenames under 256 shard directories, uint32 offsets/sizes, and
32-byte footer tails. This system has not shipped; do not maintain compatibility
with discarded development layouts.

Store owns routing and one global segment registry. Each ioQueue owns one
scheduler, local virtual slots, an append stream and/or read-handle cache.
WithRings defaults to 1; WithDedicatedWriteRings reserves write queues, leaving
the rest for reads. Keys route to writers; segment IDs route to readers. Queue
roles are fixed while open and are not persisted. Linux coordinators use dedicated
OS threads and request affinity to distinct allowed CPUs; failures are logged
and DIO continues without the requested affinity. DIO remains one ring per scheduler. No global
dispatcher, fd hashing, descriptor migration or cross-ring chain execution.
Read/write budget shares are static, split by the number of queues serving each
class. WithIOBudget sets the per-class modeled device-time allowance (default
1.5 ms); zero disables budgets without changing ring capacity or file ordering.
The read-handle limit is total, not per ring.

Each producer's active mutex orders reserve, rotation and Submit. The last write
**decides and submits its seal under that mutex**: last write, immutable footer,
fdatasync, slot close, directory fsync. Hard links attempt every seal/cleanup
operation even after errors. The next segment opens independently in another
slot; it never waits for the preceding seal. Partial segments seal on Close.
A footer lists reservations, not proof that all writes succeeded. Reads verify
records independently. There is no segment poisoning or mutable footer handoff.
A synchronously rejected seal must submit a close to drain earlier operations
before releasing its slot. Close owns cleanup if the scheduler rejects that too.

The seal completion callback records an error metric, marks completion and
releases the slot. It takes no locks and performs
no filesystem work. Value write tickets are consumed by blobcache's completer.
A failed write leaves its valid memory value usable. Put publishes the reserved
Location immediately, with a pending bit until I/O completes. Memory eviction
clears slices immediately; pending records become misses until the completer
marks their locations readable. Failed records still require read verification.
Block eviction clears memory slices conditionally on block identity. Late
publications cannot restore retired memory or an evicted disk location. The
completer records errors, clears pending only for the same location, and releases
write pins. Put retains caller ownership on ErrBusy, so retries reuse the buffer.

Read queues divide exactly WithMaxReadHandles slots; each has a map and CLOCK.
Lookup, open-with-initial-read, subsequent reads and replacement submissions
all occur under its mutex. dio orders execution after earlier opens and before
replacement closes. Waiting and verification occur outside the mutex. No spare
slots or pins. Pending opening/replacement chains cannot themselves be replaced.
Read buffers have their trailers cleared before submission, so a short initial
read cannot validate stale buffer contents when the chain ticket's byte count
belongs to its open. Errors unmap only the same slot generation. Retirement submits closes without
waiting. A pending initial open is closed by its reader; a pending replacement
already queued its old descriptor's close. CLOCK reuses closing slots after
their tickets complete.

WithMaxSegments or WithMaxSize enables FIFO batch eviction; choose one.
The global registry receives producer creation/growth under its mutex and seal
completion through an atomic flag. No lifecycle channel or worker. Its mutex
protects IDs and disk reservations, and ordinary appends do not take it.
Creation/growth selects completed prefixes and immediately releases their
logical reservations. After releasing store locks, the caller retires read
handles, submits best-effort unlinks through write-capable queues, and
notifies the index without waiting for I/O. No eviction goroutine, evicting
flag, retry queue, worker loop, ticker or lifecycle channels. The open WaitGroup joins writes only.
Compact the segment slice in place with slices.Delete, preserving capacity and clearing retired pointers.

Keep one segment of headroom; eviction targets 80% usage or two segments of
headroom, whichever leaves more room. Normally remove at least two segments;
a single completed victim may be necessary to admit a blocked reservation.
Never skip a segment still being written. If a failed reservation is blocked
by a quiet producer's active segment, release all locks and ask that producer
to seal it; return ErrBusy while I/O finishes. Acquire at most one producer
lock at a time, and never acquire one under the registry lock. Two segment
allocations per producer are the minimum bounded budget.
DiskBytes counts retained segments
and reservations; pending/failed unlinks can make physical usage larger.

Eviction is a slice of segment IDs. The callback owns it and is invoked on the
caller's goroutine outside store locks; callers may schedule work elsewhere.
Concurrent notifications may arrive out of order. The index callback starts
its own goroutine and sweeps each shard once, deleting the retired prefix.
The existing shard lock protects a monotonic boundary that rejects delayed write publications. Blobstore does
not reread footers or know the index's shard layout. Its read cache protects
its boundary under its existing mutex to prevent reopening retired handles.
Recovery runs before writes; Close must not race public calls. Eviction
callbacks must not call Close or ReadIndex.

Keep dio extensions minimal. Its Submit is nonblocking on Linux, but the POSIX
scheduler executes inside Submit on macOS; submission locks serialize that
development path. Ticket.Wait uses a WaitGroup; Ticket.Done allocates a channel
only when selectable completion is requested. User callbacks cannot retain
lent value slices. Even a panicking callback must release its memory pin.

## Engineering rules

- Preserve zero-copy value I/O and short hot-path critical sections.
- Never ignore I/O errors: return them or account for them in metrics/logging.
- Keep this file, README and API comments current with design changes.
- Keep the dev loop short: default to targeted normal tests. Use Linux when
  needed for io_uring; reserve race runs for concurrency validation and
  benchmarks for deliberate performance work, not every edit.
- Test concurrency, corruption, recovery, failures and resource lifetimes with
  real files/schedulers and deterministic submission hooks where needed.
- Run gofmt, go vet, staticcheck, and race tests before committing; no warnings.
- Do not commit unless asked. Preserve the user's existing staged deletions and
  untracked rewrite. Do not include local/, .codegraph/ or .claude.json.

## Build and Test Commands

### Running Tests

```bash
# Run all tests
go test ./...

# Run a single test
go test -run TestReadWhileWriteInFlight

# Run with race detector (ALWAYS run before commits for concurrent code)
go test -race ./...

# Test for flaky tests (run same test N times)
go test -run TestConcurrentStress -count=100
```

*ALL* test results must be validated on a remote
linux machine: workspace-yevgeniy-miretskiy-m7gd-8xlarge

Use ssh commands to execute tests and benchmark on linux. On macOS the store
uses dio's POSIX scheduler (synchronous, chosen at build time), so the tests
run there too, but only Linux exercises io_uring and O_DIRECT alignment
(`TestUsesIOUring` is Linux-only).

### Code Quality Checks (REQUIRED before commits)

```bash
# Format all code
go fmt ./...

# Run vet (MUST pass with zero warnings)
go vet ./...

# Run staticcheck (install: go install honnef.co/go/tools/cmd/staticcheck@latest)
staticcheck ./...

# Full pre-commit check (run on macOS and on Linux)
go fmt ./... && go vet ./... && staticcheck ./... && go test -race ./...
```

`GOOS=linux go vet ./... && GOOS=linux staticcheck ./...` also works on macOS
(the module has no cgo dependencies); tests still need the Linux box.

### Primary Benchmark: `BenchmarkBlobCache`

This is the **most critical benchmark** for validating system behavior under realistic production load.

**What it does:**
- Each benchmark iteration (`-benchtime=XXXx`) represents **one write** of 100 KB–2 MB (~1 MB average)
- Interspersed with each write are reads: 30% writes, 30% hot reads (Zipfian, s=1.1, over the newest keys), 30% cold reads (4 consecutive keys), 10% misses; `BLOBCACHE_WRITE_PERCENT=w` changes the write share
- Writes download into cache memory like a caller would: `Alloc`, copy test data in (standing in for the download), `Put`; on `ErrBusy` writers keep the buffer, wait for that worker's previous write ticket, and retry (allocation and Put busy counts are separate). Reads lend the value to a callback that only measures it. `Drain` runs inside the timed region
- Every read's latency is recorded by where it was served: memory (`GET-memory`), disk (`GET-disk`), or a miss (`GET-miss`)
- `BLOBCACHE_CACHE_MEMORY_MB=m` sets the cache memory (default 1024); `BLOBCACHE_NO_MEMORY_HITS=1` serves every read from disk with the same code (the disk-only comparison)
- Reads use O_DIRECT; `BLOBCACHE_BUFFERED_READS=1` switches them to the page cache; `BLOBCACHE_PARALLELISM=p` runs p workers per CPU (Get is synchronous, so workers bound the reads in flight)

**Typical workloads:**

```bash
# Small test: ~10GB logical writes
go test -bench=BenchmarkBlobCache -benchtime=10000x | tee bench-10k.log

# Medium test: ~100GB logical writes
go test -bench=BenchmarkBlobCache -benchtime=100000x | tee bench-100k.log

# Full stress test: ~1TB (configure WithMaxSize or provide the disk space)
go test -bench=BenchmarkBlobCache -benchtime=1000000x | tee bench-1m.log

# Per-operation CPU and allocation cost
go test -run xxx -bench=BenchmarkPutGet -benchmem
```

**IMPORTANT: Use `tee` to capture output**
Benchmark runs for extended periods (15-60+ minutes depending on iterations). The output contains:
- A heartbeat every 30 seconds
- Final latency histograms (p50/p99/p999 for GET and PUT)
- Cache counters (items, segments, failed segments, disk/memory hits, corruption, write errors)

**Monitoring During Benchmark (CRITICAL):**

Open separate terminal windows to observe real-time system behavior:

```bash
# Terminal 1: Disk I/O utilization (updates every 5 seconds)
iostat -x 5

# Terminal 2: System statistics (updates every 5 seconds)
vmstat 5
```

**iostat observations:**
- `%util` column: Time with outstanding work; interpret alongside throughput and queue depth, not as proof of an NVMe device's bandwidth ceiling.
- `r/s` + `w/s`: Total IOPS (operations per second)
- `rkB/s` + `wkB/s`: Actual hardware throughput
- `await`: Average I/O wait time (should be consistent, not spiking)

**vmstat and thread observations:**
- `b` can include kernel I/O workers even with O_DIRECT; it does not by itself identify dirty-page throttling.
- Compare context switches with throughput and CPU cost; no fixed count diagnoses thrashing.
- Check RSS against the configured memory budget plus metadata; buffered reads also consume page cache.
- Check `si`/`so` for swapping.
- Use Go schedtrace for runtime thread counts and `/proc/PID/task` to identify kernel `iou-*` workers separately. Pinned coordinators own threads, not entire CPU cores.

**Benchmark heartbeat output:**
- `MEM`: RSS and writes in flight (RSS should stay near the workers' buffers + index)
- `DISK`: utilization, physical read/write throughput, free space
- `TPUT`: logical write/read throughput — what the application sees
- `READS`: read rate, hit rate, hits and misses
- `CACHE`: items, segments, failed segments

## Testing Methodology

### Principle: Real Components Over Mocks

**Prefer real implementations:**
- Avoid mocks whenever possible
- Use actual components with real behavior: real files, real schedulers, real pools
- Integration tests provide more value than heavily mocked unit tests

**When interposition is needed:** wrap the real component and override one
behavior. The only hook is `blobstore.TestingWithPreSubmit` (unexported
`withPreSubmit` in blobcache), which sees every operation the store is about
to submit and returns the one to submit — for example blobcache's `gate`
holds a write before submission to test reads of writes in flight, and the
writes-in-flight bound, deterministically, and the store's `failSubmission`
swaps one write for one that fails, to test independent record recovery. Seal
completion functions are attached after the hook.

Store tests use a recorder to check what ReadIndex reports.

**Crash simulation:** close the cache, then edit the on-disk state the way a
crash would leave it (for example, zero the active segment's footer), and
reopen. Corruption tests flip bytes in segment files.

### Test Categories (ALL must pass before commits)

1. **Format**: record framing and trailer, footer encoding, corruption
2. **Round trips and persistence**: sizes across page boundaries, reopen across segments, overwrite
3. **Eviction**: byte bounds, FIFO callbacks, retired descriptors, corrupt footers, unlink failures and delayed index publication
4. **Crash and failure**: an unsealed segment is discarded on reload, sealed ones survive; a segment's file is created by its first write, so a store closed unwritten leaves none; a failed record does not poison successful peers; missing/corrupt records are rejected on read
5. **Memory**: a value is served from memory once Put returns; reads from disk are kept; memory is evicted oldest first, immediately refusing new pins; physical reuse waits for existing pins (`ErrBusy` when capacity remains held); Alloc/Put/Free each hand memory back once and refuse foreign memory; Close reports memory still held
6. **Corruption**: value checksum and trailer failures become misses
7. **Concurrency**: mixed workload with rotation under `-race`

### Formal Verification with TLA+

There are no TLA+ models at present (the WAL group-commit model left with the
WAL). Consider one for a new concurrent protocol, or when changing the
last-write sealing, retirement boundaries, or descriptor submission ordering.

## Package Structure

```
blobcache/
├── blobcache.go     # Cache, New/Close, Drain, Stats, completer
├── io.go            # Alloc/Free/Put/Get and shared cold reads
├── memory.go        # Lazy backing chunks, block pins and reclamation
├── inflight.go      # Queue of writes in flight, for the completer and Drain
├── index.go         # Sharded index (key hash → memory and/or Location)
├── options.go       # Configuration (store options pass through)
├── blobstore/       # Pure I/O layer
│   ├── store.go     #   Common Store, Open/ReadIndex/Close, routing and Location
│   ├── io.go        #   Per-ring scheduler, append stream, rotation and sealing
│   ├── segment.go   #   Segment paths and lifecycle metadata
│   ├── read.go      #   Fixed-slot descriptor cache and ordered read submissions
│   ├── eviction.go  #   FIFO disk budget and segment-level callbacks
│   ├── scheduler_*.go #  io_uring on Linux, POSIX elsewhere
│   ├── format.go    #   Record framing and trailer, RecordSize, segment footer
│   └── options.go   #   Configuration
└── internal/xmap/   # Sharded map used by the index
```

## Common Gotchas and Best Practices

1. **Memory has one owner at a time.** Memory from Alloc is the caller's
   until Put or Free takes it back; Put returning ErrBusy leaves it with the caller.
   After a transfer the caller must not touch it.
   A value lent by Get is valid only inside the callback. Every pin taken
   (Alloc, a write in flight, a Get) must be released, or its block cannot be
   physically reclaimed. Retired blocks cannot acquire new pins. Put rejects insufficient
   framing space rather than copying into blobstore-owned memory.
2. **Never block while holding the active segment's lock** (reservation and submission) or in a
   completion function (they run on the io_uring coordinator), and never make
   the completer wait on anything that needs the completer.
3. **Index changes from the background must be conditional** on (segment,
   offset), or they will clobber newer writes.
4. **No registered buffers (yet).** Every I/O pins its pages per operation,
   measured at about a quarter of the process's CPU samples (≈0.07 of a core)
   at 2.4 GB/s in an earlier benchmark. Registering the changing set of backing
   chunks would need explicit lifetime management; this is not implemented.
5. **Segment ids come from a counter, never the clock.** FIFO eviction and
   reload order (a later record of a key wins) depend on id order.
6. **NEVER run `go clean -cache`**: rebuilding the cache is extremely
   expensive, especially on remote machines. If you suspect a stale cache, ask
   the user first.
