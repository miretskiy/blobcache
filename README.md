# BlobCache

BlobCache caches large immutable blobs on local disk, with a bounded memory
cache for recent writes and reads. A downloader obtains aligned memory with
`Alloc`, fills it, and hands it to `Put`. The same bytes serve memory reads and
are written to disk; the cache adds no payload copy on this path.

Linux uses [DIO](https://github.com/miretskiy/dio)'s io_uring scheduler and
`O_DIRECT` by default. Other platforms use synchronous POSIX I/O for development.
The module requires Go 1.25.11 or newer.

## Usage

The cache directory must already exist. A directory belongs to one open cache;
concurrent use by multiple processes is unsupported.

```go
cache, err := blobcache.New("/data/cache",
    blobcache.WithMemory(1<<30),     // up to 1 GiB of value memory
    blobcache.WithMaxSize(100<<30), // target 100 GiB of retained disk segments
)
if err != nil {
    return err
}
defer cache.Close() // return all borrowed buffers and finish calls first
```

Download directly into cache memory, reserving space for the record framing:

```go
buf, err := cache.Alloc(cache.RecordSize(len(key), contentLength))
if err != nil {
    return err // ErrBusy lets the caller skip caching or retry later
}
if _, err := io.ReadFull(body, buf[:contentLength]); err != nil {
    return errors.Join(err, cache.Free(buf))
}

ticket, err := cache.Put(key, buf, contentLength)
if err != nil {
    if errors.Is(err, blobcache.ErrBusy) || errors.Is(err, blobcache.ErrClosed) {
        // These errors leave our buffer with us. We could retry the same
        // buffer; here we give it back because we are skipping the cache.
        return errors.Join(err, cache.Free(buf))
    }
    return err
}
// Put accepted ownership. Do not touch buf again.
// Waiting is optional: ticket.Wait() reports completion of this write.
_ = ticket
```

Read through a callback. Its slice is immutable and valid only during the call:

```go
err := cache.Get(key, func(value []byte) error {
    _, err := destination.Write(value)
    return err
})
if errors.Is(err, blobcache.ErrNotFound) {
    // Fetch from the source and optionally populate the cache.
}
```

Use keys that identify immutable content, such as an object key plus version or
ETag. Writing an existing key replaces its cache entry. There is no separate
Delete API.

## Ownership, completion, and errors

- `Alloc` lends a contiguous, page-aligned record buffer. Return it exactly once
  through `Put` or `Free`, using the original slice.
- A successful `Put` takes ownership and returns without waiting for disk I/O on
  Linux. `ErrBusy`, `ErrClosed`, and `ErrForeignMemory` do not take ownership;
  other `Put` errors consume a valid borrowed buffer. Retry `ErrBusy` with the
  same buffer rather than allocating or downloading again.
- `Get` lends a memory value or waits for its disk read. Concurrent cold reads
  of the same record share one I/O and one buffer. Callback errors propagate.
- `ErrBusy` means allocation, write admission, segment rotation, or read-handle
  capacity cannot currently accommodate the request. Calls do not wait for
  those resources. A caller may retry or treat the cache as unavailable.
- `ErrNotFound` covers absent keys, evicted files, and records rejected by read
  verification. Value checksum failures also carry a `*blobstore.ChecksumError`
  with the expected and actual CRC. Failed writes are logged and counted; valid
  memory values remain usable while cached.
- `Ticket.Wait` reports write completion. `Drain` waits for writes accepted before
  its barrier; neither promises crash durability for a still-open segment.
- `Close` waits for accepted writes and seals partial segments. It must not race
  other calls. Return every allocated buffer first; held buffers make Close
  report an error and leave their backing memory mapped.

`Stats` reports memory use, hits and misses, corruption, write failures,
retained disk reservations, segment evictions, unlink failures, and writes in
flight. `SetLogger` configures cache and blobstore logging.

## Memory cache

Memory grows lazily up to `WithMemory`. Normally, 16 MiB mmap chunks are divided
into 4 MiB blocks; small budgets scale both down. Records occupy contiguous
ranges within a block. Larger records use dedicated mappings under the same
budget. First use maps and prefaults memory. Index and allocator metadata are
additional to the configured value-memory limit.

Pressure retires the oldest blocks immediately, preventing new memory pins.
Each block records its keys so index cleanup can clear references in batches,
locking each affected shard once. Existing downloads, disk I/O, and callbacks
keep the bytes alive until they finish. They delay physical reuse, not logical
eviction. There is no memory eviction worker and no payload movement.

An index entry holds its disk location and, while cached, its memory slice.
Writes publish the reserved location immediately but mark it pending until
completion. If memory is evicted first, the pending record is a cache miss;
completed records can be read and verified from disk. Retired memory cannot be
republished by a late write or read completion.

## Disk storage and recovery

`blobstore` owns record I/O, segment files, checksums, rotation, and disk
retirement. `blobcache` owns the key index and memory. Applications with their
own index and allocation policy can use `blobstore.Store` directly:
`Write` returns a ticket with a reserved `Location`, `Read` addresses a location,
and `ReadIndex` rebuilds an index from sealed segment footers. Callers retain
write buffers until completion. BlobCache supplies aligned, fully sized record
buffers so blobstore can frame and submit them in place.

Records contain the value, padding, full key, and trailer. Metadata and value
CRC32C checksums are mandatory. Locations use a 64-bit segment ID and 32-bit
record offset and size. Segment IDs increase monotonically; filenames are
16-digit hexadecimal IDs distributed across 256 directories.

A producer orders reservation and submission under its segment lock. Its last
record decides and submits the seal as one chain: record, footer, fdatasync,
close, directory fsync. The next segment opens independently. Footers describe
reserved records; a failed record does not poison its successful neighbors.
Read validation rejects short, mismatched, or corrupt records.

Restart loads sealed segment footers in segment order and discards segments
without valid footers. This is a rebuildable cache, not a durable object store.
Earlier development formats are unsupported.

`WithMaxSegments` or `WithMaxSize` enables FIFO segment eviction. The store
retires completed prefixes in batches, targeting 80% usage or two segments of
headroom. Pressure can seal an old partial segment whose producer has gone
quiet. Unlinks are submitted through DIO outside store locks. The index receives
one segment-ID notification per batch and runs its own shard sweep.

Disk limits count retained segments and reservations. Pending or failed unlinks
can leave physical usage above the target; failures are counted, not retried.
Memory eviction and disk eviction are independent.

## I/O queues and budgets

One ring is the default. `WithRings(4)` creates four shared queues; adding
`WithDedicatedWriteRings(1)` reserves one writer and leaves three read queues.
Writes route by key hash, reads by segment ID. Each queue owns its scheduler,
virtual descriptors, and append stream and/or read-handle cache. The segment
registry and disk eviction policy are shared. Queue counts and roles are not
persisted and can change on reopen.

Read-handle lookup and submission share a queue-local lock. The first read
submits `open -> read`; followers wait behind that chain in DIO. Descriptor
replacement drains prior operations before reopening the slot. Disk waits
happen outside store locks on Linux.

Each Linux coordinator runs on a dedicated OS thread and requests affinity to a
distinct allowed CPU. `WithCoordinatorCPUs` selects those CPUs explicitly.
Affinity setup is best effort in DIO and failures are logged. CPU affinity does
not reserve a core or isolate physical device bandwidth.

Read and write admission budgets are separate. Device bandwidth and IOPS shares
are divided among the queues serving each class. `WithIOBudget` controls the
modeled device time allowed in flight per class: 1.5 ms by default, a larger
duration for more outstanding work, or zero to disable budgets. Ring capacity
and file ordering still apply. This is not an application latency guarantee;
an idle class can admit one oversized operation. Shares are static, so skew and
large requests can make multi-ring behavior differ from one shared budget.

## Configuration

| Option | Default | Meaning |
|---|---|---|
| `WithMemory` | 1 GiB | Maximum mapped/reserved value memory; metadata is additional. |
| `WithSegmentSize` | 256 MiB | Preallocated segment size, up to 2 GiB. The last record may extend it. |
| `WithMaxSize` | 0, unbounded | Target retained disk bytes; allow at least two segments per writer. |
| `WithMaxSegments` | 0, unbounded | Retained segment limit; allow at least two per writer. Choose this or `WithMaxSize`. |
| `WithMaxReadHandles` | 4096 | Total cached segment descriptors, divided among read queues. |
| `WithRings` | 1 | Independent I/O queues. |
| `WithDedicatedWriteRings` | 0 | Reserve N queues for writes; zero shares all queues. |
| `WithCoordinatorCPUs` | First allowed CPUs | One distinct Linux CPU per queue. |
| `WithRingDepth` | 256 | Submission queue depth per ring. |
| `WithIOBudget` | 1.5 ms | Per-class allowance in modeled device time; zero disables budgets. |
| `WithDirectReads` | true | Use O_DIRECT; requires direct writes. |
| `WithDirectWrites` | true | Use O_DIRECT; buffered writes require buffered reads. |

## Tests and benchmarks

```sh
go test ./...
go test -race ./... # run on Linux to validate io_uring concurrency
```

The mixed benchmark uses synchronous reads and asynchronous writes. Each
iteration is one 100 KB–2 MB write, with interleaved hot reads, four-record cold
reads, and misses. Writes fill cache memory once and retain it across retries.
Read latency is reported separately for memory, disk, and misses. PUT timing
includes allocation, filling, and admission retries; it does not measure disk
completion. The final write drain is included in throughput timing.

```sh
# Historical read-heavy configuration: 1 GiB memory, no disk eviction.
go test -run '^$' -bench '^BenchmarkBlobCache$' -benchtime=100000x

# Four rings, one dedicated writer, disk eviction, and budgets disabled.
BLOBCACHE_RINGS=4 BLOBCACHE_WRITE_RINGS=1 BLOBCACHE_IO_BUDGET=off \
  BLOBCACHE_CACHE_MEMORY_MB=1024 BLOBCACHE_MAX_SEGMENTS=128 \
  go test -run '^$' -bench '^BenchmarkBlobCache$' -benchtime=100000x
```

Other controls include `BLOBCACHE_IO_BUDGET=6ms`, `BLOBCACHE_PARALLELISM`,
`BLOBCACHE_WRITE_PERCENT`, `BLOBCACHE_NO_MEMORY_HITS=1`, and
`BLOBCACHE_BUFFERED_READS=1`. Benchmark GB/s labels use binary GiB/s units.

Compare runs with the same retention policy: evicted historical keys become
cheap misses, changing the actual disk read/write mix. Workers wait for reads
before generating more writes, so read saturation can limit write throughput.
The device measurements in [the NVMe baseline](docs/nvme-baseline-m7gd.md) and
[fio scripts](docs/fio/README.md) provide a separate hardware reference.
