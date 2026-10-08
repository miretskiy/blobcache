# BlobCache

A disk cache for large blobs — chunks of objects downloaded from S3 or GCS, or
the results of ranged reads — built to run the NVMe device at full bandwidth
with no data copies or write buffering. A write completer handles completions;
rotation schedules eviction batches when disk space is needed.

- **No copies.** Callers download into cache memory (`Alloc`) and hand it to
  `Put`; that memory is the cached value, written to disk in place with
  `O_DIRECT` through io_uring ([dio](https://github.com/miretskiy/dio)).
  `Get` lends values where they lie; a read from disk lands in cache memory
  and stays there.
- **Bounded, lazy memory.** Backing memory grows on demand up to `WithMemory`.
  Records share 4 MiB blocks carved from 16 MiB chunks (scaled down for small
  budgets); larger records use dedicated mappings under the same budget.
  `Alloc` lends memory, `Put` or `Free` takes it back, and `Get` lends immutable
  values during its callback. Whole blocks are reclaimed oldest first,
  skipping pinned blocks. Their index slices are cleared before storage reuse.
- **Shared cold reads.** Concurrent reads of one disk record share one I/O and
  one buffer, pinned until every participating callback returns.
- **Asynchronous record writes.** `Put` submits record I/O without waiting; the value is readable from memory
  immediately and from disk once the write lands. Creating, writing and
  sealing segment files are all asynchronous io_uring operations. When no
  memory can be reclaimed, `Alloc` and `Get` return `ErrBusy`.
- **One data I/O per read and per write.** Each record stores the key, framing
  CRC32C and value CRC32C. A footer lists reserved locations; failed records
  become misses on verification without invalidating successful peers.
- **FIFO eviction** of whole segments with `WithMaxSize`, without compaction.
  The capacity target includes preallocation and outstanding reservations.
  Unlinks are best effort; pending or failed removals can exceed the target.
- **No Delete.** A key is invalidated by overwriting it (the newer record wins,
  also after a restart) or by its segment's eviction, so keys should name
  immutable content, such as object key plus version or ETag.
- CRC32C checksums of values are always stored and verified.

## Usage

```go
cache, err := blobcache.New("/data/cache", // the directory must exist
	blobcache.WithMemory(4<<30),  // up to 4 GiB of value memory
    blobcache.WithMaxSize(100<<30), // 100 GiB on disk
)
if err != nil {
	return err
}
defer cache.Close()

// Write: download into cache memory, then hand it to Put.
buf, err := cache.Alloc(cache.RecordSize(len(key), contentLength))
if err != nil {
	return err // ErrBusy: no memory can be reclaimed right now; skip caching
}
n, err := io.ReadFull(resp.Body, buf[:contentLength])
if err != nil {
	return errors.Join(err, cache.Free(buf)) // not stored: give it back
}
serve(buf[:n])                         // the request that triggered the download
if _, err := cache.Put(key, buf, n); err != nil { // Put takes buf unless closed/foreign
	log.Print(err)
}

// Read: the value is lent to the callback, from memory or read from disk.
err = cache.Get(key, func(value []byte) error {
	_, err := w.Write(value) // valid only inside the callback
	return err
})
```

Allocation never waits for borrowers: memory in use (held from `Alloc`, being
written, or lent by `Get`) is never reclaimed, and when nothing can be,
`Alloc` and `Get` return `ErrBusy`; the caller decides whether to skip caching
or retry. `Get` waits for the shared read of its record. Growing memory maps and prefaults
new chunks on demand. `Drain` waits for accepted
writes, for callers that need a barrier.

## Layers

`blobcache` is an index and memory tier over a `blobstore.Store`, which does
the I/O: segment files, record framing and verification, rotation and
sealing. The store keeps no index. `Store.Write` returns a ticket that
reveals where a record went (a `Location`) once the write lands, and
`Store.Read` reads one by location. `Store.ReadIndex` reports every record on
disk, in write order, to build the caller's index. A caller that keeps its own
index and memory can use `blobstore` directly.

Rotation selects a batch of the oldest completed segments from metadata already
in memory. It retires them immediately and submits closes for cached handles.
After releasing the write lock, it submits unlinks through dio and invokes
the callback without waiting for I/O. Blobstore starts no eviction goroutine
and keeps no in-progress flag or retry queue. Pending read opens close their handles when the initial read ends.

`WithEvictionCallback(func(Eviction))` receives the retired segment IDs on the
caller's goroutine, outside store locks. The caller owns the slice and may
schedule cleanup elsewhere. The index callback starts its own goroutine and
sweeps each shard once, deleting locations in the retired prefix; its shard lock protects a boundary that rejects late
write completions and tolerates notifications arriving out of order. Blobstore
never rereads footers or knows about index shards.

Eviction targets 80% usage or two segments of headroom, whichever leaves more
room. Normal batches include at least two segments; a single completed victim
can be retired when necessary for a small store to make progress. Unlink errors
are counted; failed removals are not retried. `DiskBytes` measures retained
segments and reservations, excluding pending or failed unlinks.

The handle cache uses one fixed slot table. All lookups and submissions happen
under its mutex; a miss submits `open → read`, and dio holds followers behind
that chain. Replacement drains prior reads before opening the next file. There
are no spare slots or lookup-to-submission pins.

Segment IDs are 64-bit monotonic counters. The format stores both metadata
and value checksums; no compatibility with earlier development layouts is
maintained.

## Configuration

| Option | Default | |
|--------|---------|---|
| `WithSegmentSize` | 256 MiB | Preallocated segment size (≤ 2 GiB); the final record may extend it. |
| `WithMaxSize` | 0 (unbounded) | Disk budget; must fit at least two segments. Enables FIFO eviction. |
| `WithMemory` | 1 GiB | Maximum mapped value memory, allocated lazily; metadata is additional. |
| `WithDirectWrites` | on | `O_DIRECT` writes of page-padded records; `false` writes packed records through the page cache (needs `WithDirectReads(false)`). |
| `WithDirectReads` | on | `O_DIRECT` reads; `false` reads through the kernel page cache. |
| `WithChecksum` | always on | Compatibility option; value verification cannot be disabled. |
| `WithMaxReadHandles` | 4096 | Segment files kept open for reads, in io_uring slots, replaced using CLOCK. |
| `WithRingDepth` | 256 | io_uring queue depth. |

On Linux BlobCache requires io_uring: `New` fails without it. There is no
fallback to synchronous I/O, which would make `Put` wait for the disk. Other
platforms use synchronous POSIX I/O, for development.

## Benchmarks

```sh
go test -bench=BenchmarkBlobCache -benchtime=100000x | tee bench-100k.log   # ~100 GB
go test -bench=BenchmarkPutGet -benchmem                                    # per-op cost
```
