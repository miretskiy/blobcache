package blobcache

import (
	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	store      []blobstore.Option
	memory     int64
	memoryHits bool
}

func defaultConfig() config {
	return config{memory: 1 << 30, memoryHits: true}
}

// Option configures a Cache.
type Option interface {
	apply(*config)
}

type funcOpt func(*config)

func (f funcOpt) apply(c *config) { f(c) }

func storeOpt(o blobstore.Option) Option {
	return funcOpt(func(c *config) { c.store = append(c.store, o) })
}

// WithSegmentSize sets the size of each segment file. Default: 256 MiB.
func WithSegmentSize(bytes int64) Option { return storeOpt(blobstore.WithSegmentSize(bytes)) }

// WithRingDepth sets the io_uring submission queue depth. Default: 256.
func WithRingDepth(n uint32) Option { return storeOpt(blobstore.WithRingDepth(n)) }

// WithDirectReads chooses between O_DIRECT reads (the default) and reads
// through the kernel page cache. Direct reads copy nothing; buffered reads
// copy each read out of the page cache, which may serve repeated reads of
// older values. With direct writes (the default) recently written values are
// not in the page cache.
func WithDirectReads(enabled bool) Option { return storeOpt(blobstore.WithDirectReads(enabled)) }

// WithDirectWrites chooses between O_DIRECT writes (the default) and writes
// through the kernel page cache. Direct writes go from the cache's memory to
// the device with no copy; buffered writes are copied into the page cache and
// written back by the kernel, and need WithDirectReads(false). See
// blobstore.WithDirectWrites.
func WithDirectWrites(enabled bool) Option { return storeOpt(blobstore.WithDirectWrites(enabled)) }

// WithChecksum is retained for compatibility; value checksums are always enabled.
func WithChecksum() Option { return storeOpt(blobstore.WithChecksum()) }

// WithMemory bounds mapped value memory in bytes, a page multiple of at least
// 128 KiB. Memory grows on demand: normally 16 MiB chunks split into 4 MiB
// blocks, scaled down for small budgets. Larger records use dedicated mappings
// under the same limit. Full blocks are reclaimed oldest first, skipping those
// held by downloads, writes, or readers. Alloc and Get return ErrBusy when no
// suitable memory can be reclaimed. Default: 1 GiB. Metadata is additional.
func WithMemory(bytes int64) Option {
	return funcOpt(func(c *config) { c.memory = bytes })
}

// withoutMemoryHits makes Get ignore values in memory and read everything
// from disk, for comparing the memory tier with a disk-only cache. Tests and
// benchmarks only.
func withoutMemoryHits() Option {
	return funcOpt(func(c *config) { c.memoryHits = false })
}

// withPreSubmit passes every operation the store submits through fn (see
// blobstore.TestingWithPreSubmit). Tests only.
func withPreSubmit(fn func(iosched.Op) iosched.Op) Option {
	return storeOpt(blobstore.TestingWithPreSubmit(fn))
}

// WithMaxSize targets disk usage for retained segments and reservations. Zero
// disables eviction. The limit must fit two segments. Pending or failed best
// effort unlinks can leave more bytes on disk than this target.
func WithMaxSize(bytes int64) Option { return storeOpt(blobstore.WithMaxSize(bytes)) }

// WithMaxReadHandles bounds cached segment descriptors. Default: 4096.
func WithMaxReadHandles(n int) Option { return storeOpt(blobstore.WithMaxReadHandles(n)) }
