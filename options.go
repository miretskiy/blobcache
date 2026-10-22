package blobcache

import (
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
)

type config struct {
	store     []blobstore.Option
	memory    int64
	allocator []alloc.Option
}

func defaultConfig() config {
	return config{memory: 1 << 30}
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

// WithRingDepth sets the io_uring submission queue depth. Default: 256.
func WithRingDepth(n uint32) Option { return storeOpt(blobstore.WithRingDepth(n)) }

// WithIOBudget limits modeled device time in flight per I/O class.
// Default: 1.5 ms. Zero disables the limit.
func WithIOBudget(goal time.Duration) Option { return storeOpt(blobstore.WithIOBudget(goal)) }

// WithMemory sets the memory limit for cached values and I/O buffers, excluding
// metadata. bytes must be a page multiple of at least 128 KiB. Default: 1 GiB.
// opts configure the memory allocator.
func WithMemory(bytes int64, opts ...alloc.Option) Option {
	return funcOpt(func(c *config) { c.memory, c.allocator = bytes, opts })
}

// WithTargetSize sets a soft limit on cached disk payload, evicting oldest
// records first. Default: 1 GiB, rounded up to a page with a 64 KiB minimum.
func WithTargetSize(bytes int64) Option { return storeOpt(blobstore.WithTargetSize(bytes)) }

// WithDurableIndex controls whether Put waits for index writes to be synced to
// disk. Default: true.
func WithDurableIndex(enabled bool) Option { return storeOpt(blobstore.WithDurableIndex(enabled)) }

// WithVerifyChecksums enables verification of stored checksums on disk reads
// and index recovery. Default: false.
func WithVerifyChecksums(enabled bool) Option {
	return storeOpt(blobstore.WithVerifyChecksums(enabled))
}

// WithInlineThreshold sets the largest known value stored in data.db and
// retained in memory. Default: 1 MiB. Unknown sizes always use object files.
func WithInlineThreshold(bytes int64) Option { return storeOpt(blobstore.WithInlineThreshold(bytes)) }

// WithStagingSize sets the buffer size for disk reads and streaming writes.
// Default: 256 KiB, reduced when needed to fit WithMemory. Must be positive.
func WithStagingSize(bytes int64) Option { return storeOpt(blobstore.WithStagingSize(bytes)) }
