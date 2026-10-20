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

// WithIOBudget sets the scheduler's per-class in-flight budget in modeled device
// time. Default: 1.5 ms. Zero disables the budgets. See blobstore.WithIOBudget.
func WithIOBudget(goal time.Duration) Option { return storeOpt(blobstore.WithIOBudget(goal)) }

// WithMemory bounds mapped value memory in bytes, a page multiple of at least
// 128 KiB. Inline records and streaming buffers share one slab. SIEVE drops
// resident buffers while retaining metadata for live disk records. Writers keep
// their memory pinned until Close, as do readers; close them all before closing the cache.
// At 80% occupancy, allocations signal background reclamation toward 75% usage.
// Required reclamation makes room for the request plus at least another request
// or 25% of the pool, capped at total capacity. Each shard contributes in
// proportion to its resident bytes; only final-reference releases count.
// NewWriter and disk reads return ErrBusy when no suitable storage can be reclaimed.
// Allocator options can set chunk size and prefault backing memory.
// Default: 1 GiB, 4 KiB chunks, with pages faulted in on use. Metadata is additional.
func WithMemory(bytes int64, opts ...alloc.Option) Option {
	return funcOpt(func(c *config) { c.memory, c.allocator = bytes, opts })
}

// WithTargetSize sets the live-payload target. Writes may exceed the target
// while background reclamation catches up. The initial trigger is 95% of the
// target. Each completed pass sets headroom to the payload published during that
// pass plus 25%, with a 1 MiB floor scaled down for small caches. Headroom can
// grow to the full target; the next trigger is target minus headroom. Requests
// are capped at 5% of the target, and passes continue until usage is below the
// updated trigger. Reclamation evicts external objects before inline records,
// oldest first within each group, and submits external unlinks asynchronously.
// Inline allocation wraps data.db at the target and retires overlapping records;
// freed index slots are reused independently. Unfinished writes defer eviction;
// NewWriter returns ErrBusy if reuse would overlap them. Resident values survive
// disk retirement until memory reclamation releases them.
// This is not a filesystem quota. Default: 1 GiB, rounded up to a page with a
// 64 KiB floor.
// See blobstore.WithTargetSize.
func WithTargetSize(bytes int64) Option { return storeOpt(blobstore.WithTargetSize(bytes)) }

// WithDurableIndex enables DIO group commit for descriptor writes (default true).
func WithDurableIndex(enabled bool) Option { return storeOpt(blobstore.WithDurableIndex(enabled)) }

// WithVerifyChecksums enables verification of stored header CRCs and key/value
// hashes on disk reads and index recovery. Resident values are trusted.
// Default: false. Checksums absent from older records are skipped. This does not
// change the checksums written with new records.
func WithVerifyChecksums(enabled bool) Option {
	return storeOpt(blobstore.WithVerifyChecksums(enabled))
}

// WithInlineThreshold sets the largest known value stored in data.db and
// retained in memory. Default: 1 MiB. Unknown sizes always use object files.
func WithInlineThreshold(bytes int64) Option { return storeOpt(blobstore.WithInlineThreshold(bytes)) }

// WithStagingSize sets reusable disk-read and object-write memory, excluding the
// retained write header page.
// Default: 256 KiB, reduced when needed to fit WithMemory. Must be positive.
func WithStagingSize(bytes int64) Option { return storeOpt(blobstore.WithStagingSize(bytes)) }
