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

// WithSchedulerCPUs creates one scheduler per CPU. -1 leaves it unpinned.
// Default: one unpinned scheduler. Pinning is best-effort; kernel workers may
// inherit the coordinator's mask. Multiple schedulers or pinning require io_uring.
func WithSchedulerCPUs(cpus ...int) Option { return storeOpt(blobstore.WithSchedulerCPUs(cpus...)) }

// WithWriteSchedulers dedicates the first n schedulers to writes and cleanup;
// the rest serve reads. Default: zero (all shared). At least one must serve
// reads. Each reader/writer stays on one scheduler, selected round-robin.
func WithWriteSchedulers(n int) Option { return storeOpt(blobstore.WithWriteSchedulers(n)) }

// WithRingDepth sets each io_uring's submission queue depth. Default: 256.
func WithRingDepth(n uint32) Option { return storeOpt(blobstore.WithRingDepth(n)) }

// WithIOBudget limits modeled in-flight write service time across all schedulers.
// Default: 1.5 ms. Coordinators queue admitted work until allowance is available.
// Each I/O completion returns its allowance. Zero disables this limit.
// One oversized chain can proceed at idle. Reads are never time-budgeted.
// All rings share one device model and allowance; adding rings does not multiply it.
func WithIOBudget(goal time.Duration) Option { return storeOpt(blobstore.WithIOBudget(goal)) }

// WithMemory sets the memory limit for cached values and I/O buffers, excluding
// metadata. bytes must be at least 4 MiB; the allocator uses whole 128 KiB
// spans of it. Default: 1 GiB. opts configure the memory allocator
// (alloc.Allocator).
func WithMemory(bytes int64, opts ...alloc.Option) Option {
	return funcOpt(func(c *config) { c.memory, c.allocator = bytes, opts })
}

// WithTargetSize sets a soft limit on cached disk payload, evicting oldest
// records first. Default: 1 GiB, rounded up to a page with a 64 KiB minimum.
func WithTargetSize(bytes int64) Option { return storeOpt(blobstore.WithTargetSize(bytes)) }

// WithVerifyChecksums enables verification of stored checksums on disk reads
// and index recovery. Default: false.
func WithVerifyChecksums(enabled bool) Option {
	return storeOpt(blobstore.WithVerifyChecksums(enabled))
}

// WithInlineThreshold sets the largest value stored in data.db and
// retained in memory. Default: 1 MiB.
func WithInlineThreshold(bytes int64) Option { return storeOpt(blobstore.WithInlineThreshold(bytes)) }

// WithStagingSize sets the disk-read buffer size and minimum writer capacity.
// Writers also reserve enough capacity for one inline value and its framing.
// Default: 256 KiB, reduced when needed to fit WithMemory. Must be positive.
func WithStagingSize(bytes int64) Option { return storeOpt(blobstore.WithStagingSize(bytes)) }

// WithWriteBacklogLimit rejects writes with ErrRejected while the modeled
// device time of admitted, unfinished writes across all schedulers is at least
// limit. Zero, the default, disables it. The POSIX fallback ignores it.
func WithWriteBacklogLimit(limit time.Duration) Option {
	return storeOpt(blobstore.WithWriteBacklogLimit(limit))
}

// WithWriteBacklogRecovery keeps writes rejected, once the backlog limit is
// reached, until the backlog is modeled to drain to target, which must be below
// the limit. Negative, the default, reopens admission as soon as the backlog
// falls below the limit.
func WithWriteBacklogRecovery(target time.Duration) Option {
	return storeOpt(blobstore.WithWriteBacklogRecovery(target))
}

// WithReadBacklogLimit is the read counterpart of WithWriteBacklogLimit.
func WithReadBacklogLimit(limit time.Duration) Option {
	return storeOpt(blobstore.WithReadBacklogLimit(limit))
}
