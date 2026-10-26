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

// WithRejectWriteQueueDepth closes write admission when coordinators observe this
// many outstanding write operations across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectWriteQueueDepth(n int64) Option {
	return storeOpt(blobstore.WithRejectWriteQueueDepth(n))
}

// WithRejectWriteBacklog closes write admission when coordinators observe this
// many outstanding write buffer bytes across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectWriteBacklog(bytes int64) Option {
	return storeOpt(blobstore.WithRejectWriteBacklog(bytes))
}

// WithWriteBacklogLowWatermark enables a timed pause at the write backlog
// threshold. The coordinator closes admission for the estimated drain duration;
// submissions return ErrRejected during the pause. Reads and required index wipes
// continue. Set below WithRejectWriteBacklog; zero targets a full drain.
// Default: -1 (disabled). The write time budget remains independent.
// The POSIX fallback ignores this option.
func WithWriteBacklogLowWatermark(bytes int64) Option {
	return storeOpt(blobstore.WithWriteBacklogLowWatermark(bytes))
}

// WithRejectReadQueueDepth closes read admission when coordinators observe this
// many outstanding read operations across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectReadQueueDepth(n int64) Option {
	return storeOpt(blobstore.WithRejectReadQueueDepth(n))
}

// WithRejectReadBacklog closes read admission when coordinators observe this
// many outstanding read buffer bytes across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectReadBacklog(bytes int64) Option {
	return storeOpt(blobstore.WithRejectReadBacklog(bytes))
}
