package blobcache

import (
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	store     []blobstore.Option
	memory    int64
	inline    int64
	allocator []alloc.Option
}

func defaultConfig() config {
	return config{memory: 1 << 30, inline: 1 << 20}
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

// WithReadSchedulers sets how many io_uring schedulers serve reads; see
// blobstore.WithReadSchedulers. Default: one.
func WithReadSchedulers(n int) Option { return storeOpt(blobstore.WithReadSchedulers(n)) }

// WithWriteSchedulers sets how many io_uring schedulers serve writes, including
// footer and directory writes. Default: one.
func WithWriteSchedulers(n int) Option { return storeOpt(blobstore.WithWriteSchedulers(n)) }

// WithRingDepth sets each io_uring's submission queue depth, capped by the
// device's request queue depth. Default: 256.
func WithRingDepth(n uint32) Option { return storeOpt(blobstore.WithRingDepth(n)) }

// WithIOBudget limits modeled in-flight write service time across all schedulers.
// Default: 1.5 ms. Coordinators queue admitted work until allowance is available.
// Each I/O completion returns its allowance. Zero disables this limit.
// One oversized chain can proceed at idle. Reads are never time-budgeted.
// All rings share one device model and allowance; adding rings does not multiply it.
func WithIOBudget(goal time.Duration) Option { return storeOpt(blobstore.WithIOBudget(goal)) }

// WithDiskPolicy uses a caller-built policy for all of the cache's schedulers.
// Construct it with iosched.NewDiskPolicy and retain it for CanAdmit and Stats.
// A non-nil policy takes precedence over WithIOBudget and the backlog options.
func WithDiskPolicy(policy *iosched.DiskPolicy) Option {
	return storeOpt(blobstore.WithDiskPolicy(policy))
}

// WithMemory sets the memory limit for cached values and I/O buffers, excluding
// metadata. bytes must be at least 4 MiB; the allocator uses whole 128 KiB
// spans of it. Default: 1 GiB. opts configure the memory allocator
// (alloc.Allocator).
func WithMemory(bytes int64, opts ...alloc.Option) Option {
	return funcOpt(func(c *config) { c.memory, c.allocator = bytes, opts })
}

// WithSealInterval bounds how long written records wait to become recoverable
// after a crash; see blobstore.WithSealInterval. Default: one second.
func WithSealInterval(d time.Duration) Option { return storeOpt(blobstore.WithSealInterval(d)) }

// WithReadWindow sets how many bytes one disk read of a long range covers;
// see blobstore.WithReadWindow. Default: 8 MiB.
func WithReadWindow(bytes int64) Option { return storeOpt(blobstore.WithReadWindow(bytes)) }

// WithInlineThreshold sets the largest value kept resident in memory. Such
// values are readable from memory as soon as Put returns; larger values are
// published when their write completes and are read from disk. Every value is
// stored the same way on disk. Default: 1 MiB.
func WithInlineThreshold(bytes int64) Option { return funcOpt(func(c *config) { c.inline = bytes }) }

// WithWriteBacklogLimit rejects writes with ErrRejected while the modeled
// device time of admitted, unfinished writes across all schedulers is at least
// limit. Zero, the default, disables it.
func WithWriteBacklogLimit(limit time.Duration) Option {
	return storeOpt(blobstore.WithWriteBacklogLimit(limit))
}

// WithRegisteredMemory registers the cache's memory with the io_uring
// schedulers; see blobstore.WithRegisteredMemory. It requires WithMemory of
// at most 2 GiB. Default: disabled.
func WithRegisteredMemory() Option { return storeOpt(blobstore.WithRegisteredMemory()) }

// WithSmallWriteAdmission admits values up to bytes through a full write backlog.
// Their I/O remains accounted and waits for the normal dispatch allowance.
// Default: disabled. Admitted writes can queue beyond WithWriteBacklogLimit.
func WithSmallWriteAdmission(bytes int64) Option {
	return storeOpt(blobstore.WithSmallWriteAdmission(bytes))
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
