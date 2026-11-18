package blobstore

import (
	"errors"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	disk         *iosched.DiskPolicy
	ringDepth    uint32
	ioBudget     time.Duration
	sealInterval time.Duration
	readWindow   int64 // bytes per disk read of a long range

	readSchedulers            int
	writeSchedulers           int
	writeBacklog, readBacklog time.Duration // admission limits; zero disables
	writeBacklogRecovery      time.Duration // timed-pause target; negative disables
	smallWriteAdmission       int64         // payload bytes allowed through a full backlog; zero disables
	registerMemory            bool
	onMiss                    func(RecordHeader)
}

func defaultConfig() config {
	return config{
		ringDepth:            256,
		ioBudget:             iosched.DefaultLatencyGoal,
		sealInterval:         time.Second,
		readWindow:           defaultReadWindow,
		writeBacklogRecovery: -1,
		readSchedulers:       1,
		writeSchedulers:      1,
	}
}

func (c *config) validate() error {
	if c.readSchedulers < 1 || c.writeSchedulers < 1 {
		return errors.New("blobstore: at least one read and one write scheduler are required")
	}
	if c.disk == nil && c.writeBacklog > 0 && c.writeBacklogRecovery >= c.writeBacklog {
		return errors.New("blobstore: write backlog recovery target must be below the limit")
	}
	return nil
}

const defaultReadWindow = 8 << 20

// Option configures a Store.
type Option interface{ apply(*config) }

type funcOpt func(*config)

func (f funcOpt) apply(c *config) { f(c) }

// WithMissFunc has f called with a record's index header when a read finds
// that the log has overwritten it. Calls from different readers may run
// concurrently.
func WithMissFunc(f func(RecordHeader)) Option {
	return funcOpt(func(c *config) { c.onMiss = f })
}

// WithSealInterval seals the active segment this often once it spans a
// directory unit, bounding the records a crash forgets. Zero disables timed
// seals; segments then seal by size and at Close. Default: one second.
func WithSealInterval(d time.Duration) Option {
	return funcOpt(func(c *config) { c.sealInterval = max(0, d) })
}

// WithReadWindow sets how many bytes one disk read of a long range covers; a
// reader keeps at most two windows. It is rounded up to a page. Default: 8 MiB.
func WithReadWindow(bytes int64) Option {
	return funcOpt(func(c *config) { c.readWindow = max(align.PageAlign(bytes), align.BlockSize) })
}

// WithReadSchedulers and WithWriteSchedulers set how many io_uring schedulers
// serve reads and writes; each needs at least one. All share one disk policy.
// A ring places work in acceptance order, so separate rings keep writes waiting
// for their in-flight allowance from delaying reads. Each read or write picks a
// ring of its pool round-robin. Default: one of each. Setup errors are
// returned by Open.
func WithReadSchedulers(n int) Option {
	return funcOpt(func(c *config) { c.readSchedulers = n })
}

// WithWriteSchedulers: see WithReadSchedulers.
func WithWriteSchedulers(n int) Option {
	return funcOpt(func(c *config) { c.writeSchedulers = n })
}

// WithRingDepth sets each io_uring's submission queue depth, capped by the
// device's request queue depth (nvme.Info.QueueDepth). Default: 256.
func WithRingDepth(n uint32) Option { return funcOpt(func(c *config) { c.ringDepth = n }) }

// WithIOBudget limits modeled in-flight write service time across all schedulers.
// Default: 1.5 ms. Coordinators queue admitted work until allowance is available.
// Each I/O completion returns its allowance. Zero disables this limit.
// One oversized chain can proceed at idle. Reads are never time-budgeted.
// All rings share one device model and allowance; adding rings does not multiply it.
func WithIOBudget(d time.Duration) Option { return funcOpt(func(c *config) { c.ioBudget = max(0, d) }) }

// WithDiskPolicy shares a caller-built policy across the store's schedulers.
// Construct it with iosched.NewDiskPolicy. A non-nil policy takes precedence over
// WithIOBudget and the backlog options; nil builds a policy from those options.
func WithDiskPolicy(policy *iosched.DiskPolicy) Option {
	return funcOpt(func(c *config) { c.disk = policy })
}

// WithWriteBacklogLimit rejects writes with ErrRejected while the modeled
// device time of admitted, unfinished writes across all schedulers is at least
// limit. Zero, the default, disables it.
func WithWriteBacklogLimit(limit time.Duration) Option {
	return funcOpt(func(c *config) { c.writeBacklog = max(0, limit) })
}

// WithRegisteredMemory registers the allocator's memory with every scheduler,
// so NVMe commands on its buffers skip pinning their pages; it cut CPU per
// 128 KiB command by about 20% on EC2 instance storage. Entries are one
// allocator span (128 KiB): Linux before 6.15 rejects commands in larger
// entries, and a ring holds at most 16,384, so Open fails for an allocator
// larger than 2 GiB. Registration locks the memory once per ring against
// RLIMIT_MEMLOCK. Default: disabled.
func WithRegisteredMemory() Option {
	return funcOpt(func(c *config) { c.registerMemory = true })
}

// WithSmallWriteAdmission admits values up to bytes even when the write backlog
// is full. These writes retain their backlog charges and normal dispatch limits.
// Default: disabled. This can increase write queueing beyond WithWriteBacklogLimit.
func WithSmallWriteAdmission(bytes int64) Option {
	return funcOpt(func(c *config) { c.smallWriteAdmission = max(0, bytes) })
}

// WithWriteBacklogRecovery keeps writes rejected, once the backlog limit is
// reached, until the backlog is modeled to drain to target, which must be below
// the limit. Negative, the default, reopens admission as soon as the backlog
// falls below the limit.
func WithWriteBacklogRecovery(target time.Duration) Option {
	return funcOpt(func(c *config) { c.writeBacklogRecovery = max(-1, target) })
}

// WithReadBacklogLimit is the read counterpart of WithWriteBacklogLimit.
func WithReadBacklogLimit(limit time.Duration) Option {
	return funcOpt(func(c *config) { c.readBacklog = max(0, limit) })
}
