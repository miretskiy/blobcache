package blobstore

import (
	"errors"
	"slices"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	directIO                  bool
	ringDepth                 uint32
	ioBudget                  time.Duration
	targetSize                int64
	inlineThreshold           int64
	stagingSize               int64
	flags                     RecordFlags
	verifyChecksums           bool
	schedulerCPUs             []int
	writeSchedulers           int
	writeBacklog, readBacklog time.Duration // admission limits; zero disables
	writeBacklogRecovery      time.Duration // timed-pause target; negative disables
	onRetire                  func(RecordHeader)
}

func defaultConfig() config {
	return config{
		directIO:             true,
		ringDepth:            256,
		ioBudget:             iosched.DefaultLatencyGoal,
		writeBacklogRecovery: -1,
		targetSize:           1 << 30,
		inlineThreshold:      1 << 20,
		stagingSize:          256 << 10,
		flags:                FlagHeaderIntegrity,
		schedulerCPUs:        []int{-1},
	}
}

func (c *config) validate() error {
	if len(c.schedulerCPUs) == 0 {
		return errors.New("blobstore: at least one scheduler CPU is required")
	}
	for _, cpu := range c.schedulerCPUs {
		if cpu < -1 {
			return errors.New("blobstore: scheduler CPU must be -1 or nonnegative")
		}
	}
	if c.writeSchedulers < 0 || c.writeSchedulers >= len(c.schedulerCPUs) {
		return errors.New("blobstore: write-only schedulers must leave at least one read scheduler")
	}
	c.targetSize = align.PageAlign(max(64<<10, c.targetSize))
	if c.inlineThreshold < 0 {
		return errors.New("blobstore: invalid inline threshold")
	}
	if c.stagingSize <= 0 {
		return errors.New("blobstore: invalid staging size")
	}
	if c.writeBacklog > 0 && c.writeBacklogRecovery >= c.writeBacklog {
		return errors.New("blobstore: write backlog recovery target must be below the limit")
	}
	return nil
}

// Option configures a Store.
type Option interface{ apply(*config) }

type funcOpt func(*config)

func (f funcOpt) apply(c *config) { f(c) }

// WithTargetSize sets a soft limit on cached disk payload, evicting oldest
// records first. Default: 1 GiB, rounded up to a page with a 64 KiB minimum.
func WithTargetSize(bytes int64) Option { return funcOpt(func(c *config) { c.targetSize = bytes }) }

// WithRetireFunc has f called for each record that eviction removes from the
// index, outside the store lock and possibly concurrently.
func WithRetireFunc(f func(RecordHeader)) Option {
	return funcOpt(func(c *config) { c.onRetire = f })
}

// WithInlineThreshold sets the largest value stored in data.db. Default: 1 MiB.
// bytes must be nonnegative.
func WithInlineThreshold(bytes int64) Option {
	return funcOpt(func(c *config) { c.inlineThreshold = bytes })
}

// WithStagingSize sets the disk-read buffer size and minimum writer capacity.
// Writers also reserve enough capacity for one inline value and its framing.
// Default: 256 KiB. bytes must be positive.
func WithStagingSize(bytes int64) Option {
	return funcOpt(func(c *config) { c.stagingSize = bytes })
}

// WithDataIntegrity selects None (the default) or CRC32C for checksums written
// with new records. Read verification is controlled by WithVerifyChecksums.
func WithDataIntegrity(integrity DataIntegrity) Option {
	return funcOpt(func(c *config) { c.flags.SetDataIntegrity(integrity) })
}

// WithVerifyChecksums enables verification of stored checksums on reads and
// index recovery. Default: false.
func WithVerifyChecksums(enabled bool) Option {
	return funcOpt(func(c *config) { c.verifyChecksums = enabled })
}

// WithSchedulerCPUs creates one scheduler per CPU, in the supplied order.
// -1 leaves a coordinator unpinned. Default: one unpinned scheduler.
// Multiple schedulers or a pinned coordinator require io_uring; setup errors
// are returned by Open. Affinity failures are logged and I/O continues.
// Coordinator affinity may also be inherited by kernel workers.
// Each reader/writer stays on one scheduler for its lifetime because virtual
// file descriptors belong to the ring that opened them.
func WithSchedulerCPUs(cpus ...int) Option {
	cpus = slices.Clone(cpus)
	return funcOpt(func(c *config) { c.schedulerCPUs = cpus })
}

// WithWriteSchedulers reserves the first n schedulers for writes and object
// removal. Remaining schedulers serve only reads. Default: zero,
// which shares all schedulers between reads and writes. n must be less than
// the number of CPUs supplied to WithSchedulerCPUs. Selection is round-robin
// within each pool; it does not depend on a key or its previous writer.
func WithWriteSchedulers(n int) Option {
	return funcOpt(func(c *config) { c.writeSchedulers = n })
}

// WithRingDepth sets each io_uring's submission queue depth. Default: 256.
func WithRingDepth(n uint32) Option { return funcOpt(func(c *config) { c.ringDepth = n }) }

// WithIOBudget limits modeled in-flight write service time across all schedulers.
// Default: 1.5 ms. Coordinators queue admitted work until allowance is available.
// Each I/O completion returns its allowance. Zero disables this limit.
// One oversized chain can proceed at idle. Reads are never time-budgeted.
// All rings share one device model and allowance; adding rings does not multiply it.
func WithIOBudget(d time.Duration) Option { return funcOpt(func(c *config) { c.ioBudget = max(0, d) }) }

// TestingWithDirectIO enables or disables direct I/O for tests. Default: true.
func TestingWithDirectIO(enable bool) Option {
	return funcOpt(func(c *config) { c.directIO = enable })
}

// WithWriteBacklogLimit rejects writes with ErrRejected while the modeled
// device time of admitted, unfinished writes across all schedulers is at least
// limit. Zero, the default, disables it. The POSIX fallback ignores it.
func WithWriteBacklogLimit(limit time.Duration) Option {
	return funcOpt(func(c *config) { c.writeBacklog = max(0, limit) })
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
