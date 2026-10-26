package blobstore

import (
	"errors"
	"slices"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	directIO                           bool
	ringDepth                          uint32
	ioBudget                           time.Duration
	targetSize                         int64
	indexDurable                       bool
	inlineThreshold                    int64
	stagingSize                        int64
	flags                              RecordFlags
	verifyChecksums                    bool
	schedulerCPUs                      []int
	writeSchedulers                    int
	rejectWriteDepth, rejectWriteBytes int64
	writeBacklogLowWatermark           int64
	rejectReadDepth, rejectReadBytes   int64
}

func defaultConfig() config {
	return config{
		directIO:                 true,
		ringDepth:                256,
		ioBudget:                 iosched.DefaultLatencyGoal,
		writeBacklogLowWatermark: -1,
		targetSize:               1 << 30,
		indexDurable:             true,
		inlineThreshold:          1 << 20,
		stagingSize:              256 << 10,
		flags:                    FlagHeaderIntegrity,
		schedulerCPUs:            []int{-1},
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
	return nil
}

// Option configures a Store.
type Option interface{ apply(*config) }

type funcOpt func(*config)

func (f funcOpt) apply(c *config) { f(c) }

// WithTargetSize sets a soft limit on cached disk payload, evicting oldest
// records first. Default: 1 GiB, rounded up to a page with a 64 KiB minimum.
func WithTargetSize(bytes int64) Option { return funcOpt(func(c *config) { c.targetSize = bytes }) }

// WithDurableIndex controls whether Put waits for index writes to be synced to
// disk. Default: true.
func WithDurableIndex(enabled bool) Option {
	return funcOpt(func(c *config) { c.indexDurable = enabled })
}

// WithInlineThreshold sets the largest value stored in data.db. Default: 1 MiB.
// Unknown sizes always use object files. bytes must be nonnegative.
func WithInlineThreshold(bytes int64) Option {
	return funcOpt(func(c *config) { c.inlineThreshold = bytes })
}

// WithStagingSize sets the buffer size for disk reads and streaming writes.
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

// WithWriteSchedulers reserves the first n schedulers for writes, index wipes,
// and object removal. Remaining schedulers serve only reads. Default: zero,
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

// WithRejectWriteQueueDepth closes write admission when coordinators observe this
// many outstanding write operations across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectWriteQueueDepth(n int64) Option {
	return funcOpt(func(c *config) { c.rejectWriteDepth = max(0, n) })
}

// WithRejectWriteBacklog closes write admission when coordinators observe this
// many outstanding write buffer bytes across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectWriteBacklog(bytes int64) Option {
	return funcOpt(func(c *config) { c.rejectWriteBytes = max(0, bytes) })
}

// WithWriteBacklogLowWatermark enables a timed pause at the write backlog
// threshold. The coordinator closes admission for the estimated drain duration;
// submissions return ErrRejected during the pause. Reads and required index wipes
// continue. Set below WithRejectWriteBacklog; zero targets a full drain.
// Default: -1 (disabled). The write time budget remains independent.
// The POSIX fallback ignores this option.
func WithWriteBacklogLowWatermark(bytes int64) Option {
	return funcOpt(func(c *config) { c.writeBacklogLowWatermark = max(-1, bytes) })
}

// WithRejectReadQueueDepth closes read admission when coordinators observe this
// many outstanding read operations across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectReadQueueDepth(n int64) Option {
	return funcOpt(func(c *config) { c.rejectReadDepth = max(0, n) })
}

// WithRejectReadBacklog closes read admission when coordinators observe this
// many outstanding read buffer bytes across all schedulers. Counts last through
// ticket completion, including sync. Staged requests may overshoot the threshold.
// Zero disables it. Rejected submissions return ErrRejected.
// Required index wipes bypass rejection; the POSIX fallback ignores this option.
func WithRejectReadBacklog(bytes int64) Option {
	return funcOpt(func(c *config) { c.rejectReadBytes = max(0, bytes) })
}
