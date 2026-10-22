package blobstore

import (
	"errors"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	directIO        bool
	ringDepth       uint32
	ioBudget        time.Duration
	targetSize      int64
	indexDurable    bool
	inlineThreshold int64
	stagingSize     int64
	flags           RecordFlags
	verifyChecksums bool
}

func defaultConfig() config {
	return config{
		directIO:        true,
		ringDepth:       256,
		ioBudget:        iosched.DefaultLatencyGoal,
		targetSize:      1 << 30,
		indexDurable:    true,
		inlineThreshold: 1 << 20,
		stagingSize:     256 << 10,
		flags:           FlagHeaderIntegrity,
	}
}

func (c *config) validate() error {
	c.targetSize = align.PageAlign(max(64<<10, c.targetSize))
	if c.ioBudget < 0 {
		return errors.New("blobstore: I/O budget must be nonnegative")
	}
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

// WithDataIntegrity selects None (the default) or XXHash128 for checksums written
// with new records. Read verification is controlled by WithVerifyChecksums.
func WithDataIntegrity(integrity DataIntegrity) Option {
	return funcOpt(func(c *config) { c.flags.SetDataIntegrity(integrity) })
}

// WithVerifyChecksums enables verification of stored checksums on reads and
// index recovery. Default: false.
func WithVerifyChecksums(enabled bool) Option {
	return funcOpt(func(c *config) { c.verifyChecksums = enabled })
}

// WithRingDepth sets the io_uring submission queue depth. Default: 256.
func WithRingDepth(n uint32) Option { return funcOpt(func(c *config) { c.ringDepth = n }) }

// WithIOBudget limits modeled device time in flight per I/O class.
// Default: 1.5 ms. Zero disables the limit.
func WithIOBudget(d time.Duration) Option { return funcOpt(func(c *config) { c.ioBudget = d }) }

// TestingWithDirectIO enables or disables direct I/O for tests. Default: true.
func TestingWithDirectIO(enable bool) Option {
	return funcOpt(func(c *config) { c.directIO = enable })
}
