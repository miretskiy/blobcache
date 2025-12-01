package blobcache

import (
	"errors"
	"time"
)

// config holds internal configuration
type config struct {
	Path                  string
	MaxSize               int64
	Shards                int
	EvictionStrategy      EvictionStrategy
	EvictionHysteresis    float64 // Percentage over MaxSize to evict (e.g., 0.1 = evict 10% extra)
	MemTableCapacity      int     // Channel capacity (-1=disabled, 0=unbuffered, >0=buffered)
	BloomFPRate           float64
	BloomEstimatedKeys    int
	BloomRefreshInterval  time.Duration
	OrphanCleanupInterval time.Duration
	Checksums             bool
	Fsync                 bool
	VerifyOnRead          bool
}

// Option configures BlobCache
type Option interface {
	apply(*config)
}

// funcOpt wraps a function as an Option
type funcOpt func(*config)

func (f funcOpt) apply(c *config) {
	f(c)
}

// WithMaxSize sets the maximum cache size in bytes
func WithMaxSize(size int64) Option {
	return funcOpt(func(c *config) {
		c.MaxSize = size
	})
}

// WithShards sets the number of filesystem shards (default: 256)
// Can only be set for new cache (not for existing)
func WithShards(n int) Option {
	return funcOpt(func(c *config) {
		c.Shards = n
	})
}

// WithEvictionStrategy sets how files are selected for eviction
func WithEvictionStrategy(strategy EvictionStrategy) Option {
	return funcOpt(func(c *config) {
		c.EvictionStrategy = strategy
	})
}

// WithEvictionHysteresis sets extra percentage to evict beyond target (default: 0.1 = 10%)
// This prevents thrashing at the eviction boundary by creating a buffer zone.
// Example: With MaxSize=100MB and Hysteresis=0.1, eviction will remove 110MB when triggered.
func WithEvictionHysteresis(pct float64) Option {
	return funcOpt(func(c *config) {
		c.EvictionHysteresis = pct
	})
}

// WithMemTableCapacity sets async write buffer size (default: -1 = disabled)
// Emulates RocksDB memtable for async I/O:
//
//	-1: Disabled (synchronous writes)
//	 0: Unbuffered channel (synchronous handoff to background writer)
//	>0: Buffered channel (async up to N pending writes)
//
// Example: capacity=1000 allows 1000 Put() calls to queue before blocking
func WithMemTableCapacity(capacity int) Option {
	return funcOpt(func(c *config) {
		c.MemTableCapacity = capacity
	})
}

// WithBloomFPRate sets the bloom filter false positive rate (default: 0.01 = 1%)
// Bloom filter size estimates (for 1M keys):
//
//	FP Rate 0.01 (1%):    ~9.6 bits/key  → 1.2 MB
//	FP Rate 0.001 (0.1%): ~14.4 bits/key → 1.8 MB
//	FP Rate 0.0001:       ~19.2 bits/key → 2.4 MB
func WithBloomFPRate(rate float64) Option {
	return funcOpt(func(c *config) {
		c.BloomFPRate = rate
	})
}

// WithBloomEstimatedKeys sets estimated key count for bloom filter sizing (default: 1M)
func WithBloomEstimatedKeys(n int) Option {
	return funcOpt(func(c *config) {
		c.BloomEstimatedKeys = n
	})
}

// WithBloomRefreshInterval sets how often bloom filter is rebuilt and persisted (default: 24h)
func WithBloomRefreshInterval(d time.Duration) Option {
	return funcOpt(func(c *config) {
		c.BloomRefreshInterval = d
	})
}

// WithChecksums enables/disables CRC32 checksums (default: true)
func WithChecksums(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.Checksums = enabled
	})
}

// WithFsync enables/disables fsync on writes (default: false, cache semantics)
func WithFsync(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.Fsync = enabled
	})
}

// WithVerifyOnRead enables checksum verification on reads (default: false, opt-in)
func WithVerifyOnRead(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.VerifyOnRead = enabled
	})
}

// WithOrphanCleanupInterval sets how often orphaned files are cleaned (default: 24h, 0 = disabled)
func WithOrphanCleanupInterval(d time.Duration) Option {
	return funcOpt(func(c *config) {
		c.OrphanCleanupInterval = d
	})
}

// EvictionStrategy determines how files are selected for eviction
type EvictionStrategy int

const (
	EvictByCTime EvictionStrategy = iota // Oldest created (FIFO)
	EvictByMTime                         // Least recently modified (LRU)
)

func (e EvictionStrategy) String() string {
	switch e {
	case EvictByCTime:
		return "ctime"
	case EvictByMTime:
		return "mtime"
	default:
		return "ctime"
	}
}

// Common errors
var (
	ErrNotFound  = errors.New("key not found")
	ErrCorrupted = errors.New("data corruption detected")
)

// defaultConfig returns sensible defaults (path set by caller)
func defaultConfig(path string) config {
	return config{
		Path:                  path,
		MaxSize:               0, // TODO: Auto-detect 80% of disk capacity
		Shards:                256,
		EvictionStrategy:      EvictByCTime,
		EvictionHysteresis:    0.1,       // Evict 10% extra to prevent thrashing
		MemTableCapacity:      -1,        // Disabled by default (synchronous writes)
		BloomFPRate:           0.01,      // 1% FP rate
		BloomEstimatedKeys:    1_000_000, // 1M keys → ~1.2 MB bloom
		BloomRefreshInterval:  24 * time.Hour,
		OrphanCleanupInterval: 24 * time.Hour, // Daily, 0 = disabled
		Checksums:             true,
		Fsync:                 false,
		VerifyOnRead:          false,
	}
}
