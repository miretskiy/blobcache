package blobcache

import (
	"errors"
	"hash"
	"hash/crc32"
	"time"
)

// IOConfig holds I/O strategy settings
type IOConfig struct {
	DirectIO bool // Use O_DIRECT (bypass OS cache)
	Fsync    bool // Use fdatasync for durability
}

// ResilienceConfig holds data integrity settings
type ResilienceConfig struct {
	ChecksumHash Hasher           // Hash factory for checksums (nil = disabled)
	VerifyOnRead bool             // Verify checksums on reads
	Checksums    ChecksumHandling // Whether to include checksums in segment footers
}

// config holds internal configuration
type config struct {
	Path                  string
	MaxSize               int64
	Shards                int // Number of shard directories (default: 256)
	EvictionStrategy      EvictionStrategy
	EvictionHysteresis    float64 // Percentage over MaxSize to evict (e.g., 0.1 = evict 10% extra)
	WriteBufferSize       int64   // Memtable batch size in bytes (like RocksDB write_buffer_size)
	SegmentSize           int64   // Target segment file size (0 = one blob per file)
	MaxInflightBatches    int     // Max batches queued (like RocksDB max_write_buffer_number)
	BloomFPRate           float64
	BloomEstimatedKeys    int
	BloomRefreshInterval  time.Duration
	OrphanCleanupInterval time.Duration
	IO                    IOConfig
	Resilience            ResilienceConfig
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

// WithWriteBufferSize sets memtable batch threshold in bytes (default: 100MB, production ~1GB)
// When accumulated entries exceed this size, batch is flushed to disk
// Emulates RocksDB write_buffer_size parameter
func WithWriteBufferSize(bytes int64) Option {
	return funcOpt(func(c *config) {
		c.WriteBufferSize = bytes
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

// WithChecksum enables CRC32 checksums (hardware accelerated)
func WithChecksum() Option {
	return funcOpt(func(c *config) {
		c.Resilience.ChecksumHash = func() hash.Hash32 {
			return crc32.NewIEEE()
		}
		c.Resilience.Checksums = IncludeChecksums
	})
}

// WithChecksumHash enables checksums with a custom hash factory
// Example: WithChecksumHash(crc32.NewIEEE) or WithChecksumHash(xxhash.New)
func WithChecksumHash(factory Hasher) Option {
	return funcOpt(func(c *config) {
		c.Resilience.ChecksumHash = factory
		c.Resilience.Checksums = IncludeChecksums
	})
}

// WithFsync enables fdatasync on writes (default: false, cache semantics)
// Uses fdatasync(2) on Linux and F_FULLFSYNC on Darwin for data durability
// Note: Syncing data flushes only - metadata (mtime, etc.) not synced since immutable blobs don't need it
func WithFsync(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.IO.Fsync = enabled
	})
}

// WithVerifyOnRead enables checksum verification on reads (default: false, opt-in)
func WithVerifyOnRead(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.Resilience.VerifyOnRead = enabled
	})
}

// WithSegmentSize sets target segment file size (default: 0 = one blob per file)
// When > 0, multiple blobs are written to large segment files
// Reduces file count and syscall overhead for high-throughput workloads
func WithSegmentSize(size int64) Option {
	return funcOpt(func(c *config) {
		c.SegmentSize = size
	})
}

// WithDirectIOWrites enables DirectIO for writes (default: false)
// DirectIO uses aligned writes with padding, then truncates to actual size
// Provides better sustained throughput for large workloads by bypassing OS cache
func WithDirectIOWrites() Option {
	return funcOpt(func(c *config) {
		c.IO.DirectIO = true
	})
}

// WithBitcaskIndex is deprecated - Bitcask is now the default index
// This option is kept for backwards compatibility but has no effect
func WithBitcaskIndex() Option {
	return funcOpt(func(c *config) {
		// No-op: Bitcask is now the default
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
		EvictionHysteresis:    0.1,               // Evict 10% extra to prevent thrashing
		WriteBufferSize:       100 * 1024 * 1024, // 100MB (production ~1GB)
		SegmentSize:           0,                 // 0 = one blob per file
		MaxInflightBatches:    6,                 // Max batches queued
		BloomFPRate:           0.01,              // 1% FP rate
		BloomEstimatedKeys:    1_000_000,         // 1M keys → ~1.2 MB bloom
		BloomRefreshInterval:  10 * time.Minute,
		OrphanCleanupInterval: 1 * time.Hour,
		IO: IOConfig{
			DirectIO: false,
			Fsync:    false,
		},
		Resilience: ResilienceConfig{
			ChecksumHash: nil, // Disabled by default - use WithChecksum()
			VerifyOnRead: false,
			Checksums:    OmitChecksums,
		},
	}
}
