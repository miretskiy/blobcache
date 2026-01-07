package blobcache

import (
	"hash"
	"hash/crc32"

	"github.com/cespare/xxhash/v2"
)

// IOConfig holds I/O strategy settings
type IOConfig struct {
	FDataSync bool // Use fdatasync for durability
	Fadvise   bool // Use fadvise to provide data access hints to the kernel.
}

// ResilienceConfig holds data integrity settings
type ResilienceConfig struct {
	ChecksumHasher Hasher // Hash factory for checksums (nil = disabled)
	VerifyOnRead   bool   // Verify checksums on reads
}

type KeyHasherFn func(b []byte) uint64

// config holds internal configuration
type config struct {
	Path                string
	MaxSize             int64
	KeyHasher           KeyHasherFn
	Shards              int   // Number of shard directories (default: 0 = no sharding)
	WriteBufferSize     int64 // Memtable batch size in bytes (like RocksDB write_buffer_size)
	LargeWriteThreshold int64 // Writes exceeding this threshold are handed off to the flusher directly.
	SegmentSize         int64 // Target segment file size (0 = flush on every write, for testing)
	MaxInflightBatches  int   // Max batches queued (like RocksDB max_write_buffer_number)
	FlushConcurrency    int   // How many flush workers to have; default: MaxInflightBatches
	BloomFPRate         float64
	BloomEstimatedKeys  int
	IO                  IOConfig
	Resilience          ResilienceConfig

	// Testing hooks
	testingInjectWriteErr func() error // Called before writer.Write() in flush
	testingInjectIndexErr func() error // Called before index.PutBatch() in flush
	testingInjectEvictErr func() error // Called in runEviction()
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

// WithShards sets the number of filesystem shards (default: 0 = no sharding)
// Can only be set for new cache (not for existing)
func WithShards(n int) Option {
	return funcOpt(func(c *config) {
		c.Shards = n
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

// WithChecksum enables CRC32 checksums (hardware accelerated)
func WithChecksum() Option {
	return funcOpt(func(c *config) {
		c.Resilience.ChecksumHasher = func() hash.Hash32 {
			return crc32.NewIEEE()
		}
	})
}

// WithChecksumHash enables checksums with a custom hash factory
// Example: WithChecksumHash(crc32.NewIEEE) or WithChecksumHash(xxhash.New)
func WithChecksumHash(factory Hasher) Option {
	return funcOpt(func(c *config) {
		c.Resilience.ChecksumHasher = factory
	})
}

// WithFDataSync enables fdatasync on writes (default: false, cache semantics)
// Uses fdatasync(2) on Linux and F_FULLFSYNC on Darwin for data durability
// Note: Syncing data flushes only - metadata (mtime, etc.) not synced since immutable blobs don't need it
func WithFDataSync(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.IO.FDataSync = enabled
	})
}

// WithVerifyOnRead enables checksum verification on reads (default: false, opt-in)
func WithVerifyOnRead(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.Resilience.VerifyOnRead = enabled
	})
}

// WithSegmentSize sets target segment file size (default: 32MB)
// Multiple blobs are written to large segment mu up to this size
// Set to 0 to flush on every write (testing mode - creates one segment per blob)
func WithSegmentSize(size int64) Option {
	return funcOpt(func(c *config) {
		c.SegmentSize = size
	})
}

// WithLargeWriteThreshold sets the threshold when the writes exceeding this
// value are sent directly to the flusher, bypassing normal memtable.
func WithLargeWriteThreshold(size int64) Option {
	return funcOpt(func(c *config) {
		c.LargeWriteThreshold = size
	})
}

// WithKeyHasher configures this cache to use specified key hasher
// Default: xxhash
func WithKeyHasher(hasher KeyHasherFn) Option {
	return funcOpt(func(c *config) {
		c.KeyHasher = hasher
	})
}

// WithTestingFlushOnPut configures this cache to create separate mu
// for each key.
func WithTestingFlushOnPut() Option {
	return funcOpt(func(c *config) {
		c.SegmentSize = 0
	})
}

// WithTestingInjectWriteError injects errors during blob writes in flush worker
func WithTestingInjectWriteError(fn func() error) Option {
	return funcOpt(func(c *config) {
		c.testingInjectWriteErr = fn
	})
}

// WithTestingInjectIndexError injects errors during index updates in flush worker
func WithTestingInjectIndexError(fn func() error) Option {
	return funcOpt(func(c *config) {
		c.testingInjectIndexErr = fn
	})
}

// WithTestingInjectEvictError injects errors during eviction
func WithTestingInjectEvictError(fn func() error) Option {
	return funcOpt(func(c *config) {
		c.testingInjectEvictErr = fn
	})
}

// WithMaxInflightBatches configures blob cache with the specified number
// of the outstanding (waiting for flush) memtables write buffers (memtables).
func WithMaxInflightBatches(n int) Option {
	return funcOpt(func(c *config) {
		c.MaxInflightBatches = n
	})
}

// WithFlushConcurrency configures blobcache with the specified number
// of "flushers" -- go routines performing IO.
func WithFlushConcurrency(n int) Option {
	return funcOpt(func(c *config) {
		c.FlushConcurrency = n
	})
}

// WithFadvise allows enabling/disabling kernel page cache hints (default: true).
func WithFadvise(enabled bool) Option {
	return funcOpt(func(c *config) {
		c.IO.Fadvise = enabled
	})
}

// defaultConfig returns sensible defaults (path set by caller)
func defaultConfig(path string) config {
	return config{
		Path:                path,
		MaxSize:             0, // TODO: Auto-detect 80% of disk capacity
		KeyHasher:           func(b []byte) uint64 { return xxhash.Sum64(b) },
		Shards:              0,
		WriteBufferSize:     128 << 20, // 100MB (production ~1GB)
		LargeWriteThreshold: 4 << 20,
		SegmentSize:         2 << 30,   // 64MB
		MaxInflightBatches:  6,         // Max batches queued
		FlushConcurrency:    6,         // same as MaxInflightBatches
		BloomFPRate:         0.01,      // 1% FP rate
		BloomEstimatedKeys:  1_000_000, // 1M keys → ~1.2 MB bloom
		IO:                  defaultIOConfig,
	}
}
