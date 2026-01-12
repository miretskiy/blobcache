package blobcache

import (
	"hash"
	"hash/crc32"
	
	"github.com/cespare/xxhash/v2"
)

// IOConfig holds I/O strategy settings
type IOConfig struct {
	FDataSync     bool // Use fdatasync for durability
	Fadvise       bool // Use fadvise to provide data access hints to the kernel.
	DirectIOWrite bool // Use O_DIRECT (Linux) or F_NOCACHE (Darwin) for segment writes
}

// ResilienceConfig holds data integrity settings
type ResilienceConfig struct {
	ChecksumHasher Hasher // Hash factory for checksums (nil = disabled)
	VerifyOnRead   bool   // Verify checksums on reads
}

type KeyHasherFn func(b []byte) uint64

// config holds internal configuration
type config struct {
	Path      string
	MaxSize   int64
	KeyHasher KeyHasherFn
	Shards    int
	
	// --- Slab Configuration ---
	WriteBufferSize  int64 // Size of one memory slab
	MaxInflightSlabs int   // Max slabs queueing for flush
	MaxCachedSlabs   int   // Max slabs kept in memory for reading
	
	LargeWriteThreshold int64
	SegmentSize         int64
	FlushConcurrency    int
	BloomFPRate         float64
	BloomEstimatedKeys  int
	IO                  IOConfig
	Resilience          ResilienceConfig
	
	// Testing hooks
	testingInjectWriteErr func() error
	testingInjectIndexErr func() error
	testingInjectEvictErr func() error
}

// Option configures BlobCache
type Option interface {
	apply(*config)
}

type funcOpt func(*config)

func (f funcOpt) apply(c *config) {
	f(c)
}

func WithMaxSize(size int64) Option {
	return funcOpt(func(c *config) { c.MaxSize = size })
}

func WithShards(n int) Option {
	return funcOpt(func(c *config) { c.Shards = n })
}

// WithWriteBufferSize sets the size of the memory chunks used for buffering.
// Default: 128MB.
func WithWriteBufferSize(bytes int64) Option {
	return funcOpt(func(c *config) { c.WriteBufferSize = bytes })
}

// WithMaxInflightSlabs sets how many slabs can be queued for flushing
// before backpressure (blocking writes) kicks in.
func WithMaxInflightSlabs(n int) Option {
	return funcOpt(func(c *config) { c.MaxInflightSlabs = n })
}

// WithMaxCachedSlabs sets how many sealed slabs are kept in memory for reading.
// Increasing this improves read performance for recently written data at the cost of RAM.
// Set to 0 to disable the in-memory read cache (all reads go to disk).
// Default: 4.
func WithMaxCachedSlabs(n int) Option {
	return funcOpt(func(c *config) { c.MaxCachedSlabs = n })
}

func WithBloomFPRate(rate float64) Option {
	return funcOpt(func(c *config) { c.BloomFPRate = rate })
}

func WithBloomEstimatedKeys(n int) Option {
	return funcOpt(func(c *config) { c.BloomEstimatedKeys = n })
}

func WithChecksum() Option {
	return funcOpt(func(c *config) {
		c.Resilience.ChecksumHasher = func() hash.Hash32 { return crc32.NewIEEE() }
	})
}

func WithChecksumHash(factory Hasher) Option {
	return funcOpt(func(c *config) { c.Resilience.ChecksumHasher = factory })
}

func WithFDataSync(enabled bool) Option {
	return funcOpt(func(c *config) { c.IO.FDataSync = enabled })
}

func WithVerifyOnRead(enabled bool) Option {
	return funcOpt(func(c *config) { c.Resilience.VerifyOnRead = enabled })
}

func WithSegmentSize(size int64) Option {
	return funcOpt(func(c *config) { c.SegmentSize = size })
}

func WithLargeWriteThreshold(size int64) Option {
	return funcOpt(func(c *config) { c.LargeWriteThreshold = size })
}

func WithKeyHasher(hasher KeyHasherFn) Option {
	return funcOpt(func(c *config) { c.KeyHasher = hasher })
}

func WithTestingFlushOnPut() Option {
	return funcOpt(func(c *config) { c.SegmentSize = 0 })
}

func WithTestingInjectWriteError(fn func() error) Option {
	return funcOpt(func(c *config) { c.testingInjectWriteErr = fn })
}

func WithTestingInjectIndexError(fn func() error) Option {
	return funcOpt(func(c *config) { c.testingInjectIndexErr = fn })
}

func WithTestingInjectEvictError(fn func() error) Option {
	return funcOpt(func(c *config) { c.testingInjectEvictErr = fn })
}

func WithFlushConcurrency(n int) Option {
	return funcOpt(func(c *config) { c.FlushConcurrency = n })
}

func WithFadvise(enabled bool) Option {
	return funcOpt(func(c *config) { c.IO.Fadvise = enabled })
}

func WithDirectIOWrite(enabled bool) Option {
	return funcOpt(func(c *config) { c.IO.DirectIOWrite = enabled })
}

func defaultConfig(path string) config {
	return config{
		Path:                path,
		MaxSize:             0,
		KeyHasher:           func(b []byte) uint64 { return xxhash.Sum64(b) },
		Shards:              0,
		WriteBufferSize:     128 << 20, // 128MB
		LargeWriteThreshold: 4 << 20,
		SegmentSize:         2 << 30, // 2GB
		MaxInflightSlabs:    6,
		MaxCachedSlabs:      8, // Keep ~1GB of recently written data in RAM
		FlushConcurrency:    6,
		BloomFPRate:         0.01,
		BloomEstimatedKeys:  1_000_000,
		IO:                  defaultIOConfig,
	}
}
