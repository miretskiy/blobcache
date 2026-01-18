package blobcache

import (
	"hash"
	"hash/crc32"

	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/wal"
)

// DegradedMode controls how the cache behaves when entering degraded mode.
type DegradedMode int

const (
	// DegradedMemoryOnly continues operating as a memory-only cache (default).
	// This is the resilient option for production caches.
	DegradedMemoryOnly DegradedMode = iota

	// DegradedPanic panics with a stack trace when degraded mode is triggered.
	// Use this for debugging and benchmarking to catch issues immediately.
	DegradedPanic
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

// CompressionConfig holds compression strategy settings
type CompressionConfig struct {
	Codec   compression.Codex // Compression algorithm (None, Zstd, LZ4, S2)
	Level   compression.Level // Compression level (Default, Speed, Best)
	MinSize int64             // Don't compress blobs smaller than this (default: 512)
}

// WALConfig holds write-ahead log settings
type WALConfig struct {
	Enabled bool // Enable WAL for durability (default: false)
	wal.Config
}

// config holds internal configuration
type config struct {
	Path    string
	MaxSize int64
	Shards  int

	// --- Slab Configuration ---
	WriteBufferSize  int64 // Size of one memory slab
	MaxInflightSlabs int   // Max slabs queueing for flush
	MaxCachedSlabs   int   // Max slabs kept in memory for reading

	LargeWriteThreshold int64
	FlushConcurrency    int
	BloomFPRate         float64
	BloomEstimatedKeys  int
	IO                  IOConfig
	Resilience          ResilienceConfig
	Compression         CompressionConfig
	WAL                 WALConfig
	DegradedMode        DegradedMode // How to handle degraded mode (default: memory-only)

	knobs *TestingKnobs
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

func WithLargeWriteThreshold(size int64) Option {
	return funcOpt(func(c *config) { c.LargeWriteThreshold = size })
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

// WithCompression enables compression with the specified codec.
// Compression is performed in the calling goroutine during Put() to distribute
// CPU load and prevent flush workers from becoming bottlenecks.
func WithCompression(codec compression.Codex) Option {
	return funcOpt(func(c *config) { c.Compression.Codec = codec })
}

// WithCompressionLevel sets the compression level.
func WithCompressionLevel(level compression.Level) Option {
	return funcOpt(func(c *config) { c.Compression.Level = level })
}

// WithCompressionMinSize sets the minimum blob size for compression.
// Blobs smaller than this are stored uncompressed.
func WithCompressionMinSize(size int64) Option {
	return funcOpt(func(c *config) { c.Compression.MinSize = size })
}

// WithTestingKnobs configures testing hooks for error injection and behavior overrides.
func WithTestingKnobs(knobs *TestingKnobs) Option {
	return funcOpt(func(c *config) { c.knobs = knobs })
}

// WithWAL enables the write-ahead log for durability.
// When enabled, all writes are logged to WAL before being acknowledged.
// This transforms blobcache from an ephemeral cache into durable storage.
func WithWAL() Option {
	return funcOpt(func(c *config) {
		c.WAL.Enabled = true
		c.IO.FDataSync = true // If using wal, not using data sync is lying to yourself.
	})
}

// WithWALFlags sets the file flags for WAL writes.
// FlDirectIO: bypass OS page cache (default: enabled)
// FlDSync: fdatasync after writes (default: enabled)
// FlSync: full fsync after writes
// Use sys.SyncNone (0) for testing only.
func WithWALFlags(flags sys.OpenFlag) Option {
	return funcOpt(func(c *config) { c.WAL.Flags = flags })
}

// WithDegradedMode controls how the cache handles degraded mode.
// DegradedMemoryOnly (default): continues operating as memory-only cache
// DegradedPanic: panics with stack trace (use for debugging/benchmarking)
func WithDegradedMode(mode DegradedMode) Option {
	return funcOpt(func(c *config) { c.DegradedMode = mode })
}

func defaultConfig(path string) config {
	return config{
		Path:                path,
		MaxSize:             0,
		Shards:              0,
		WriteBufferSize:     128 << 20, // 128MB
		LargeWriteThreshold: 4 << 20,
		MaxInflightSlabs:    6,
		MaxCachedSlabs:      8, // Keep ~1GB of recently written data in RAM
		FlushConcurrency:    6,
		BloomFPRate:         0.01,
		BloomEstimatedKeys:  1_000_000,
		IO: IOConfig{
			FDataSync:     false,
			Fadvise:       sys.UseFadvise,
			DirectIOWrite: true,
		},

		Compression: CompressionConfig{
			Codec:   compression.CodexNone, // Disabled by default
			Level:   compression.CompressionDefault,
			MinSize: 512, // Don't compress small blobs
		},
		WAL: WALConfig{
			Enabled: false,
			Config:  wal.Config{Flags: sys.FlDirectIO | sys.SyncData},
		},
	}
}
