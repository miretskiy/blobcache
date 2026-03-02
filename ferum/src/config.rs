//! Configuration for Ferum cache.
//!
//! Provides a builder-style API for configuring cache and CAS modes.

use std::path::PathBuf;

use crate::compression::{Codec, Level};
use crate::iosched::IOSchedulerKind;
use crate::sys::OpenFlags;

/// Default write buffer size (128 MB).
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 128 * 1024 * 1024;

/// Default maximum number of in-flight slabs.
pub const DEFAULT_MAX_INFLIGHT_SLABS: usize = 6;

/// Default maximum number of cached slabs for read-after-write.
pub const DEFAULT_MAX_CACHED_SLABS: usize = 8;

/// Default flush concurrency (parallel I/O workers).
pub const DEFAULT_FLUSH_CONCURRENCY: usize = 6;

/// Default maximum cache size (0 = unlimited, set by user).
pub const DEFAULT_MAX_SIZE: u64 = 0;

/// Default minimum size for compression (512 bytes, matching Go).
pub const DEFAULT_COMPRESSION_MIN_SIZE: usize = 512;

/// Default number of segment shards.
pub const DEFAULT_SHARDS: u32 = 4;

/// Default bloom filter false positive rate.
pub const DEFAULT_BLOOM_FP_RATE: f64 = 0.01;

/// Default bloom filter estimated keys.
pub const DEFAULT_BLOOM_ESTIMATED_KEYS: u32 = 1_000_000;

/// Cache configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the cache directory.
    pub path: PathBuf,

    /// Maximum cache size in bytes.
    pub max_size: u64,

    /// Write buffer size (slab size).
    pub write_buffer_size: usize,

    /// Maximum number of in-flight slabs waiting to be flushed.
    pub max_inflight_slabs: usize,

    /// Maximum number of cached slabs for read-after-write optimization.
    pub max_cached_slabs: usize,

    /// Number of concurrent flush workers.
    pub flush_concurrency: usize,

    /// Number of segment directory shards.
    pub shards: u32,

    /// Enable Write-Ahead Log for durability (CAS mode).
    pub wal_enabled: bool,

    /// WAL file open flags.
    pub wal_flags: OpenFlags,

    /// Enable checksum on writes.
    pub checksum_enabled: bool,

    /// Verify checksums on reads.
    pub verify_on_read: bool,

    /// Use Direct I/O for segment writes (O_DIRECT on Linux, F_NOCACHE on Darwin).
    pub direct_io_write: bool,

    /// Use Direct I/O for segment reads (O_DIRECT on Linux, F_NOCACHE on Darwin).
    ///
    /// Default: false (buffered reads leverage kernel page cache).
    /// Enable for iterator-heavy workloads to prevent page cache thrashing from
    /// non-sequential access patterns. See DESIGN.md §11.5.
    pub direct_io_read: bool,

    /// I/O scheduler for segment reads.
    ///
    /// Default: PreadScheduler (synchronous pread(2), portable).
    /// Use URing on Linux for lower latency at high I/O concurrency.
    pub iosched_kind: IOSchedulerKind,

    /// Use fdatasync for durability.
    pub fdatasync: bool,

    /// Use fadvise to provide data access hints to the kernel.
    pub fadvise: bool,

    /// Bloom filter false positive rate.
    pub bloom_fp_rate: f64,

    /// Bloom filter estimated keys.
    pub bloom_estimated_keys: u32,

    /// Compression codec.
    pub compression: Codec,

    /// Compression level.
    pub compression_level: Level,

    /// Minimum size for compression (skip smaller blobs).
    pub compression_min_size: usize,

    /// Behavior when entering degraded mode due to I/O errors.
    pub degraded_mode: DegradedMode,

    /// When true, skip key comparison on Librarian hits (trust hash == key).
    /// Default: true for cache mode, false for CAS/WAL mode (where collisions
    /// would corrupt durable state).
    pub trust_hash: bool,

    /// Enable the redb KeyIndex for ordered iteration and key-by-name lookup.
    /// Default: false (KeyIndex adds per-write overhead).
    pub enable_keyindex: bool,
}

impl Config {
    /// Creates a new configuration with the given path.
    /// Defaults match Go's defaultConfig().
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Config {
            path: path.into(),
            max_size: DEFAULT_MAX_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            max_inflight_slabs: DEFAULT_MAX_INFLIGHT_SLABS,
            max_cached_slabs: DEFAULT_MAX_CACHED_SLABS,
            flush_concurrency: DEFAULT_FLUSH_CONCURRENCY,
            shards: DEFAULT_SHARDS,
            wal_enabled: false,
            wal_flags: OpenFlags::direct_dsync(),
            checksum_enabled: false,
            verify_on_read: false,
            direct_io_write: true,  // Default: Direct I/O for writes (matches Go)
            direct_io_read: false,  // Default: buffered reads (leverage page cache)
            iosched_kind: IOSchedulerKind::Pread, // Default: synchronous pread(2)
            fdatasync: false,       // Default: false (matches Go)
            fadvise: crate::sys::USE_FADVISE, // Platform-dependent
            bloom_fp_rate: DEFAULT_BLOOM_FP_RATE,
            bloom_estimated_keys: DEFAULT_BLOOM_ESTIMATED_KEYS,
            compression: Codec::None,
            compression_level: Level::Default,
            compression_min_size: DEFAULT_COMPRESSION_MIN_SIZE,
            degraded_mode: DegradedMode::Log,
            trust_hash: true,   // Cache mode default: trust hash
            enable_keyindex: false,
        }
    }

    /// Returns the OpenFlags for segment writes based on config.
    pub fn segment_write_flags(&self) -> OpenFlags {
        OpenFlags {
            direct_io: self.direct_io_write,
            dsync: self.fdatasync,
            sync: false,
        }
    }

    /// Sets the number of segment shards.
    pub fn shards(mut self, count: u32) -> Self {
        self.shards = count;
        self
    }

    /// Sets the maximum cache size.
    pub fn max_size(mut self, size: u64) -> Self {
        self.max_size = size;
        self
    }

    /// Sets the write buffer (slab) size.
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Sets the maximum number of in-flight slabs.
    pub fn max_inflight_slabs(mut self, count: usize) -> Self {
        self.max_inflight_slabs = count;
        self
    }

    /// Sets the maximum number of cached slabs for read-after-write.
    pub fn max_cached_slabs(mut self, count: usize) -> Self {
        self.max_cached_slabs = count;
        self
    }

    /// Sets the flush concurrency (parallel I/O workers).
    pub fn flush_concurrency(mut self, count: usize) -> Self {
        self.flush_concurrency = count;
        self
    }

    /// Enables the Write-Ahead Log for durability (CAS mode).
    /// Also disables hash trust (key verification required for correctness).
    pub fn with_wal(mut self) -> Self {
        self.wal_enabled = true;
        self.trust_hash = false;
        self
    }

    /// Sets WAL file open flags.
    pub fn wal_flags(mut self, flags: OpenFlags) -> Self {
        self.wal_flags = flags;
        self
    }

    /// Enables checksum on writes.
    pub fn with_checksum(mut self) -> Self {
        self.checksum_enabled = true;
        self
    }

    /// Enables checksum verification on reads.
    pub fn with_verify_on_read(mut self) -> Self {
        self.verify_on_read = true;
        self
    }

    /// Enables or disables Direct I/O for segment writes.
    pub fn direct_io_write(mut self, enabled: bool) -> Self {
        self.direct_io_write = enabled;
        self
    }

    /// Enables or disables Direct I/O for segment reads.
    ///
    /// When enabled, fadvise is automatically suppressed. Recommended for
    /// iterator-heavy workloads to prevent page cache thrashing.
    pub fn direct_io_read(mut self, enabled: bool) -> Self {
        self.direct_io_read = enabled;
        self
    }

    /// Sets the I/O scheduler for segment reads.
    pub fn io_scheduler(mut self, kind: IOSchedulerKind) -> Self {
        self.iosched_kind = kind;
        self
    }

    /// Enables fdatasync for durability.
    pub fn with_fdatasync(mut self, enabled: bool) -> Self {
        self.fdatasync = enabled;
        self
    }

    /// Enables fadvise for read hints.
    pub fn with_fadvise(mut self, enabled: bool) -> Self {
        self.fadvise = enabled;
        self
    }

    /// Sets the bloom filter false positive rate.
    pub fn bloom_fp_rate(mut self, rate: f64) -> Self {
        self.bloom_fp_rate = rate;
        self
    }

    /// Sets the bloom filter estimated keys.
    pub fn bloom_estimated_keys(mut self, keys: u32) -> Self {
        self.bloom_estimated_keys = keys;
        self
    }

    /// Sets the compression codec.
    pub fn compression(mut self, codec: Codec) -> Self {
        self.compression = codec;
        self
    }

    /// Sets the compression level.
    pub fn compression_level(mut self, level: Level) -> Self {
        self.compression_level = level;
        self
    }

    /// Sets the minimum size for compression.
    pub fn compression_min_size(mut self, size: usize) -> Self {
        self.compression_min_size = size;
        self
    }

    /// Sets the degraded mode behavior.
    pub fn degraded_mode(mut self, mode: DegradedMode) -> Self {
        self.degraded_mode = mode;
        self
    }

    /// Enables the redb KeyIndex for ordered iteration.
    pub fn with_keyindex(mut self) -> Self {
        self.enable_keyindex = true;
        self
    }

    /// Enables or disables hash trust for Librarian hits.
    ///
    /// When `true`, the stored key is not verified against the expected key
    /// on Librarian hits (saves one comparison). Safe for cache mode where
    /// a hash collision just returns wrong data (eviction will fix it).
    ///
    /// Set to `false` in CAS/WAL mode where correctness is critical.
    pub fn with_trust_hash(mut self, trusted: bool) -> Self {
        self.trust_hash = trusted;
        self
    }

    /// Validates the configuration.
    pub fn validate(&self) -> crate::Result<()> {
        use crate::error::Error;

        if self.write_buffer_size == 0 {
            return Err(Error::InvalidConfig {
                message: "write_buffer_size must be > 0".to_string(),
            });
        }

        if self.max_inflight_slabs == 0 {
            return Err(Error::InvalidConfig {
                message: "max_inflight_slabs must be > 0".to_string(),
            });
        }

        if self.flush_concurrency == 0 {
            return Err(Error::InvalidConfig {
                message: "flush_concurrency must be > 0".to_string(),
            });
        }

        // write_buffer_size should be a power of 2 for optimal alignment
        if !self.write_buffer_size.is_power_of_two() {
            return Err(Error::InvalidConfig {
                message: "write_buffer_size should be a power of 2".to_string(),
            });
        }

        Ok(())
    }
}

/// Behavior when the cache enters degraded mode due to I/O errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DegradedMode {
    /// Serve RAM (Librarian) hits only; skip index + archivist after first error.
    MemoryOnly,
    /// Log the error and continue (best effort).
    #[default]
    Log,
    /// Return errors to callers but continue operating.
    Return,
    /// Panic immediately (useful for testing).
    Panic,
}

/// Builder for cache mode configuration.
pub fn cache(path: impl Into<PathBuf>) -> Config {
    Config::new(path)
}

/// Builder for CAS mode configuration.
pub fn cas(path: impl Into<PathBuf>) -> Config {
    Config::new(path)
        .with_wal()
        .with_checksum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = Config::new("/tmp/cache");
        // Match Go's defaultConfig()
        assert_eq!(config.write_buffer_size, 128 << 20); // 128MB
        assert_eq!(config.max_inflight_slabs, 6);
        assert_eq!(config.max_cached_slabs, 8);
        assert_eq!(config.flush_concurrency, 6);
        assert_eq!(config.bloom_fp_rate, 0.01);
        assert_eq!(config.bloom_estimated_keys, 1_000_000);
        assert!(config.direct_io_write); // Default: true (like Go)
        assert!(!config.fdatasync);      // Default: false (like Go)
        assert!(!config.wal_enabled);
        assert!(!config.checksum_enabled);
        assert!(!config.verify_on_read);
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new("/tmp/cache")
            .max_size(1 << 40)
            .write_buffer_size(1 << 30)
            .with_wal()
            .with_checksum()
            .compression(Codec::Zstd)
            .compression_level(Level::Speed);

        assert_eq!(config.max_size, 1 << 40);
        assert_eq!(config.write_buffer_size, 1 << 30);
        assert!(config.wal_enabled);
        assert!(config.checksum_enabled);
        assert_eq!(config.compression, Codec::Zstd);
        assert_eq!(config.compression_level, Level::Speed);
    }

    #[test]
    fn test_config_validation() {
        let config = Config::new("/tmp/cache");
        assert!(config.validate().is_ok());

        let bad_config = Config::new("/tmp/cache").write_buffer_size(0);
        assert!(bad_config.validate().is_err());

        let bad_config = Config::new("/tmp/cache").write_buffer_size(1000); // Not power of 2
        assert!(bad_config.validate().is_err());
    }

    #[test]
    fn test_cas_preset() {
        let config = cas("/tmp/cas");
        assert!(config.wal_enabled);
        assert!(config.checksum_enabled);
    }
}
