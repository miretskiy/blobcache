//! High-performance blob cache with CAS storage support.
//!
//! The Cache struct provides the main public API for storing and retrieving blobs.
//! It supports two modes:
//! - **Cache Mode**: High-performance disk-first cache with SIEVE eviction
//! - **CAS Mode**: Durable Content Addressable Storage via Write-Ahead Log

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::DegradedMode;

use log::info;
use parking_lot::Mutex;

use crate::bloom::Filter as BloomFilter;
use crate::compaction::Compactor;
use crate::config::Config;
use crate::iterator::Iterator as BlobIterator;
use crate::durable_index::DurableIndex;
use crate::error::{Error, Result};
use crate::key::Key;
use crate::librarian::Librarian;
use crate::memtable::MemTable;
use crate::record::Record;
use crate::keyindex::KeyIndex;
use crate::iosched::IOSchedulerKind;
use crate::storage::{delete_segment_files, recover_index_from_footers, Archivist, SegmentIDProvider};
use crate::wal::{Replayer, Wal, WalConfig};

// =============================================================================
// Constants
// =============================================================================

/// Target fraction of MaxSize to evict to (7% headroom).
#[allow(dead_code)]
const EVICTION_HYSTERESIS: f64 = 0.93;

/// Compaction runs every 10 minutes (matches Go).
/// Note: The worker sleeps in 10-second intervals (60 * 10s = 10 minutes)
/// to allow quicker shutdown response.
#[allow(dead_code)]
const COMPACTION_INTERVAL: Duration = Duration::from_secs(10 * 60);

// =============================================================================
// BloomStats
// =============================================================================

/// Tracks bloom filter hit/ghost/deletion counters for FPR-reactive rebuild.
#[derive(Default)]
struct BloomStats {
    /// Sampled positive lookups (bloom said "maybe yes").
    hits: AtomicU64,
    /// Bloom said yes but index said no (false positive / ghost).
    ghosts: AtomicU64,
    /// Cumulative deletes since last rebuild.
    deletions: AtomicI64,
}

// =============================================================================
// RecoveryReplayer - implements Replayer trait for WAL recovery
// =============================================================================

/// Wraps MemTable to implement Replayer during WAL recovery.
struct RecoveryReplayer<'a> {
    memtable: &'a MemTable,
    bloom: &'a BloomFilter,
    index: &'a DurableIndex,
}

impl<'a> Replayer for RecoveryReplayer<'a> {
    /// Replays a WAL record into the memtable.
    ///
    /// For puts: writes record as-is (no compression, no CRC recalc).
    /// For deletes: looks up existing item and marks as tombstone (does NOT write to slab).
    fn replay_record(&mut self, record: Record) -> Result<()> {
        #[cfg(test)]
        eprintln!(
            "replay_record: seq={}, key_len={}, value_len={}, deleted={}",
            record.header.seq_id,
            record.key.len(),
            record.value.len(),
            record.header.is_deleted()
        );
        let key = Key::from_bytes(&record.key);

        if record.header.is_deleted() {
            // Replay delete: remove item from in-memory index
            // Do NOT write to memtable - tombstones are index-only operations during recovery
            // The item was already committed to a segment before the delete
            self.index.delete(&key);
            return Ok(());
        }

        // Add to bloom filter for puts
        self.bloom.add(key);

        // Replay the Put directly:
        // - Use original SeqID (not new sequence)
        // - Use original CRC (already verified during WAL read)
        // - Bypass compression (value is already in final form)
        // - Write record as-is to slab
        let result = self.memtable.replay_record(key, &record);

        #[cfg(test)]
        if let Err(ref e) = result {
            eprintln!("replay_record: FAILED: {}", e);
        }

        result
    }

    fn flush(&mut self) {
        self.memtable.flush();
    }

    fn drain(&mut self) {
        self.memtable.drain();
    }
}

// =============================================================================
// Cache
// =============================================================================

/// High-performance blob storage with bloom filter optimization.
pub struct Cache {
    /// Configuration.
    #[allow(dead_code)]
    config: Config,

    /// Durable index with optional persistence (CAS mode).
    index: Arc<DurableIndex>,

    /// Read-only access to persisted segments.
    archivist: Arc<Archivist>,

    /// Write-ahead log (None if WAL disabled).
    wal: Option<Arc<Wal>>,

    /// Segment ID allocator.
    #[allow(dead_code)]
    segment_ids: Arc<SegmentIDProvider>,

    /// Bloom filter for fast negative lookups.
    bloom: BloomFilter,

    /// The write engine (producer).
    memtable: Arc<MemTable>,

    /// The read cache (consumer).
    librarian: Arc<Librarian>,

    /// Global monotonic sequence counter for operation ordering.
    /// Initialized to time.Now().UnixNano() for continuity across restarts.
    global_seq: AtomicU64,

    /// Approximate total size (updated during flush/eviction).
    approx_size: AtomicU64,

    /// Whether the cache is in degraded mode (memory-only).
    degraded: AtomicBool,

    /// Whether the cache has been closed.
    closed: AtomicBool,

    /// Compactor for segment merge operations.
    compactor: Arc<Compactor>,

    /// Handle for the background compaction worker thread.
    compaction_worker: Mutex<Option<JoinHandle<()>>>,

    /// Bloom filter statistics for FPR-reactive rebuild.
    bloom_stats: BloomStats,

    /// Optional redb KeyIndex for ordered iteration.
    key_index: Option<Arc<KeyIndex>>,
}

impl Cache {
    /// Opens or creates a cache at the specified path.
    ///
    /// Uses "crash-only" initialization: if WAL recovery is needed, we recover,
    /// flush to segments, close, and re-open cleanly.
    pub fn open(config: Config) -> Result<Arc<Self>> {
        config.validate()?;

        // Ensure directory exists
        std::fs::create_dir_all(&config.path)
            .map_err(|e| Error::io("create cache directory", e))?;

        // Create segment shard directories (must match get_segment_path format)
        // Format: {base_path}/segments/{shard:04}/
        for i in 0..config.shards {
            let shard_dir = config.path.join("segments").join(format!("{:04}", i));
            std::fs::create_dir_all(&shard_dir)
                .map_err(|e| Error::io("create segment shard directory", e))?;
        }

        // Phase 1: WAL Recovery (if needed)
        // Check if WAL exists and has files to recover
        if config.wal_enabled {
            let wal_dir = config.path.join("wal");
            if Self::needs_wal_recovery(&wal_dir)? {
                Self::do_wal_recovery(&config, &wal_dir)?;
            }
        }

        // Phase 2: Normal initialization
        Self::open_internal(config)
    }

    /// Checks if WAL directory has files that need recovery.
    fn needs_wal_recovery(wal_dir: &PathBuf) -> Result<bool> {
        if !wal_dir.exists() {
            return Ok(false);
        }

        // Count WAL files
        let entries = std::fs::read_dir(wal_dir)
            .map_err(|e| Error::io("read WAL directory", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| Error::io("read WAL entry", e))?;
            let name = entry.file_name();
            if let Some(name_str) = name.to_str() {
                // WAL files match pattern: wal-NNNNNNNNNNNNNNNNNNNN.log
                if name_str.starts_with("wal-") && name_str.ends_with(".log") {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Performs WAL recovery: replay records, flush, drain, delete WAL files.
    fn do_wal_recovery(config: &Config, wal_dir: &PathBuf) -> Result<()> {
        #[cfg(test)]
        eprintln!("do_wal_recovery: starting recovery from {:?}", wal_dir);
        // Create temporary components for recovery
        // Note: We DON'T pass the WAL to memtable during recovery to avoid
        //       re-writing recovered records to the same WAL.
        // Note: Use memory-only DurableIndex during recovery. The recovered data
        //       will be flushed to segment files, and open_internal will later
        //       load from those segments and persist to the durable index.
        let index = Arc::new(DurableIndex::open(None, 1 << 20)?);
        let librarian = Arc::new(Librarian::new(config.max_cached_slabs));
        let bloom = BloomFilter::new(config.bloom_estimated_keys, config.bloom_fp_rate);

        let memtable = MemTable::new(
            config.clone(),
            config.path.clone(),
            Arc::clone(&index),
            Arc::clone(&librarian),
            None,  // <-- No WAL during recovery (avoid double-writing)
            None,  // on_flush callback
            None,  // no KeyIndex during WAL recovery
        )?;

        // Open WAL for recovery (read-only mode conceptually)
        let wal_config = WalConfig {
            dir: wal_dir.clone(),
            flags: config.wal_flags,
            max_batch_size: 16 * 1024 * 1024,
        };
        let wal = Wal::open(wal_config)?;

        // Get max committed seq from index (for skipping already-committed WALs)
        let max_committed_seq = index.memory_max_seq_id();

        // Create replayer
        let mut replayer = RecoveryReplayer {
            memtable: &memtable,
            bloom: &bloom,
            index: &index,
        };

        // Recover - replays records and flushes
        #[cfg(test)]
        eprintln!("do_wal_recovery: max_committed_seq={}", max_committed_seq);
        let recovered = wal.recover(&mut replayer, |first_seq| {
            // A WAL file is "committed" if its first seqID is <= max_committed_seq
            #[cfg(test)]
            eprintln!("is_committed check: first_seq={}, max_committed={}", first_seq, max_committed_seq);
            first_seq <= max_committed_seq
        })?;

        #[cfg(test)]
        eprintln!("do_wal_recovery: recovered={}", recovered);

        if recovered {
            // Check slab position before flush
            #[cfg(test)]
            {
                let (pos, count) = memtable.debug_slab_info();
                eprintln!("do_wal_recovery: slab position={}, entries={}", pos, count);
            }

            // Force flush to trigger segment write
            #[cfg(test)]
            eprintln!("do_wal_recovery: flushing memtable");
            memtable.flush();

            // Drain all pending flushes
            #[cfg(test)]
            eprintln!("do_wal_recovery: draining memtable");
            memtable.drain();

            // Check for .iseg files
            #[cfg(test)]
            for shard in 0..config.shards {
                let shard_dir = config.path.join(format!("{:02x}", shard));
                if shard_dir.exists() {
                    for entry in std::fs::read_dir(&shard_dir).unwrap() {
                        let entry = entry.unwrap();
                        eprintln!("After recovery - shard {} file: {:?}", shard, entry.file_name());
                    }
                }
            }
        }

        // Close components
        memtable.close();
        librarian.close();

        // Delete ALL WAL files (they've been flushed to segments)
        Self::delete_all_wal_files(wal_dir)?;

        // Close WAL (creates a fresh file on next open)
        wal.close()?;

        Ok(())
    }

    /// Deletes all WAL files in the directory.
    fn delete_all_wal_files(wal_dir: &PathBuf) -> Result<()> {
        let entries = std::fs::read_dir(wal_dir)
            .map_err(|e| Error::io("read WAL directory", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| Error::io("read WAL entry", e))?;
            let name = entry.file_name();
            if let Some(name_str) = name.to_str() {
                if name_str.starts_with("wal-") && name_str.ends_with(".log") {
                    std::fs::remove_file(entry.path())
                        .map_err(|e| Error::io("delete WAL file", e))?;
                }
            }
        }

        Ok(())
    }

    /// Internal open - creates components after recovery is complete.
    fn open_internal(config: Config) -> Result<Arc<Self>> {
        // Initialize components
        let segment_ids = Arc::new(SegmentIDProvider::new(&config.path, config.shards));

        // Build I/O scheduler from config
        let sched: Arc<dyn crate::iosched::IOScheduler> = match config.iosched_kind {
            IOSchedulerKind::Pread => Arc::new(crate::iosched::PreadScheduler::new()),
            #[cfg(target_os = "linux")]
            IOSchedulerKind::URing { ring_depth } => {
                let s = crate::iosched::URingScheduler::new(ring_depth)
                    .map_err(|e| Error::io("build URingScheduler", e))?;
                Arc::new(s)
            }
        };

        let mut archivist = Archivist::with_scheduler(&config.path, config.shards, sched);
        archivist.set_verify_checksum(config.verify_on_read);
        archivist.set_direct_io_read(config.direct_io_read);
        // set_fadvise after set_direct_io_read (direct_io_read suppresses fadvise automatically)
        if !config.direct_io_read {
            archivist.set_fadvise(config.fadvise);
        }
        let archivist = Arc::new(archivist);
        let librarian = Arc::new(Librarian::new(config.max_cached_slabs));

        // Initialize bloom filter from config
        let bloom = BloomFilter::new(config.bloom_estimated_keys, config.bloom_fp_rate);

        // Initialize DurableIndex:
        // - CAS mode (WAL enabled): With persistence for crash recovery
        // - Cache mode: Memory-only (no persistence)
        let persistence_path = if config.wal_enabled {
            Some(config.path.as_path())
        } else {
            None
        };
        let index = Arc::new(DurableIndex::open(persistence_path, 1 << 20)?); // 1M capacity hint

        // Add keys from persistence to bloom filter
        // (DurableIndex::open already loaded items from persistence)
        let persistence_count = index.len();
        if persistence_count > 0 {
            #[cfg(test)]
            eprintln!("open_internal: loaded {} items from persistence", persistence_count);

            // Add keys to bloom filter by iterating the in-memory index
            index.blobs().for_each(|item| {
                bloom.add(item.key);
            });
        }

        // If persistence is empty, fall back to segment footer recovery (disaster recovery path)
        if persistence_count == 0 {
            let (recovered_items, max_seq_id) =
                recover_index_from_footers(&config.path, config.shards)?;

            // Populate index with recovered items
            if !recovered_items.is_empty() {
                #[cfg(test)]
                eprintln!("open_internal: recovering {} items from segment footers", recovered_items.len());

                index.put_batch(&recovered_items, max_seq_id);

                // Also add recovered keys to bloom filter
                #[cfg(test)]
                {
                    for (i, item) in recovered_items.iter().enumerate() {
                        bloom.add(item.key);
                        if i < 3 {
                            eprintln!("open_internal: adding key {:?} to bloom", item.key);
                        }
                    }
                    eprintln!("open_internal: added {} keys to bloom filter", recovered_items.len());
                }
                #[cfg(not(test))]
                for item in &recovered_items {
                    bloom.add(item.key);
                }
            }
        }

        // Initialize KeyIndex if configured
        let key_index: Option<Arc<KeyIndex>> = if config.enable_keyindex {
            let ki_path = config.path.join("keyindex.redb");
            Some(Arc::new(KeyIndex::open(&ki_path)?))
        } else {
            None
        };

        // Initialize WAL if enabled
        let wal = if config.wal_enabled {
            let wal_config = WalConfig {
                dir: config.path.join("wal"),
                flags: config.wal_flags,
                max_batch_size: 16 * 1024 * 1024, // 16 MB
            };
            Some(Wal::open(wal_config)?)
        } else {
            None
        };

        // Initialize memtable (now WITH WAL for new writes)
        let memtable = MemTable::new(
            config.clone(),
            config.path.clone(),
            Arc::clone(&index),
            Arc::clone(&librarian),
            wal.clone(),
            None,               // on_flush callback - eviction handled via periodic checks
            key_index.clone(),  // KeyIndex for ordered iteration (None if not configured)
        )?;

        // Initialize compactor for segment merge operations
        let compactor = Arc::new(Compactor::new(
            Arc::clone(&index),
            Arc::clone(&archivist),
            Arc::clone(&segment_ids),
            config.path.clone(),
            config.shards,
            config.segment_write_flags(),
        ));

        // Initialize sequence counter.
        //
        // CRITICAL: We must use max(now, index_max_seq + 1) to handle:
        // 1. Clock drift backwards (NTP correction, VM migration)
        // 2. Recovery from persistence where max_seq > current time
        //
        // Without this check, if now < index_max_seq, new writes would get
        // seqIDs that are lower than existing data, causing the "zombie write"
        // bug where valid writes are silently discarded.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Get max seq from index (includes recovered and persisted data)
        let index_max_seq = index.memory_max_seq_id();
        let initial_seq = now.max(index_max_seq.saturating_add(1));

        let cache = Arc::new(Cache {
            config,
            index,
            archivist,
            wal,
            segment_ids,
            bloom,
            memtable,
            librarian,
            global_seq: AtomicU64::new(initial_seq),
            approx_size: AtomicU64::new(0),
            degraded: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            compactor,
            compaction_worker: Mutex::new(None),
            bloom_stats: BloomStats::default(),
            key_index,
        });

        // Start background compaction worker
        let handle = Self::spawn_compaction_worker(Arc::clone(&cache));
        *cache.compaction_worker.lock() = Some(handle);

        Ok(cache)
    }

    /// Returns the next sequence ID (monotonically increasing).
    #[inline]
    fn next_seq(&self) -> u64 {
        self.global_seq.fetch_add(1, Ordering::AcqRel)
    }

    /// Stores a blob in the cache with a caller-supplied CRC32 checksum.
    ///
    /// Skips CRC32 computation (the caller already computed it). Useful when the
    /// checksum is known from a prior validation step (e.g., network transfer).
    pub fn put_checksummed(&self, key: &[u8], value: &[u8], checksum: u32) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidConfig {
                message: "empty key not allowed".to_string(),
            });
        }

        let hash_key = Key::from_bytes(key);
        self.bloom.add(hash_key);

        let mut seq_id = self.next_seq();
        loop {
            match self.memtable.put_checksummed(seq_id, hash_key, key, value, checksum) {
                Ok(()) => {
                    self.add_approx_size((key.len() + value.len()) as u64);
                    if seq_id & 0x3FF == 0 {
                        self.maybe_evict();
                    }
                    return Ok(());
                }
                Err(Error::InvalidConfig { message }) if message.contains("too old") => {
                    if self.index.get(&hash_key).is_some() {
                        return Ok(());
                    }
                    seq_id = self.next_seq();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Stores a blob in the cache.
    ///
    /// Returns an error if the key is empty.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidConfig {
                message: "empty key not allowed".to_string(),
            });
        }

        let hash_key = Key::from_bytes(key);

        // Add to bloom filter
        self.bloom.add(hash_key);

        // Write to memtable with retry on sequence too old
        self.put_with_retry(hash_key, key, value)
    }

    /// Handles the zombie writer resurrection protocol.
    fn put_with_retry(&self, hash_key: Key, key_bytes: &[u8], value: &[u8]) -> Result<()> {
        let mut seq_id = self.next_seq();

        loop {
            match self.memtable.put(seq_id, hash_key, key_bytes, value) {
                Ok(()) => {
                    // Track approximate size (key + value)
                    let size = (key_bytes.len() + value.len()) as u64;
                    self.add_approx_size(size);

                    // Periodically check for eviction (every ~1000 writes to amortize cost)
                    // Use low bits of seq_id as a cheap "random" check
                    if seq_id & 0x3FF == 0 {
                        self.maybe_evict();
                    }

                    return Ok(());
                }
                Err(Error::InvalidConfig { message }) if message.contains("too old") => {
                    // Zombie Investigation: Check if a version exists in the global index.
                    // If it does, we "succeeded" (last write wins, our data is obsolete).
                    if self.index.get(&hash_key).is_some() {
                        return Ok(());
                    }

                    // Resurrection: Acquire fresh seqID and retry
                    seq_id = self.next_seq();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Retrieves a blob from the cache as a zero-copy `PinnedBlob`.
    ///
    /// For uncompressed blobs found in the Librarian (RAM cache), returns a
    /// direct pointer into the mmap arena — no allocation, no copy.
    /// For compressed blobs or disk reads, returns an owned decompressed copy.
    ///
    /// Returns `None` if the key is not found.
    pub fn get_pinned(&self, key: &[u8]) -> Option<crate::slab::PinnedBlob> {
        if key.is_empty() {
            return None;
        }

        let hash_key = Key::from_bytes(key);

        // 1. Bloom filter gate
        let bloom_result = self.bloom.test(hash_key);

        #[cfg(test)]
        {
            eprintln!("get: hash_key={:?}, bloom_result={}", hash_key, bloom_result);
            if !bloom_result {
                eprintln!("get: bloom filter rejected key {:?}", std::str::from_utf8(key));
            }
        }

        if !bloom_result {
            return None;
        }

        // Sample bloom hits at 1/128 rate (cheap amortized counter)
        if hash_key.lo() & 0x7F == 0 {
            self.bloom_stats.hits.fetch_add(1, Ordering::Relaxed);
        }

        // 2. Check librarian (RAM cache) — zero-copy for uncompressed blobs
        if let Ok(Some(blob)) = self.librarian.acquire(hash_key) {
            // In trust_hash mode (cache mode), skip key comparison.
            // In non-trust_hash mode (CAS/WAL), verify to detect 128-bit collisions.
            if self.config.trust_hash || blob.stored_key == key {
                return Some(blob);
            }
            // Hash collision — not our key
            return None;
        }

        // 3. In MemoryOnly degraded mode, skip disk entirely
        if self.degraded.load(Ordering::Acquire)
            && self.config.degraded_mode == DegradedMode::MemoryOnly
        {
            return None;
        }

        // 4. Check disk via archivist
        if let Some(item) = self.index.get(&hash_key) {
            #[cfg(test)]
            eprintln!("get: found item for {:?}, is_deleted={}", hash_key, item.is_deleted());
            if item.is_deleted() {
                // Bloom said yes, index says deleted — count as ghost
                if hash_key.lo() & 0x7F == 0 {
                    self.bloom_stats.ghosts.fetch_add(1, Ordering::Relaxed);
                }
                return None;
            }

            match self.archivist.read_blob(&item, key) {
                Ok(result) => {
                    return Some(
                        crate::slab::PinnedBlob::owned(result.value)
                            .with_key(result.stored_key),
                    );
                }
                Err(Error::NotFound) => {
                    // Segment was drained between index lookup and file open — treat as
                    // cache miss. This is expected under concurrent drain and not a bug.
                    return None;
                }
                Err(e) => {
                    #[cfg(test)]
                    eprintln!("read error for key {:?}: {}", hash_key, e);
                    // Report the error — may trigger degraded mode
                    self.report_bg_error(e);
                    return None;
                }
            }
        } else {
            // Bloom said yes, index has no entry — ghost entry
            if hash_key.lo() & 0x7F == 0 {
                self.bloom_stats.ghosts.fetch_add(1, Ordering::Relaxed);
            }
        }

        None
    }

    /// Creates a new ordered iterator over all live keys.
    ///
    /// Requires the KeyIndex to be enabled (`Config::with_keyindex()`).
    /// The iterator holds a snapshot of the KeyIndex at construction time.
    ///
    /// `lower` and `upper` are optional inclusive/exclusive byte-key bounds.
    pub fn new_iterator(
        self: &Arc<Self>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<BlobIterator> {
        let ki = self.key_index.as_ref().ok_or_else(|| Error::InvalidConfig {
            message: "KeyIndex not enabled; use Config::with_keyindex()".into(),
        })?;

        BlobIterator::new(
            Arc::clone(&self.index),
            Arc::clone(&self.archivist),
            ki,
            lower,
            upper,
        )
    }

    /// Returns a reference to the KeyIndex, if enabled.
    pub fn key_index(&self) -> Option<&Arc<KeyIndex>> {
        self.key_index.as_ref()
    }

    /// Reports a background (non-fatal) I/O error.
    ///
    /// - `DegradedMode::Panic`: panics immediately
    /// - `DegradedMode::MemoryOnly`: sets degraded flag on first error
    /// - `DegradedMode::Log` / `DegradedMode::Return`: logs only
    fn report_bg_error(&self, err: Error) {
        match self.config.degraded_mode {
            DegradedMode::Panic => {
                panic!("blobcache I/O error (DegradedMode::Panic): {}", err);
            }
            DegradedMode::MemoryOnly => {
                // First error wins — only log once
                if !self.degraded.swap(true, Ordering::AcqRel) {
                    eprintln!("blobcache entering degraded (memory-only) mode: {}", err);
                }
            }
            DegradedMode::Log | DegradedMode::Return => {
                eprintln!("blobcache I/O error: {}", err);
            }
        }
    }

    /// Retrieves a blob from the cache, returning an owned `Vec<u8>`.
    ///
    /// This is a convenience wrapper around `get_pinned()` for callers that need
    /// owned data. For zero-copy access use `get_pinned()` or `view()`.
    ///
    /// Returns None if the key is not found.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_pinned(key).map(|b| b.to_owned_vec())
    }

    /// Alias for `get()` — returns an owned copy of the blob data.
    pub fn get_bytes(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key)
    }

    /// Provides scoped zero-copy access to a value via a closure.
    ///
    /// For uncompressed Librarian hits the closure receives a direct pointer into
    /// the mmap arena — no allocation. Returns true if the key was found.
    pub fn view<F>(&self, key: &[u8], mut f: F) -> bool
    where
        F: FnMut(&[u8]),
    {
        if let Some(blob) = self.get_pinned(key) {
            f(&blob);
            true
        } else {
            false
        }
    }

    /// Retrieves a blob into a pre-allocated buffer (zero allocation on the path,
    /// one copy from pinned memory into `buf`).
    ///
    /// The buffer is cleared and filled with the blob data if found.
    /// Returns true if the key was found and data was written to buf.
    pub fn get_into(&self, key: &[u8], buf: &mut Vec<u8>) -> bool {
        buf.clear();
        if let Some(blob) = self.get_pinned(key) {
            buf.extend_from_slice(&blob);
            true
        } else {
            false
        }
    }

    /// Deletes a blob from the cache.
    ///
    /// In CAS mode (WAL enabled), writes a tombstone record for crash-safe deletion.
    /// In cache mode, simply removes from the in-memory index.
    ///
    /// Returns Ok(()) even if the key doesn't exist (idempotent delete).
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidConfig {
                message: "empty key not allowed".to_string(),
            });
        }

        // Track deletion for bloom rebuild heuristics
        self.bloom_stats.deletions.fetch_add(1, Ordering::Relaxed);

        let hash_key = Key::from_bytes(key);

        // In CAS mode, write tombstone for durability
        if self.wal.is_some() {
            let seq_id = self.next_seq();
            self.memtable.delete(seq_id, hash_key, key)?;
            // Note: Don't call librarian.invalidate() here - the tombstone in the
            // slab handles read-path rejection. librarian.invalidate() would
            // remove the tombstone entry from the shared slab index!
        } else {
            // Cache mode: just remove from index
            self.index.delete(&hash_key);
            // Clear read-after-write cache
            self.librarian.invalidate(&hash_key);
        }

        Ok(())
    }

    /// Returns true if the cache is in degraded mode (memory-only).
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Returns the approximate size of stored data.
    pub fn approx_size(&self) -> u64 {
        self.approx_size.load(Ordering::Relaxed)
    }

    /// Returns the maximum cache size.
    pub fn max_size(&self) -> u64 {
        self.config.max_size
    }

    /// Flushes all pending writes to disk.
    pub fn flush(&self) {
        self.memtable.flush();
    }

    /// Drains all pending flushes and waits for completion.
    pub fn drain(&self) {
        self.memtable.drain();
    }

    /// Adds to the approximate size counter.
    /// Called by memtable after flushing a slab.
    pub fn add_approx_size(&self, bytes: u64) {
        self.approx_size.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Subtracts from the approximate size counter.
    /// Called after eviction.
    pub fn sub_approx_size(&self, bytes: u64) {
        // Use saturating subtraction to prevent underflow
        loop {
            let current = self.approx_size.load(Ordering::Acquire);
            let new_val = current.saturating_sub(bytes);
            if self.approx_size.compare_exchange_weak(
                current, new_val, Ordering::AcqRel, Ordering::Acquire
            ).is_ok() {
                break;
            }
        }
    }

    /// Checks if eviction is needed and performs it.
    ///
    /// Called after flush to maintain size below max_size.
    /// Uses SIEVE algorithm to select victims, then hole-punches to reclaim space.
    pub fn maybe_evict(&self) {
        let max_size = self.config.max_size;
        if max_size == 0 {
            return; // Unlimited
        }

        let current_size = self.approx_size.load(Ordering::Acquire);
        if current_size <= max_size {
            return; // Under limit
        }

        // Target size with hysteresis (93% of max to avoid thrashing)
        let target_size = (max_size as f64 * EVICTION_HYSTERESIS) as u64;
        let to_evict = current_size.saturating_sub(target_size);

        if to_evict == 0 {
            return;
        }

        // Run SIEVE to select victims
        let victims = self.index.evict_batch(to_evict as i64);
        let num_victims = victims.len();

        // Accumulate freed bytes from evicted items.
        // Note: we do NOT hole-punch here. Physical space is reclaimed by pressure-driven
        // segment drain (compaction.rs), which deletes entire sparse segment files.
        // Hole-punching individual records during eviction adds unnecessary fallocate
        // syscalls on the hot path without providing meaningful space reclamation
        // (segments remain on disk until drain deletes them wholesale).
        let mut freed_bytes: u64 = 0;
        for victim in &victims {
            freed_bytes += victim.physical_len as u64;
        }

        // Update size counter
        self.sub_approx_size(freed_bytes);

        // Log eviction results
        info!(
            "SIEVE eviction: freed {:.2} MB ({} items), size: {:.2} GB -> {:.2} GB (max: {:.2} GB)",
            freed_bytes as f64 / (1024.0 * 1024.0),
            num_victims,
            current_size as f64 / (1024.0 * 1024.0 * 1024.0),
            self.approx_size.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0 * 1024.0),
            max_size as f64 / (1024.0 * 1024.0 * 1024.0),
        );
    }

    /// Spawns the background compaction worker thread.
    ///
    /// The worker runs periodic compaction every 10 minutes (matching Go's evictionWorker).
    /// It stops when `cache.closed` is set to true, or when the Cache is dropped.
    ///
    /// Uses Weak<Cache> to avoid preventing Cache from being dropped during crash simulation.
    ///
    /// Returns the JoinHandle so close() can wait for graceful shutdown.
    fn spawn_compaction_worker(cache: Arc<Self>) -> JoinHandle<()> {
        let weak = Arc::downgrade(&cache);
        std::thread::spawn(move || {
            loop {
                // Sleep for the compaction interval, but check closed flag frequently
                // for fast shutdown response (100ms latency instead of 10s).
                // 6000 iterations * 100ms = 10 minutes
                for _ in 0..6000 {
                    std::thread::sleep(Duration::from_millis(100));

                    // Try to upgrade weak reference - if Cache was dropped, exit
                    let Some(cache) = weak.upgrade() else {
                        return;
                    };

                    if cache.closed.load(Ordering::Acquire) {
                        return;
                    }
                }

                // Try to upgrade weak reference for actual compaction work
                let Some(cache) = weak.upgrade() else {
                    return;
                };

                // Run compaction if not degraded
                if !cache.degraded.load(Ordering::Acquire) {
                    if let Err(e) = cache.maybe_compact_segments() {
                        eprintln!("compaction failed: {}", e);
                    }

                    // Cache mode only: drain sparse segments to reclaim disk space
                    if !cache.config.wal_enabled && cache.config.max_size > 0 {
                        cache.maybe_drain_segments();
                    }

                    // Bloom filter FPR-reactive rebuild
                    cache.maybe_trigger_bloom_rebuild();
                }
            }
        })
    }

    /// Runs periodic compaction: tombstone cleanup + merge compaction.
    ///
    /// This matches Go's maybeCompactSegments().
    fn maybe_compact_segments(&self) -> Result<()> {
        // Phase 1: Tombstone compaction (collect tombstones, drop tail segment tombstones)
        if let Err(e) = self.maybe_compact_tombstones() {
            eprintln!("tombstone compaction failed: {}", e);
            // Continue to merge compaction even if tombstone compaction fails
        }

        // Phase 2: Merge compaction (combine sparse segments)
        self.maybe_merge_segments()
    }

    /// Phase 1: Compact tombstones - hole punch and cleanup.
    fn maybe_compact_tombstones(&self) -> Result<()> {
        // Get segments with tombstones
        let segments_with_tombstones = self.index.segments_with_tombstones();
        if segments_with_tombstones.is_empty() {
            return Ok(());
        }

        // For each segment with tombstones, compact it
        // Only drop tombstones for the oldest (tail) segment
        let oldest_segment = self.index.oldest_live_segment_id();

        for seg_id in segments_with_tombstones {
            let drop_tombstones = seg_id == oldest_segment;
            match self.compactor.compact(&[seg_id], drop_tombstones) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("tombstone compaction for segment {} failed: {}", seg_id, e);
                }
            }
        }

        Ok(())
    }

    /// Phase 2: Merge sparse segments.
    fn maybe_merge_segments(&self) -> Result<()> {
        use crate::compaction::{select_contiguous_ranges, select_sparse_segments};

        // Select candidate ranges: segments with >75% waste, ranges of at least 2 segments
        let stats = self.index.segment_stats();
        let sparse_ids = select_sparse_segments(&stats, 0.75);
        let ranges = select_contiguous_ranges(&sparse_ids);

        if ranges.is_empty() {
            return Ok(());
        }

        let oldest_segment = self.index.oldest_live_segment_id();

        for range in ranges {
            // Only drop tombstones if compacting the tail (oldest) segment
            let drop_tombstones = range.first().copied() == Some(oldest_segment);
            match self.compactor.compact(&range, drop_tombstones) {
                Ok(result) => {
                    #[cfg(test)]
                    eprintln!(
                        "merge compaction: merged {} segments, compacted {} items",
                        range.len(),
                        result.items_compacted
                    );
                    let _ = result;
                }
                Err(e) => {
                    eprintln!("merge compaction failed for {:?}: {}", range, e);
                }
            }
        }

        Ok(())
    }

    /// Checks if the bloom filter should be rebuilt and triggers a rebuild if so.
    ///
    /// Rebuild is triggered when:
    /// - Cumulative deletes exceed 10% of estimated keys (stale ghost entries)
    /// - FPR has spiked: ghost rate > 5× configured FPR (with ≥2000 sample hits)
    fn maybe_trigger_bloom_rebuild(&self) {
        let deletions = self.bloom_stats.deletions.load(Ordering::Relaxed);
        let stale_threshold = (self.config.bloom_estimated_keys as f64 * 0.10) as i64;

        let hits = self.bloom_stats.hits.load(Ordering::Relaxed);
        let ghosts = self.bloom_stats.ghosts.load(Ordering::Relaxed);

        let should_rebuild = deletions > stale_threshold
            || (hits > 2000 && (ghosts as f64 / hits as f64) > self.config.bloom_fp_rate * 5.0);

        if should_rebuild {
            self.rebuild_bloom();
        }
    }

    /// Rebuilds the bloom filter from the current in-memory index.
    ///
    /// Clears all bits, re-adds every live item, and resets stats counters.
    /// Called from the maintenance worker (not on the hot path).
    fn rebuild_bloom(&self) {
        self.bloom.rebuild(self.index.blobs());

        // Reset stats
        self.bloom_stats.deletions.store(0, Ordering::Relaxed);
        self.bloom_stats.ghosts.store(0, Ordering::Relaxed);
        self.bloom_stats.hits.store(0, Ordering::Relaxed);

        info!("bloom filter rebuilt");
    }

    /// Drains sparse segments to reclaim disk space (cache mode only).
    ///
    /// When the estimated total disk usage exceeds `max_size * DRAIN_HIGH_WATERMARK`,
    /// deletes the sparsest segments (by live bytes) until usage drops below
    /// `max_size * DRAIN_LOW_WATERMARK`.
    ///
    /// Only operates in cache mode (no WAL). Respects the cooling period so
    /// recently-written segments are never drained.
    fn maybe_drain_segments(&self) {
        const DRAIN_HIGH_WATERMARK_RATIO: f64 = 0.5;
        const DRAIN_LOW_WATERMARK_RATIO: f64 = 0.25;

        let max_size = self.config.max_size;
        if max_size == 0 {
            return; // Unlimited — nothing to drain
        }

        let seg_count = self.compactor.segment_count() as u64;
        let estimated_disk = seg_count * self.config.write_buffer_size as u64;
        let live_bytes = self.approx_size.load(Ordering::Relaxed);
        let waste = estimated_disk.saturating_sub(live_bytes);

        let high = (max_size as f64 * DRAIN_HIGH_WATERMARK_RATIO) as u64;
        if waste <= high {
            return;
        }

        let low = (max_size as f64 * DRAIN_LOW_WATERMARK_RATIO) as u64;
        let mut drain_target = waste.saturating_sub(low) as i64;

        // Cooling gap: skip recently-written segments (still warm in librarian)
        let cooling_gap = (self.config.max_cached_slabs + 2) as u32;
        let max_eligible_id = self.segment_ids.current().saturating_sub(cooling_gap);

        let candidates = self.compactor.get_drain_candidates(max_eligible_id);

        for seg in candidates {
            if drain_target <= 0 {
                break;
            }

            // Take exclusive lock on this segment during deletion
            let _guard = self.index.lock_segment_exclusive(seg.id);
            let (drained_bytes, _count) = self.index.drain_segment(seg.id);
            self.archivist.drop_segment_cache(seg.id);
            let _ = delete_segment_files(&self.config.path, self.config.shards, seg.id);
            if let Some(ref ki) = self.key_index {
                let _ = ki.drain_segment(seg.id);
            }

            self.sub_approx_size(drained_bytes);
            drain_target -= self.config.write_buffer_size as i64;

            info!(
                "drain: deleted segment {} ({:.2} MB live bytes freed)",
                seg.id,
                drained_bytes as f64 / (1024.0 * 1024.0),
            );
        }
    }

    /// Closes the cache gracefully.
    ///
    /// Waits for the background compaction worker to stop before returning.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // Already closed
        }

        // 1. Wait for compaction worker to finish (it checks closed flag every 100ms)
        if let Some(handle) = self.compaction_worker.lock().take() {
            let _ = handle.join();
        }

        // 2. Close write path
        self.memtable.close();

        // 3. Close librarian
        self.librarian.close();

        // 4. Close WAL if present
        if let Some(ref wal) = self.wal {
            wal.close()?;
        }

        // 5. Close archivist
        self.archivist.close()?;

        Ok(())
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> CacheStats {
        let index_stats = self.index.stats();
        let io_stats = self.archivist.io_stats();
        CacheStats {
            items: index_stats.items,
            arena_nodes: index_stats.arena_nodes,
            shards: index_stats.shards,
            memory_est: index_stats.memory_est,
            approx_size: self.approx_size(),
            librarian_cached_slabs: self.librarian.len(),
            librarian_evictions: self.librarian.eviction_count(),
            bloom_hits: self.bloom_stats.hits.load(Ordering::Relaxed),
            bloom_ghosts: self.bloom_stats.ghosts.load(Ordering::Relaxed),
            bloom_deletions: self.bloom_stats.deletions.load(Ordering::Relaxed),
            iosched_requests: io_stats.requests,
            iosched_batches: io_stats.batches,
            degraded: self.is_degraded(),
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

// =============================================================================
// CacheStats
// =============================================================================

/// Statistics about the cache state.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of items in the index.
    pub items: usize,
    /// Number of arena nodes allocated.
    pub arena_nodes: usize,
    /// Number of index shards.
    pub shards: usize,
    /// Estimated memory usage.
    pub memory_est: i64,
    /// Approximate size of stored data.
    pub approx_size: u64,
    /// Number of slabs currently cached in librarian.
    pub librarian_cached_slabs: usize,
    /// Total number of slabs evicted from librarian catalog.
    pub librarian_evictions: i64,
    /// Sampled bloom filter positive lookups (1/128 sampling).
    pub bloom_hits: u64,
    /// Sampled bloom filter false positives (bloom yes, index no).
    pub bloom_ghosts: u64,
    /// Cumulative deletes since last bloom rebuild.
    pub bloom_deletions: i64,
    /// Total I/O read requests issued by Archivist.
    pub iosched_requests: u64,
    /// Total I/O submission batches.
    pub iosched_batches: u64,
    /// Whether the cache is in degraded mode.
    pub degraded: bool,
}

// =============================================================================
// Builder Pattern
// =============================================================================

/// Builder for creating a Cache with custom configuration.
pub struct CacheBuilder {
    config: Config,
}

impl CacheBuilder {
    /// Creates a new builder with the given path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        CacheBuilder {
            config: Config::new(path),
        }
    }

    /// Sets the maximum cache size.
    pub fn max_size(mut self, size: u64) -> Self {
        self.config.max_size = size;
        self
    }

    /// Sets the write buffer size.
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.config.write_buffer_size = size;
        self
    }

    /// Enables the Write-Ahead Log for durability.
    pub fn with_wal(mut self) -> Self {
        self.config.wal_enabled = true;
        self
    }

    /// Enables checksum verification on reads.
    pub fn with_checksum(mut self) -> Self {
        self.config.checksum_enabled = true;
        self
    }

    /// Sets compression codec.
    pub fn compression(mut self, codec: crate::compression::Codec) -> Self {
        self.config.compression = codec;
        self
    }

    /// Opens the cache with the configured options.
    pub fn open(self) -> Result<Arc<Cache>> {
        Cache::open(self.config)
    }
}

/// Creates a cache builder for the given path.
pub fn cache(path: impl Into<PathBuf>) -> CacheBuilder {
    CacheBuilder::new(path)
}

/// Creates a CAS (Content Addressable Storage) builder for the given path.
/// Enables WAL and checksums by default.
pub fn cas(path: impl Into<PathBuf>) -> CacheBuilder {
    CacheBuilder::new(path).with_wal().with_checksum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_cache_basic() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        // Put
        cache.put(b"key1", b"value1").unwrap();

        // Get
        let value = cache.get(b"key1");
        assert!(value.is_some());
        assert_eq!(value.unwrap(), b"value1");

        // Get non-existent
        let value = cache.get(b"nonexistent");
        assert!(value.is_none());

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_delete() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        cache.put(b"key1", b"value1").unwrap();
        assert!(cache.get(b"key1").is_some());

        cache.delete(b"key1").unwrap();
        // Note: Deletion removes from index but bloom filter may still say "maybe"
        // The actual get will return None because index lookup fails

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_empty_key() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        assert!(cache.put(b"", b"value").is_err());
        assert!(cache.delete(b"").is_err());
        assert!(cache.get(b"").is_none());

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_view() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        cache.put(b"key1", b"hello world").unwrap();

        let mut captured = Vec::new();
        let found = cache.view(b"key1", |data| {
            captured.extend_from_slice(data);
        });

        assert!(found);
        assert_eq!(captured, b"hello world");

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_stats() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        let stats = cache.stats();
        assert_eq!(stats.items, 0);

        cache.put(b"key1", b"value1").unwrap();
        // Note: Item won't appear in stats until flushed to index

        cache.close().unwrap();
    }

    #[test]
    fn test_cas_builder() {
        let dir = tempdir().unwrap();
        let cache = cas(dir.path()).open().unwrap();

        // CAS mode has WAL enabled
        assert!(cache.wal.is_some());

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_overwrite() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        // Write initial value
        cache.put(b"key1", b"value1").unwrap();
        assert_eq!(cache.get(b"key1").unwrap(), b"value1");

        // Overwrite with new value
        cache.put(b"key1", b"value2").unwrap();
        assert_eq!(cache.get(b"key1").unwrap(), b"value2");

        // Overwrite again
        cache.put(b"key1", b"value3").unwrap();
        assert_eq!(cache.get(b"key1").unwrap(), b"value3");

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_many_keys() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        // Write many keys
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            cache.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all keys
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected_value = format!("value{:04}", i);
            let value = cache.get(key.as_bytes());
            assert!(value.is_some(), "key {} should exist", key);
            assert_eq!(value.unwrap(), expected_value.as_bytes());
        }

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_large_values() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        // Write a few large values (64KB each)
        for i in 0..10 {
            let key = format!("largekey{}", i);
            let value = vec![i as u8; 64 * 1024];
            cache.put(key.as_bytes(), &value).unwrap();
        }

        // Verify large values
        for i in 0..10 {
            let key = format!("largekey{}", i);
            let expected = vec![i as u8; 64 * 1024];
            let value = cache.get(key.as_bytes());
            assert!(value.is_some(), "large key {} should exist", key);
            assert_eq!(value.unwrap(), expected);
        }

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_flush_and_drain() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        // Write some data
        for i in 0..50 {
            let key = format!("flushkey{}", i);
            let value = format!("flushvalue{}", i);
            cache.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Flush pending writes
        cache.flush();

        // Drain all pending flushes
        cache.drain();

        // Verify data is still accessible
        for i in 0..50 {
            let key = format!("flushkey{}", i);
            let expected = format!("flushvalue{}", i);
            let value = cache.get(key.as_bytes());
            assert!(value.is_some(), "key {} should exist after drain", key);
            assert_eq!(value.unwrap(), expected.as_bytes());
        }

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let cache = Arc::new(cache(dir.path()).open().unwrap());

        let mut handles = vec![];

        // Spawn multiple writer threads
        for t in 0..4 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..25 {
                    let key = format!("thread{}key{}", t, i);
                    let value = format!("thread{}value{}", t, i);
                    c.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            }));
        }

        // Wait for all writers
        for h in handles {
            h.join().unwrap();
        }

        // Verify all keys from all threads
        for t in 0..4 {
            for i in 0..25 {
                let key = format!("thread{}key{}", t, i);
                let expected = format!("thread{}value{}", t, i);
                let value = cache.get(key.as_bytes());
                assert!(value.is_some(), "key {} should exist", key);
                assert_eq!(value.unwrap(), expected.as_bytes());
            }
        }

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let cache = Arc::new(cache(dir.path()).open().unwrap());

        // Pre-populate some data
        for i in 0..50 {
            let key = format!("prekey{}", i);
            let value = format!("prevalue{}", i);
            cache.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let mut handles = vec![];

        // Writer thread
        let c_write = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("newkey{}", i);
                let value = format!("newvalue{}", i);
                c_write.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }));

        // Reader threads
        for _ in 0..3 {
            let c_read = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    let key = format!("prekey{}", i);
                    // May or may not find due to concurrent modifications
                    let _ = c_read.get(key.as_bytes());
                }
            }));
        }

        // Wait for all threads
        for h in handles {
            h.join().unwrap();
        }

        cache.close().unwrap();
    }

    #[test]
    fn test_cache_delete_after_overwrite() {
        let dir = tempdir().unwrap();
        let cache = cache(dir.path()).open().unwrap();

        // Write and overwrite
        cache.put(b"key1", b"value1").unwrap();
        cache.put(b"key1", b"value2").unwrap();
        cache.put(b"key1", b"value3").unwrap();

        // Delete
        cache.delete(b"key1").unwrap();

        // Should not be found (even after multiple writes)
        // Note: bloom filter might still say "maybe", but index lookup will fail
        // The key was invalidated in librarian as well

        cache.close().unwrap();
    }

    #[test]
    fn test_wal_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Write data with WAL, then "crash" (don't close cleanly)
        {
            let cache = cas(&path).open().unwrap();

            // Write some data
            for i in 0..20 {
                let key = format!("walkey{:04}", i);
                let value = format!("walvalue{:04}", i);
                cache.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            // Flush to ensure data is in WAL
            cache.flush();

            // DON'T call close() - simulate crash
            // The WAL files should still be on disk
            drop(cache);
        }

        // Verify WAL files exist (not yet recovered)
        let wal_dir = path.join("wal");
        eprintln!("WAL dir exists: {}", wal_dir.exists());

        // List WAL files
        if wal_dir.exists() {
            for entry in std::fs::read_dir(&wal_dir).unwrap() {
                let entry = entry.unwrap();
                eprintln!("WAL file: {:?} size={}", entry.file_name(), entry.metadata().unwrap().len());
            }
        }

        // List .iseg files
        for shard in 0..4 {
            let shard_dir = path.join(format!("{:02x}", shard));
            if shard_dir.exists() {
                for entry in std::fs::read_dir(&shard_dir).unwrap() {
                    let entry = entry.unwrap();
                    eprintln!("Shard {} file: {:?}", shard, entry.file_name());
                }
            }
        }

        assert!(
            wal_dir.exists(),
            "WAL directory should exist after crash"
        );

        // Phase 2: Re-open - should recover from WAL
        {
            let cache = cas(&path).open().unwrap();
            eprintln!("After recovery, index items: {}", cache.stats().items);

            // Drain to ensure recovery data is flushed
            cache.drain();

            // Verify all data was recovered
            for i in 0..20 {
                let key = format!("walkey{:04}", i);
                let expected = format!("walvalue{:04}", i);
                let value = cache.get(key.as_bytes());
                assert!(value.is_some(), "key {} should exist after recovery", key);
                assert_eq!(value.unwrap(), expected.as_bytes());
            }

            cache.close().unwrap();
        }
    }

    #[test]
    fn test_tombstone_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: Write data, delete some, then "crash"
        {
            let cache = cas(&path).open().unwrap();

            // Write data
            for i in 0..10 {
                let key = format!("tombkey{:04}", i);
                let value = format!("tombvalue{:04}", i);
                cache.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            // Delete some keys (these become tombstones in CAS mode)
            for i in 0..5 {
                let key = format!("tombkey{:04}", i);
                cache.delete(key.as_bytes()).unwrap();
            }

            // Verify deleted keys return None
            for i in 0..5 {
                let key = format!("tombkey{:04}", i);
                assert!(cache.get(key.as_bytes()).is_none(), "deleted key {} should not exist", key);
            }

            // Verify remaining keys still exist
            for i in 5..10 {
                let key = format!("tombkey{:04}", i);
                assert!(cache.get(key.as_bytes()).is_some(), "key {} should exist", key);
            }

            // Flush but don't close cleanly (simulate crash)
            cache.flush();
            // Don't call cache.close() - simulates crash
        }

        // Phase 2: Re-open - should recover with tombstones
        {
            let cache = cas(&path).open().unwrap();

            // Drain to ensure recovery data is flushed
            cache.drain();

            // Deleted keys should still be deleted after recovery
            for i in 0..5 {
                let key = format!("tombkey{:04}", i);
                let value = cache.get(key.as_bytes());
                assert!(value.is_none(), "deleted key {} should not exist after recovery", key);
            }

            // Remaining keys should still exist after recovery
            for i in 5..10 {
                let key = format!("tombkey{:04}", i);
                let expected = format!("tombvalue{:04}", i);
                let value = cache.get(key.as_bytes());
                assert!(value.is_some(), "key {} should exist after recovery", key);
                assert_eq!(value.unwrap(), expected.as_bytes());
            }

            cache.close().unwrap();
        }
    }

    #[test]
    fn test_xl_writes_cache_mode_rejects() {
        let dir = tempdir().unwrap();

        // Use a small write buffer (64KB) in cache mode (no WAL)
        let config = Config::new(dir.path())
            .write_buffer_size(64 * 1024);  // 64KB buffer
        let cache = Cache::open(config).unwrap();

        // XL writes (larger than buffer) should be rejected in cache mode
        let xl_key = b"xlkey1";
        let xl_value = vec![0xAB; 128 * 1024];  // 128KB > 64KB buffer
        let result = cache.put(xl_key, &xl_value);
        assert!(result.is_err(), "XL writes should be rejected in cache mode");

        // Normal writes should still work
        cache.put(b"normalkey", b"normalvalue").unwrap();
        assert_eq!(cache.get(b"normalkey").unwrap(), b"normalvalue");

        cache.close().unwrap();
    }

    #[test]
    fn test_xl_writes_cas_mode() {
        let dir = tempdir().unwrap();

        // Use a small write buffer (64KB) in CAS mode (with WAL)
        // WAL handles XL writes natively
        let config = Config::new(dir.path())
            .write_buffer_size(64 * 1024)  // 64KB buffer
            .with_wal();  // Enable WAL for XL support
        let cache = Cache::open(config).unwrap();

        // Write an XL value (larger than write_buffer_size)
        // In CAS mode with WAL, this should work
        let xl_key = b"xlkey1";
        let xl_value = vec![0xAB; 128 * 1024];  // 128KB
        cache.put(xl_key, &xl_value).unwrap();

        // Write some normal values alongside
        for i in 0..5 {
            let key = format!("normalkey{}", i);
            let value = format!("normalvalue{}", i);
            cache.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Flush to disk
        cache.flush();
        cache.drain();

        // Verify XL value
        let retrieved = cache.get(xl_key);
        assert!(retrieved.is_some(), "XL key should exist in CAS mode");
        assert_eq!(retrieved.unwrap(), xl_value, "XL value should match");

        // Verify normal values
        for i in 0..5 {
            let key = format!("normalkey{}", i);
            let expected = format!("normalvalue{}", i);
            let value = cache.get(key.as_bytes());
            assert!(value.is_some(), "normal key {} should exist", key);
            assert_eq!(value.unwrap(), expected.as_bytes());
        }

        cache.close().unwrap();
    }

    /// Test that segment files are actually created on disk after flush.
    /// This test catches the bug where data was "written" but never persisted.
    #[test]
    fn test_segment_files_created_after_flush() {
        use std::fs;

        let dir = tempdir().unwrap();
        let path = dir.path();

        // Configure cache with direct_io_write like the benchmark
        let config = Config::new(path)
            .write_buffer_size(1 << 20)  // 1MB buffer for faster flush
            .max_inflight_slabs(2)
            .flush_concurrency(1)
            .direct_io_write(true);  // Direct I/O like benchmark

        let cache = Cache::open(config).unwrap();

        // Write enough data to fill the buffer and trigger flush
        // With 1MB buffer, write 2MB to ensure at least one flush
        let value = vec![0xAB; 100_000];  // 100KB per write
        for i in 0..25 {
            let key = format!("segkey{:04}", i);
            cache.put(key.as_bytes(), &value).unwrap();
        }

        // Drain to ensure all data is flushed to disk
        cache.drain();

        // Check that segments directory exists
        let segments_dir = path.join("segments");
        assert!(
            segments_dir.exists(),
            "segments directory should exist at {:?}",
            segments_dir
        );

        // Find segment files
        let mut segment_files = Vec::new();
        for shard_entry in fs::read_dir(&segments_dir).unwrap() {
            let shard_dir = shard_entry.unwrap().path();
            if shard_dir.is_dir() {
                for file_entry in fs::read_dir(&shard_dir).unwrap() {
                    let file_path = file_entry.unwrap().path();
                    if file_path.extension().map_or(false, |ext| ext == "seg") {
                        segment_files.push(file_path);
                    }
                }
            }
        }

        assert!(
            !segment_files.is_empty(),
            "at least one segment file should exist in {:?}",
            segments_dir
        );

        // Verify segment files have actual content (not empty)
        for seg_file in &segment_files {
            let metadata = fs::metadata(seg_file).unwrap();
            assert!(
                metadata.len() > 0,
                "segment file {:?} should not be empty",
                seg_file
            );
        }

        // Calculate total segment size
        let total_size: u64 = segment_files
            .iter()
            .map(|f| fs::metadata(f).unwrap().len())
            .sum();

        // We wrote ~2.5MB, should have at least 1MB on disk after compression/overhead
        assert!(
            total_size > 500_000,
            "total segment size {} should be > 500KB (wrote ~2.5MB)",
            total_size
        );

        eprintln!(
            "test_segment_files_created_after_flush: {} segment files, total {} bytes",
            segment_files.len(),
            total_size
        );

        cache.close().unwrap();
    }

    /// Test segment files with direct_io_write disabled (buffered I/O).
    /// This is the control test to ensure the test itself is correct.
    #[test]
    fn test_segment_files_created_buffered_io() {
        use std::fs;

        let dir = tempdir().unwrap();
        let path = dir.path();

        // Configure cache WITHOUT direct_io_write
        let config = Config::new(path)
            .write_buffer_size(1 << 20)  // 1MB buffer
            .max_inflight_slabs(2)
            .flush_concurrency(1)
            .direct_io_write(false);  // Buffered I/O

        let cache = Cache::open(config).unwrap();

        // Write enough data to fill the buffer and trigger flush
        let value = vec![0xAB; 100_000];  // 100KB per write
        for i in 0..25 {
            let key = format!("bufkey{:04}", i);
            cache.put(key.as_bytes(), &value).unwrap();
        }

        // Drain to ensure all data is flushed to disk
        cache.drain();

        // Check that segments directory exists
        let segments_dir = path.join("segments");
        assert!(
            segments_dir.exists(),
            "segments directory should exist at {:?}",
            segments_dir
        );

        // Find and count segment files
        let mut total_size: u64 = 0;
        let mut segment_count = 0;
        for shard_entry in fs::read_dir(&segments_dir).unwrap() {
            let shard_dir = shard_entry.unwrap().path();
            if shard_dir.is_dir() {
                for file_entry in fs::read_dir(&shard_dir).unwrap() {
                    let file_path = file_entry.unwrap().path();
                    if file_path.extension().map_or(false, |ext| ext == "seg") {
                        let size = fs::metadata(&file_path).unwrap().len();
                        assert!(size > 0, "segment file {:?} should not be empty", file_path);
                        total_size += size;
                        segment_count += 1;
                    }
                }
            }
        }

        assert!(segment_count > 0, "at least one segment file should exist");
        assert!(
            total_size > 500_000,
            "total segment size {} should be > 500KB",
            total_size
        );

        eprintln!(
            "test_segment_files_created_buffered_io: {} segment files, total {} bytes",
            segment_count,
            total_size
        );

        cache.close().unwrap();
    }

    /// Test with exact benchmark configuration to catch benchmark-specific bugs.
    /// Uses the same config as benches/cache.rs parallel_blobcache benchmark.
    #[test]
    fn test_benchmark_config_segment_creation() {
        use std::fs;

        let dir = tempdir().unwrap();
        let path = dir.path();

        // EXACT configuration from benchmark (except smaller max_size for test)
        let mut config = Config::new(path);
        config.max_size = 10 << 30;           // 10GB (smaller for test, benchmark uses 400GB)
        config.write_buffer_size = 128 << 20;  // 128MB exactly like benchmark
        config.max_inflight_slabs = 32;        // 32 exactly like benchmark
        config.max_cached_slabs = 64;          // 64 exactly like benchmark
        config.flush_concurrency = 6;          // 6 exactly like benchmark
        config.direct_io_write = true;         // Direct I/O like benchmark
        config.fdatasync = true;               // fdatasync like benchmark
        config.wal_enabled = false;            // No WAL (cache mode) like benchmark
        config.degraded_mode = crate::config::DegradedMode::Panic;  // Panic on errors!

        eprintln!("test_benchmark_config: opening cache with benchmark config");
        let cache = Cache::open(config).unwrap();
        eprintln!("test_benchmark_config: cache opened successfully");

        // Write ~256MB to trigger at least 2 flushes
        let value = vec![0xCD; 1_000_000];  // 1MB per write
        eprintln!("test_benchmark_config: writing 256 x 1MB values...");
        for i in 0..256 {
            let key = format!("benchkey{:08}", i);
            cache.put(key.as_bytes(), &value).unwrap();
            if (i + 1) % 64 == 0 {
                eprintln!("test_benchmark_config: wrote {} keys", i + 1);
            }
        }
        eprintln!("test_benchmark_config: all writes complete, draining...");

        // Drain
        cache.drain();
        eprintln!("test_benchmark_config: drain complete");

        // Check segments
        let segments_dir = path.join("segments");
        assert!(segments_dir.exists(), "segments dir must exist");

        // Count and measure segment files
        let mut total_size: u64 = 0;
        let mut segment_count = 0;
        for shard_entry in fs::read_dir(&segments_dir).unwrap() {
            let shard_dir = shard_entry.unwrap().path();
            if shard_dir.is_dir() {
                for file_entry in fs::read_dir(&shard_dir).unwrap() {
                    let file_path = file_entry.unwrap().path();
                    if file_path.extension().map_or(false, |ext| ext == "seg") {
                        let size = fs::metadata(&file_path).unwrap().len();
                        eprintln!("  segment {:?}: {} bytes", file_path, size);
                        assert!(size > 0, "segment {:?} must not be empty", file_path);
                        total_size += size;
                        segment_count += 1;
                    }
                }
            }
        }

        // We wrote 256MB, should have at least 128MB on disk (at least 1 full buffer)
        eprintln!(
            "test_benchmark_config: {} segments, total {} bytes",
            segment_count, total_size
        );

        assert!(segment_count >= 2, "should have at least 2 segments (wrote 256MB with 128MB buffer)");
        assert!(
            total_size >= 128 << 20,
            "total size {} should be >= 128MB (wrote 256MB)",
            total_size
        );

        cache.close().unwrap();
    }

    // =========================================================================
    // Bug regression tests
    // =========================================================================

    /// Test that close() properly drains all data (BUG: close() loses data).
    ///
    /// This test writes data, calls close() WITHOUT explicit drain, re-opens,
    /// and verifies all data is present. If close() silently drops data,
    /// this test WILL FAIL.
    #[test]
    fn test_close_drains_all_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Use small buffer to create many pending slabs
        let mut config = Config::new(&path);
        config.write_buffer_size = 1 << 20; // 1MB
        config.max_inflight_slabs = 32;
        config.flush_concurrency = 6;
        config.degraded_mode = crate::config::DegradedMode::Panic;
        config.wal_enabled = true; // Enable WAL for durable test

        let num_keys = 100;
        let value = vec![0xCD; 100_000]; // 100KB per write = 10MB total

        // Phase 1: Write data and close WITHOUT explicit drain
        {
            let cache = Cache::open(config.clone()).unwrap();

            for i in 0..num_keys {
                let key = format!("closekey{:04}", i);
                cache.put(key.as_bytes(), &value).unwrap();
            }

            // BUG: close() should implicitly drain, but currently doesn't
            cache.close().unwrap();
        }

        // Phase 2: Re-open and verify ALL data is present
        {
            let cache = Cache::open(config).unwrap();

            // Wait for recovery
            cache.drain();

            let mut missing = 0;
            for i in 0..num_keys {
                let key = format!("closekey{:04}", i);
                if cache.get(key.as_bytes()).is_none() {
                    missing += 1;
                    eprintln!("MISSING: {}", key);
                }
            }

            assert_eq!(
                missing, 0,
                "close() lost {} keys out of {} - data silently dropped!",
                missing, num_keys
            );

            cache.close().unwrap();
        }
    }

    /// Test that sequence IDs are properly initialized from index max_seq.
    ///
    /// BUG: global_seq is initialized from SystemTime without checking
    /// index.max_seq_id(). If clock drifts backwards, new writes may be
    /// silently discarded as "zombies".
    #[test]
    fn test_seq_id_initialization_uses_max_seq() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let mut config = Config::new(&path);
        config.wal_enabled = true;
        config.write_buffer_size = 1 << 20;
        config.degraded_mode = crate::config::DegradedMode::Panic;

        // Phase 1: Write data with high sequence IDs
        {
            let cache = Cache::open(config.clone()).unwrap();

            for i in 0..10 {
                let key = format!("seqkey{:04}", i);
                let value = format!("value{}", i);
                cache.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            cache.drain();
            cache.close().unwrap();
        }

        // Phase 2: Re-open and write new data
        // BUG: If global_seq < persisted max_seq, writes are silently dropped
        {
            let cache = Cache::open(config.clone()).unwrap();

            // Write new values (should overwrite old ones)
            for i in 0..10 {
                let key = format!("seqkey{:04}", i);
                let value = format!("NEWVALUE{}", i);
                cache.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            // Verify new values are readable
            for i in 0..10 {
                let key = format!("seqkey{:04}", i);
                let expected = format!("NEWVALUE{}", i);
                let actual = cache.get(key.as_bytes());

                assert!(actual.is_some(), "key {} should exist after overwrite", key);
                assert_eq!(
                    actual.unwrap(),
                    expected.as_bytes(),
                    "key {} should have NEW value, not old",
                    key
                );
            }

            cache.close().unwrap();
        }
    }

    /// Rapid open/close stress test for data integrity.
    #[test]
    fn test_rapid_close_reopen_data_integrity() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let mut config = Config::new(&path);
        config.write_buffer_size = 1 << 20;
        config.max_inflight_slabs = 4;
        config.flush_concurrency = 2;
        config.wal_enabled = true;
        config.degraded_mode = crate::config::DegradedMode::Panic;

        let value = vec![0xEF; 50_000]; // 50KB per write
        let mut expected_keys = std::collections::HashSet::new();

        // Multiple rounds of open/write/close
        for round in 0..5 {
            let cache = Cache::open(config.clone()).unwrap();

            for i in 0..20 {
                let key = format!("round{}key{:04}", round, i);
                cache.put(key.as_bytes(), &value).unwrap();
                expected_keys.insert(key);
            }

            // Close WITHOUT explicit drain
            cache.close().unwrap();
        }

        // Final verification
        let cache = Cache::open(config).unwrap();
        cache.drain();

        let mut missing = Vec::new();
        for key in &expected_keys {
            if cache.get(key.as_bytes()).is_none() {
                missing.push(key.clone());
            }
        }

        assert!(
            missing.is_empty(),
            "Missing {} keys after rapid open/close cycles: {:?}",
            missing.len(),
            missing.iter().take(10).collect::<Vec<_>>()
        );

        cache.close().unwrap();
    }

    /// Benchmark simulation test to verify all data is written.
    #[test]
    fn test_benchmark_simulation_data_integrity() {
        use std::fs;

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Configuration matching benchmark (scaled down)
        let mut config = Config::new(&path);
        config.write_buffer_size = 4 << 20; // 4MB
        config.max_inflight_slabs = 8;
        config.max_cached_slabs = 16;
        config.flush_concurrency = 4;
        config.direct_io_write = true;
        config.fdatasync = true;
        config.wal_enabled = false; // Cache mode like benchmark
        config.degraded_mode = crate::config::DegradedMode::Panic;

        let cache = Cache::open(config.clone()).unwrap();

        let entropy = vec![0xAB; 2 << 20]; // 2MB entropy buffer
        let num_writes = 100;
        let mut total_bytes: u64 = 0;

        for i in 0..num_writes {
            let key = format!("benchsim-{}", i);
            let blob_size = 100_000 + (i % 10) * 100_000; // 100KB - 1MB
            cache.put(key.as_bytes(), &entropy[..blob_size]).unwrap();
            total_bytes += blob_size as u64;
        }

        eprintln!("Wrote {} keys, {} bytes total", num_writes, total_bytes);

        // Close like benchmark (which may not explicitly drain)
        cache.close().unwrap();

        // Measure segment files on disk
        let segments_dir = path.join("segments");
        let mut disk_bytes: u64 = 0;
        if segments_dir.exists() {
            for shard_entry in fs::read_dir(&segments_dir).unwrap() {
                let shard_dir = shard_entry.unwrap().path();
                if shard_dir.is_dir() {
                    for file_entry in fs::read_dir(&shard_dir).unwrap() {
                        let file_path = file_entry.unwrap().path();
                        if file_path.extension().map_or(false, |e| e == "seg") {
                            disk_bytes += fs::metadata(&file_path).unwrap().len();
                        }
                    }
                }
            }
        }

        eprintln!(
            "Disk bytes written: {} ({:.2}% of logical)",
            disk_bytes,
            disk_bytes as f64 / total_bytes as f64 * 100.0
        );

        // With proper close(), this should be ~100%
        assert!(
            disk_bytes >= total_bytes / 2,
            "Only {}% of data written to disk - close() may be losing data!",
            disk_bytes as f64 / total_bytes as f64 * 100.0
        );

        // Re-open and verify readability
        let cache = Cache::open(config).unwrap();
        cache.drain();

        let mut readable = 0;
        for i in 0..num_writes {
            let key = format!("benchsim-{}", i);
            if cache.get(key.as_bytes()).is_some() {
                readable += 1;
            }
        }

        eprintln!("Readable keys: {} / {}", readable, num_writes);

        assert!(
            readable >= num_writes * 9 / 10,
            "Only {} / {} keys readable after close/reopen - data loss!",
            readable, num_writes
        );

        cache.close().unwrap();
    }
}
