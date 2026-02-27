//! MemTable: The Write Engine.
//!
//! Orchestrates buffered writes with slab rotation, WAL integration, and background flushing.
//!
//! # Core Patterns
//!
//! - **Reserve-First**: Reserve slab position before WAL write to prevent spillover bug
//! - **Rotation Barrier**: Block writers during slab rotation to ensure sequential handoff
//! - **Sequence Guards**: Prevent stale writes from landing in new slabs
//! - **XL Writes**: Handle oversized blobs via virtual interleaving

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::channel::{self, Receiver, Sender};
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::compression::{self, Codec};
use crate::config::Config;
use crate::durable_index::DurableIndex;
use crate::error::{Error, Result};
use crate::key::Key;
use crate::librarian::Librarian;
use crate::mempool::{MmapBuffer, MmapPool};
use crate::keyindex::KeyIndexEntry;
use crate::record::{self, FooterEntry, Record, HEADER_SIZE};
use crate::slab::{ActiveSlab, SlabEntry};
use crate::storage::{
    footer_entries_to_items, get_footer_path, get_segment_path, SegmentIDProvider, SegmentWriter,
};
use crate::wal::Wal;

use std::sync::atomic::AtomicI64;

/// Number of index shards for per-key concurrency control.
const NUM_INDEX_SHARDS: usize = 256;

/// RAII guard for managing pending_writes counter.
///
/// This ensures the counter is decremented when the guard goes out of scope,
/// even on early return or panic. This fixes the bug where WAL write errors
/// could cause the counter to leak, leading to deadlock in prepare_rotation().
struct PendingWriteGuard {
    counter: Arc<AtomicI64>,
    active: bool,
}

impl PendingWriteGuard {
    fn new(counter: Arc<AtomicI64>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        PendingWriteGuard { counter, active: true }
    }

    /// Disarms the guard, preventing the decrement on drop.
    /// Call this when the write has been successfully committed and we want
    /// to keep the pending count elevated for a bit longer (e.g., during
    /// index update).
    #[allow(dead_code)]
    fn disarm(mut self) {
        self.active = false;
        // Don't forget self - we want Drop to NOT decrement
        std::mem::forget(self);
    }
}

impl Drop for PendingWriteGuard {
    fn drop(&mut self) {
        if self.active {
            self.counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Error returned when write's seqID is older than maxSealedSeq.
#[derive(Debug, Clone)]
pub struct SequenceTooOldError;

impl std::fmt::Display for SequenceTooOldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sequence ID too old: write belongs to sealed slab")
    }
}

impl std::error::Error for SequenceTooOldError {}

// =============================================================================
// FlushTicket
// =============================================================================

/// A ticket for flushing a slab to disk.
struct FlushTicket {
    active: ActiveSlab,
}

// =============================================================================
// MemTable
// =============================================================================

/// Callback invoked after a slab is flushed to disk.
/// Receives the number of bytes flushed.
pub type OnFlushCallback = Arc<dyn Fn(u64) + Send + Sync>;

/// Write engine that manages buffered writes and background flushing.
pub struct MemTable {
    config: Config,
    base_path: PathBuf,

    // Pools
    slab_pool: MmapPool,

    // Components
    index: Arc<DurableIndex>,
    librarian: Arc<Librarian>,
    wal: Option<Arc<Wal>>,
    segment_ids: SegmentIDProvider,

    // State
    state: Mutex<MemTableState>,
    rotation_cond: Condvar,

    // Per-key locks for concurrency control (shard = key.shard() & 0xFF)
    index_locks: [Mutex<()>; NUM_INDEX_SHARDS],

    // Flush workers
    flush_tx: Sender<FlushTicket>,
    flush_rx: Receiver<FlushTicket>,
    stop: AtomicBool,

    // Pending flush tracking (for drain)
    pending_flushes: AtomicUsize,
    flush_done_cond: Condvar,
    flush_done_lock: Mutex<()>,

    // Degraded mode flag
    degraded: AtomicBool,

    // Callback for flush completion (size tracking, eviction)
    on_flush: Option<OnFlushCallback>,

    // Optional KeyIndex for ordered iteration support
    key_index: Option<Arc<crate::keyindex::KeyIndex>>,
}

struct MemTableState {
    active: Option<ActiveSlab>,
    /// Whether rotation is in progress (writers must wait).
    rotating: bool,
    /// Highest SeqID from last sealed slab (prevents time-travel writes).
    max_sealed_seq: u64,
}

impl MemTable {
    /// Creates a new MemTable.
    pub fn new(
        config: Config,
        base_path: PathBuf,
        index: Arc<DurableIndex>,
        librarian: Arc<Librarian>,
        wal: Option<Arc<Wal>>,
        on_flush: Option<OnFlushCallback>,
        key_index: Option<Arc<crate::keyindex::KeyIndex>>,
    ) -> Result<Arc<Self>> {
        let pool_capacity = config.max_cached_slabs + config.max_inflight_slabs + 2;

        let segment_ids = SegmentIDProvider::new(&base_path, config.shards);

        let slab_pool = MmapPool::new(
            "slab",
            config.write_buffer_size,
            pool_capacity,
        )?;

        let (flush_tx, flush_rx) = channel::bounded(config.max_inflight_slabs);

        // Initialize index locks
        let index_locks: [Mutex<()>; NUM_INDEX_SHARDS] =
            std::array::from_fn(|_| Mutex::new(()));

        let mt = Arc::new(MemTable {
            config,
            base_path,
            slab_pool,
            index,
            librarian,
            wal,
            segment_ids,
            state: Mutex::new(MemTableState {
                active: None,
                rotating: false,
                max_sealed_seq: 0,
            }),
            rotation_cond: Condvar::new(),
            index_locks,
            flush_tx,
            flush_rx,
            stop: AtomicBool::new(false),
            pending_flushes: AtomicUsize::new(0),
            flush_done_cond: Condvar::new(),
            flush_done_lock: Mutex::new(()),
            degraded: AtomicBool::new(false),
            on_flush,
            key_index,
        });

        // Initialize active slab
        {
            let active = mt.new_active_slab()?;
            mt.state.lock().active = Some(active);
        }

        // Start flush workers
        let num_workers = mt.config.flush_concurrency.max(1);
        for _ in 0..num_workers {
            let mt_clone = Arc::clone(&mt);
            std::thread::spawn(move || mt_clone.flush_worker());
        }

        Ok(mt)
    }

    /// Returns true if the MemTable is in degraded mode.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Puts a value into the MemTable.
    pub fn put(&self, seq_id: u64, key: Key, key_bytes: &[u8], value: &[u8]) -> Result<()> {
        // 1. Compress before lock (parallel compression)
        let (compressed, codec) = self.maybe_compress(value);
        let value_bytes = compressed.as_deref().unwrap_or(value);

        // 2. Build record
        let mut rec = Record::new(seq_id, key_bytes.to_vec(), value_bytes.to_vec(), value.len() as i64);
        rec.header.set_compression(codec);

        // 3. Write to slab with Reserve-First pattern
        self.write_to_slab(seq_id, key, rec)
    }

    /// Puts a value with a caller-supplied CRC32 checksum (bypasses computation).
    pub fn put_checksummed(
        &self,
        seq_id: u64,
        key: Key,
        key_bytes: &[u8],
        value: &[u8],
        checksum: u32,
    ) -> Result<()> {
        let (compressed, codec) = self.maybe_compress(value);
        let value_bytes = compressed.as_deref().unwrap_or(value);

        let mut rec = Record::new(seq_id, key_bytes.to_vec(), value_bytes.to_vec(), value.len() as i64);
        rec.header.set_compression(codec);
        rec.header.set_crc(checksum);

        self.write_to_slab(seq_id, key, rec)
    }

    /// Deletes a key by writing a tombstone record.
    ///
    /// A tombstone is a record with FLAG_DELETED set and empty value.
    /// The tombstone is persisted to WAL (if enabled) and segments for
    /// crash-safe deletion in CAS mode.
    pub fn delete(&self, seq_id: u64, key: Key, key_bytes: &[u8]) -> Result<()> {
        // Build tombstone record (empty value, deleted flag)
        let mut rec = Record::new(seq_id, key_bytes.to_vec(), Vec::new(), 0);
        rec.header.set_deleted();

        #[cfg(test)]
        eprintln!("memtable.delete: key={:?}, flags={:016x}, is_deleted={}",
            key, rec.header.flags, rec.header.is_deleted());

        // Write tombstone to slab with Reserve-First pattern
        self.write_to_slab(seq_id, key, rec)
    }

    /// Applies compression if enabled and beneficial.
    fn maybe_compress(&self, src: &[u8]) -> (Option<Vec<u8>>, Codec) {
        let codec = self.config.compression;
        let min_size = self.config.compression_min_size;

        // Skip if disabled or too small
        if codec == Codec::None || (min_size > 0 && src.len() < min_size) {
            return (None, Codec::None);
        }

        // Try to compress
        match compression::compress(codec, self.config.compression_level, src) {
            Ok(compressed) if compressed.len() < src.len() - src.len() / 8 => {
                // Only use compression if we save at least 12.5%
                (Some(compressed), codec)
            }
            _ => (None, Codec::None),
        }
    }

    /// Reserve-First write pattern: Reserve → WAL → Fill.
    ///
    /// XL writes (larger than write_buffer_size) are only supported in CAS mode
    /// with WAL. In this case, data goes directly to WAL without slab allocation.
    ///
    /// # Safety Note
    ///
    /// This method captures Arc references to slab components BEFORE releasing the
    /// lock. This is critical because rotation can happen while the lock is released,
    /// and we must continue working with the ORIGINAL slab we reserved space in.
    fn write_to_slab(&self, seq_id: u64, key: Key, rec: Record) -> Result<()> {
        let write_size = rec.encoded_size();
        let is_xl = write_size > self.config.write_buffer_size;

        // XL writes (larger than buffer) require WAL mode for proper handling
        if is_xl && self.wal.is_none() {
            return Err(Error::InvalidConfig {
                message: format!(
                    "write size {} exceeds buffer size {} (enable WAL for XL writes)",
                    write_size, self.config.write_buffer_size
                ),
            });
        }

        // Check degraded mode at entry - fail fast for CAS mode if configured
        if self.is_degraded() && self.wal.is_some() {
            match self.config.degraded_mode {
                crate::config::DegradedMode::Panic => {
                    panic!("write rejected: cache is in degraded mode");
                }
                crate::config::DegradedMode::Return => {
                    return Err(Error::Degraded {
                        reason: "previous I/O error caused degraded mode".to_string(),
                    });
                }
                crate::config::DegradedMode::Log | crate::config::DegradedMode::MemoryOnly => {
                    // Continue with best-effort (RAM only)
                }
            }
        }

        loop {
            let mut state = self.state.lock();

            // 1. Lifecycle Guard: Reject writes older than sealed data
            if seq_id <= state.max_sealed_seq {
                return Err(Error::InvalidConfig {
                    message: format!("sequence {} too old (max sealed: {})", seq_id, state.max_sealed_seq),
                });
            }

            // 2. Wait for rotation to complete
            if state.rotating {
                self.rotation_cond.wait(&mut state);
                continue;
            }

            let active = state.active.as_ref().expect("no active slab");

            // 3. Handle XL vs normal allocation
            // CRITICAL: Capture Arc references BEFORE dropping the lock.
            // Rotation may move the slab out of state.active, but we must
            // continue working with the slab we reserved space in.
            //
            // SAFETY: We use PendingWriteGuard to ensure pending_writes is
            // decremented even on early return (e.g., WAL write error).
            // This fixes the deadlock bug where prepare_rotation() would
            // wait forever for a leaked pending_writes counter.
            let (slab_offset, xl_buffer, slab_buf, slab_index, _pending_guard) = if is_xl {
                // XL Write in CAS mode: Skip slab allocation, data goes to WAL + xl_buf
                // Reserve position for ordering (at page boundary)
                let pos = active.position();

                // Capture references and create guard (increments pending_writes)
                let slab_index = Arc::clone(&active.index);
                let pending_guard = PendingWriteGuard::new(Arc::clone(&active.pending_writes));

                // Track max seq
                loop {
                    let current = active.current_max_seq.load(Ordering::Acquire);
                    if seq_id <= current {
                        break;
                    }
                    if active.current_max_seq.compare_exchange_weak(
                        current, seq_id, Ordering::AcqRel, Ordering::Acquire
                    ).is_ok() {
                        break;
                    }
                }

                drop(state); // Release lock before I/O

                // Create XL buffer for read-after-write (librarian reads from this)
                // XL buffers are standalone (not shared), but we use the same safe
                // pattern for consistency.
                let xl_buf = MmapBuffer::new(write_size)?;
                let encoded = rec.encode_to_vec();
                xl_buf.write_at(0, &encoded);

                (pos, Some(xl_buf), None, slab_index, pending_guard)
            } else {
                // Normal allocation
                #[cfg(test)]
                eprintln!("write_to_slab: about to alloc {} bytes, slab capacity={}", write_size, active.capacity());

                let allocation = active.alloc(write_size);

                #[cfg(test)]
                eprintln!("write_to_slab: allocation result = {:?}", allocation);

                if let Some((offset, _)) = allocation {
                    // Capture Arc references and create guard (increments pending_writes)
                    let slab_buf = Arc::clone(&active.buf);
                    let slab_index = Arc::clone(&active.index);
                    let pending_guard = PendingWriteGuard::new(Arc::clone(&active.pending_writes));

                    // Track max seq
                    loop {
                        let current = active.current_max_seq.load(Ordering::Acquire);
                        if seq_id <= current {
                            break;
                        }
                        if active.current_max_seq.compare_exchange_weak(
                            current, seq_id, Ordering::AcqRel, Ordering::Acquire
                        ).is_ok() {
                            break;
                        }
                    }

                    drop(state); // Release lock before I/O

                    // Write record to reserved region using captured buffer.
                    //
                    // SAFETY: We use encode_to_vec() + write_at() instead of
                    // encode(as_mut_slice()) to avoid undefined behavior.
                    // Multiple threads may be writing to different offsets in
                    // the same buffer concurrently. Using as_mut_slice() would
                    // create multiple overlapping &mut references, which is UB.
                    // The write_at() method uses raw pointer operations that
                    // are safe for non-overlapping concurrent writes.
                    let encoded = rec.encode_to_vec();
                    slab_buf.write_at(offset as usize, &encoded);

                    (offset, None, Some(slab_buf), slab_index, pending_guard)
                } else {
                    // Need rotation
                    self.prepare_rotation(state);
                    continue;
                }
            };

            // 4. WAL write (after reservation, before index update)
            // Capture the WAL offset for flush_via_rename path
            let wal_offset = if let Some(ref wal) = self.wal {
                if !self.is_degraded() {
                    let result = wal.write(rec.clone())?;
                    result.offset
                } else {
                    0
                }
            } else {
                0
            };

            // 5. Create and insert slab entry
            #[cfg(test)]
            if rec.header.is_deleted() {
                eprintln!("write_to_slab: creating SlabEntry for tombstone, flags={:016x}", rec.header.flags);
            }

            // For XL writes, position in footer should use wal_pos (data is in WAL)
            // For normal writes, pos is the slab offset
            let entry = SlabEntry {
                flags: rec.header.flags,
                seq_id: rec.header.seq_id,
                key_len: rec.header.key_len,
                physical_size: rec.header.physical_size,
                logical_size: rec.header.logical_size,
                pos: slab_offset,
                wal_pos: wal_offset,
                xl_buf: xl_buffer,
            };

            // 6. Concurrency Guard: sharded lock for same-key updates
            // Use captured slab_index - do NOT re-acquire main state lock
            let shard = key.shard() as usize;
            let _lock = self.index_locks[shard].lock();

            #[cfg(test)]
            let index_len_before = slab_index.len();

            if let Some(existing) = slab_index.get(&key) {
                if seq_id > existing.seq_id {
                    slab_index.insert(key, entry);
                }
            } else {
                slab_index.insert(key, entry);
            }

            #[cfg(test)]
            {
                let index_len_after = slab_index.len();
                eprintln!("write_to_slab: index insert - before={}, after={}", index_len_before, index_len_after);
            }

            // 7. Pending writes counter is decremented automatically by _pending_guard
            // when it goes out of scope (RAII pattern). This ensures the counter is
            // decremented even on early return due to errors.

            // Keep slab_buf alive until after write is complete
            drop(slab_buf);

            // _pending_guard drops here, decrementing the counter
            return Ok(());
        }
    }

    /// Prepares slab rotation.
    fn prepare_rotation(&self, mut state: MutexGuard<'_, MemTableState>) {
        state.rotating = true;

        let old = state.active.take().unwrap();

        // Update gatekeeper
        let max_seq = old.current_max_seq.load(Ordering::Acquire);
        if max_seq > state.max_sealed_seq {
            state.max_sealed_seq = max_seq;
        }

        // Retire old slab
        old.retire();

        // Release lock before blocking operations
        drop(state);

        // Wait for pending writes to complete
        // (simplified - in production would use a proper signal)
        while old.pending_writes.load(Ordering::Acquire) > 0 {
            std::thread::yield_now();
        }

        // Rotate WAL if present, capture file ID for rename-based flush
        if let Some(ref wal) = self.wal {
            match wal.enqueue_rotation() {
                Ok(closed_file_id) => {
                    // Store WAL file ID in slab for flush_via_rename
                    old.wal_file_id.store(closed_file_id, Ordering::Release);
                }
                Err(e) => {
                    // Log error but continue - flush will use copy path
                    eprintln!("warning: WAL rotation failed: {}", e);
                }
            }
        }

        // Send to flusher - use blocking send for proper backpressure
        // CRITICAL: Never use try_send here as it drops the slab (and its data!) on failure
        if !self.is_degraded() {
            // Track pending flush BEFORE sending
            self.pending_flushes.fetch_add(1, Ordering::AcqRel);
            let ticket = FlushTicket { active: old };
            // Block until flusher has capacity - this is correct backpressure behavior
            let _ = self.flush_tx.send(ticket);
        }

        // Allocate new slab (blocking operation)
        let new_slab = match self.new_active_slab() {
            Ok(slab) => slab,
            Err(e) => {
                // Enter degraded mode on allocation failure
                eprintln!("critical: slab allocation failed during rotation: {}", e);
                self.degraded.store(true, Ordering::Release);
                // Try once more with standalone buffer as last resort
                match MmapBuffer::new(self.config.write_buffer_size) {
                    Ok(buf) => {
                        let slab = ActiveSlab::new(buf);
                        slab.alloc(record::FILE_HEADER_SIZE);
                        slab
                    }
                    Err(e2) => {
                        // Complete failure - release rotation lock and let writes fail
                        eprintln!("fatal: cannot allocate any slab buffer: {}", e2);
                        let mut state = self.state.lock();
                        state.rotating = false;
                        self.rotation_cond.notify_all();
                        return;
                    }
                }
            }
        };

        // Re-acquire lock and install new slab
        let mut state = self.state.lock();
        state.active = Some(new_slab);
        state.rotating = false;
        self.rotation_cond.notify_all();
    }

    /// Creates a new active slab.
    fn new_active_slab(&self) -> Result<ActiveSlab> {
        let buf = if self.is_degraded() {
            MmapBuffer::new(self.config.write_buffer_size)?
        } else {
            self.slab_pool.acquire()?
        };

        let slab = ActiveSlab::new(buf);

        // Reserve space for file header
        slab.alloc(record::FILE_HEADER_SIZE);

        // Publish to librarian
        self.librarian.publish(slab.as_shared());

        Ok(slab)
    }

    /// Background flush worker.
    fn flush_worker(self: &Arc<Self>) {
        loop {
            if self.stop.load(Ordering::Acquire) {
                return;
            }

            match self.flush_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(ticket) => {
                    if let Err(e) = self.process_flush(ticket.active) {
                        // Handle flush failure according to degraded_mode config
                        match self.config.degraded_mode {
                            crate::config::DegradedMode::Panic => {
                                panic!("flush failed: {}", e);
                            }
                            crate::config::DegradedMode::Return => {
                                // Log and enter degraded mode
                                eprintln!("flush error (degraded mode): {}", e);
                                self.degraded.store(true, Ordering::Release);
                            }
                            crate::config::DegradedMode::Log
                            | crate::config::DegradedMode::MemoryOnly => {
                                // Enter degraded mode (MemoryOnly: reads fall back to RAM only)
                                eprintln!("flush error (degraded mode): {}", e);
                                self.degraded.store(true, Ordering::Release);
                            }
                        }
                    }
                }
                Err(channel::RecvTimeoutError::Timeout) => continue,
                Err(channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Processes a flush (writes slab to segment file).
    fn process_flush(&self, slab: ActiveSlab) -> Result<()> {
        // Flush the slab - buffer is released via Arc Drop when slab goes out of scope
        let result = self.do_flush(&slab);
        // slab (and its Arc<MmapBuffer>) dropped here, releasing reference

        // Signal that this flush is done
        self.pending_flushes.fetch_sub(1, Ordering::AcqRel);
        let _lock = self.flush_done_lock.lock();
        self.flush_done_cond.notify_all();

        result
    }

    /// Internal flush implementation.
    ///
    /// Chooses between two paths:
    /// - **flush_via_rename**: When WAL is enabled, rename WAL file directly to segment (1.0x write amp)
    /// - **flush_via_copy**: Copy slab data to new segment file (2.0x write amp, used for cache mode)
    fn do_flush(&self, slab: &ActiveSlab) -> Result<()> {
        #[cfg(test)]
        eprintln!("do_flush: starting, slab position={}", slab.position());

        if self.is_degraded() {
            #[cfg(test)]
            eprintln!("do_flush: DEGRADED MODE - skipping");
            return Ok(());
        }

        // Check if we can use the rename path (WAL enabled with valid file ID)
        let wal_file_id = slab.wal_file_id.load(Ordering::Acquire);
        let use_rename = self.wal.is_some() && wal_file_id != 0;

        if use_rename {
            self.flush_via_rename(slab, wal_file_id)
        } else {
            self.flush_via_copy(slab)
        }
    }

    /// Flush via rename: WAL file becomes the segment file (ZERO COPY!).
    ///
    /// This is the high-performance path used when WAL is enabled.
    /// Write amplification = 1.0x (data written exactly once to WAL, then renamed).
    fn flush_via_rename(&self, slab: &ActiveSlab, wal_file_id: u64) -> Result<()> {
        #[cfg(test)]
        eprintln!("flush_via_rename: wal_file_id={}", wal_file_id);

        // Collect entries using WAL positions (rename path)
        let (entries, max_seq_id) = self.collect_entries(slab, true);
        #[cfg(test)]
        eprintln!("flush_via_rename: collected {} entries, max_seq={}", entries.len(), max_seq_id);

        if entries.is_empty() {
            // Clean up WAL file even if no entries
            if let Some(ref wal) = self.wal {
                let _ = wal.delete_file(wal_file_id);
            }
            #[cfg(test)]
            eprintln!("flush_via_rename: NO ENTRIES - returning early");
            return Ok(());
        }

        // Allocate segment ID
        let segment_id = self.segment_ids.next();

        // Get paths
        let wal = self.wal.as_ref().unwrap();
        let wal_path = wal.file_path(wal_file_id);
        let segment_path = get_segment_path(&self.base_path, self.config.shards, segment_id);

        // Ensure segment directory exists
        if let Some(parent) = segment_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::io("create segment directory", e))?;
        }

        // THE MAGIC: Rename WAL file to segment file (ZERO COPY!)
        #[cfg(test)]
        eprintln!("flush_via_rename: renaming {:?} -> {:?}", wal_path, segment_path);
        std::fs::rename(&wal_path, &segment_path)
            .map_err(|e| Error::io("rename WAL to segment", e))?;

        // Write footer (separate .iseg file for disaster recovery)
        let footer_path = get_footer_path(&self.base_path, self.config.shards, segment_id);
        self.write_footer_file(&footer_path, &entries)?;

        #[cfg(test)]
        eprintln!("flush_via_rename: wrote footer for segment {} with {} entries", segment_id, entries.len());

        // Update index (with persistence for durability)
        let items = footer_entries_to_items(segment_id, &entries)?;
        self.index.ingest_batch(segment_id, &items, max_seq_id)?;
        #[cfg(test)]
        eprintln!("flush_via_rename: updated index with {} items", items.len());

        // Populate KeyIndex (for ordered iteration)
        if let Some(ref ki) = self.key_index {
            let ki_entries = Self::build_keyindex_entries_inner(&entries, slab);
            if !ki_entries.is_empty() && !ki.has_sentinel(segment_id) {
                if let Err(e) = ki.add_entries(segment_id, &ki_entries) {
                    eprintln!("keyindex add_entries failed for segment {}: {}", segment_id, e);
                }
            }
        }

        // Invoke callback
        let bytes_flushed: u64 = entries
            .iter()
            .map(|e| e.physical_size as u64 + e.key_len as u64)
            .sum();

        if let Some(ref callback) = self.on_flush {
            callback(bytes_flushed);
        }

        Ok(())
    }

    /// Flush via copy: Copy slab data to new segment file.
    ///
    /// This is the fallback path used when WAL is disabled (cache mode).
    /// Write amplification = 2.0x (data in slab buffer, then copied to segment).
    ///
    /// For XL writes, interleaves XL buffers with slab data at their reserved positions.
    fn flush_via_copy(&self, slab: &ActiveSlab) -> Result<()> {
        #[cfg(test)]
        eprintln!("flush_via_copy: starting");

        // Collect entries using slab positions (copy path)
        let (mut entries, max_seq_id) = self.collect_entries(slab, false);
        #[cfg(test)]
        eprintln!("flush_via_copy: collected {} entries, max_seq={}", entries.len(), max_seq_id);

        if entries.is_empty() {
            #[cfg(test)]
            eprintln!("flush_via_copy: NO ENTRIES - returning early");
            return Ok(());
        }

        // Collect XL entries for interleaving
        let xl_entries = self.collect_xl_entries(slab);
        let has_xl = !xl_entries.is_empty();

        #[cfg(test)]
        if has_xl {
            eprintln!("flush_via_copy: {} XL entries to interleave", xl_entries.len());
        }

        // Calculate total size including XL data
        let slab_pos = slab.position();
        let xl_total: i64 = xl_entries.iter().map(|(_, buf)| buf.capacity() as i64).sum();
        let total_size = slab_pos + xl_total;

        // Allocate segment ID
        let segment_id = self.segment_ids.next();

        // Create segment writer with total size and appropriate I/O flags
        let mut writer = SegmentWriter::create_with_flags(
            segment_id,
            &self.base_path,
            self.config.shards,
            total_size,
            self.config.segment_write_flags(),
        )?;

        // Copy file header INTO the slab buffer at position 0 (like Go does).
        // This ensures we write header + data together as one aligned write for Direct I/O.
        // Note: Slab is sealed at this point (no concurrent writers), but we use
        // write_at() for consistency with the rest of the codebase.
        let header_bytes = record::file_header_bytes();
        slab.buf.write_at(0, &header_bytes);

        if has_xl {
            // Interleave slab data with XL buffers
            let slab_data = slab.buf.as_slice();
            let mut slab_cursor = 0usize; // Start from 0 (includes header)
            let mut segment_pos = 0i64;

            for (xl_pos, xl_buf) in &xl_entries {
                // Write slab data up to XL position
                let xl_pos_usize = *xl_pos as usize;
                if xl_pos_usize > slab_cursor {
                    let chunk = &slab_data[slab_cursor..xl_pos_usize];
                    writer.write(chunk)?;
                    segment_pos += chunk.len() as i64;
                    slab_cursor = xl_pos_usize;
                }

                // Adjust footer entries that reference this XL data
                for entry in entries.iter_mut() {
                    if entry.pos == *xl_pos {
                        entry.pos = segment_pos;
                    } else if entry.pos > *xl_pos {
                        // Shift positions after XL insertion point
                        entry.pos += xl_buf.capacity() as i64;
                    }
                }

                // Write XL buffer
                writer.write(xl_buf.as_slice())?;
                segment_pos += xl_buf.capacity() as i64;
            }

            // Write remaining slab data
            if slab_cursor < slab_pos as usize {
                let remaining = &slab_data[slab_cursor..slab_pos as usize];
                writer.write(remaining)?;
            }
        } else {
            // Simple path: write entire slab (header + data) in one aligned write.
            // Use aligned_bytes to round up size to 4KB for Direct I/O.
            let data = slab.buf.aligned_bytes(slab_pos as usize);
            writer.write(data)?;
        }

        // Sync and close
        writer.close()?;

        // Write footer
        writer.write_footer(&entries)?;
        #[cfg(test)]
        {
            eprintln!("flush_via_copy: wrote footer for segment {} with {} entries", segment_id, entries.len());
            // List files in base_path after write
            for shard in 0..self.config.shards {
                let shard_dir = self.base_path.join(format!("{:02x}", shard));
                if shard_dir.exists() {
                    if let Ok(entries) = std::fs::read_dir(&shard_dir) {
                        for entry in entries.flatten() {
                            eprintln!("flush_via_copy: shard {} has file: {:?}", shard, entry.file_name());
                        }
                    }
                }
            }
        }

        // Update index (with persistence for durability if enabled)
        let items = footer_entries_to_items(segment_id, &entries)?;
        self.index.ingest_batch(segment_id, &items, max_seq_id)?;
        #[cfg(test)]
        eprintln!("flush_via_copy: updated index with {} items", items.len());

        // Populate KeyIndex (for ordered iteration)
        if let Some(ref ki) = self.key_index {
            let ki_entries = Self::build_keyindex_entries_inner(&entries, slab);
            if !ki_entries.is_empty() && !ki.has_sentinel(segment_id) {
                if let Err(e) = ki.add_entries(segment_id, &ki_entries) {
                    eprintln!("keyindex add_entries failed for segment {}: {}", segment_id, e);
                }
            }
        }

        // Calculate bytes flushed and invoke callback
        let bytes_flushed: u64 = entries
            .iter()
            .map(|e| e.physical_size as u64 + e.key_len as u64)
            .sum();

        if let Some(ref callback) = self.on_flush {
            callback(bytes_flushed);
        }

        Ok(())
    }

    /// Writes footer data to a file atomically.
    fn write_footer_file(&self, footer_path: &std::path::Path, entries: &[FooterEntry]) -> Result<()> {
        use std::io::Write;

        // Encode footer
        let encoded = record::encode_footer(entries);

        // Write atomically via temp file
        // Append ".tmp" to the full path, not replace extension
        let mut temp_name = footer_path.file_name().unwrap().to_os_string();
        temp_name.push(".tmp");
        let temp_path = footer_path.with_file_name(temp_name);

        let mut file = std::fs::File::create(&temp_path)
            .map_err(|e| Error::io("create footer temp file", e))?;
        file.write_all(&encoded)
            .map_err(|e| Error::io("write footer data", e))?;
        file.sync_all()
            .map_err(|e| Error::io("sync footer file", e))?;
        drop(file);

        std::fs::rename(&temp_path, footer_path)
            .map_err(|e| Error::io("rename footer file", e))?;

        Ok(())
    }

    /// Collects XL entries from slab index for interleaving.
    ///
    /// Returns a vec of (position, xl_buffer) pairs sorted by position.
    fn collect_xl_entries(&self, slab: &ActiveSlab) -> Vec<(i64, Arc<MmapBuffer>)> {
        let mut xl_entries = Vec::new();

        slab.index.for_each(|key, entry| {
            let _ = key; // Suppress unused warning
            if let Some(ref xl_buf) = entry.xl_buf {
                xl_entries.push((entry.pos, Arc::clone(xl_buf)));
            }
        });

        // Sort by position for ordered interleaving
        xl_entries.sort_by_key(|(pos, _)| *pos);
        xl_entries
    }

    /// Collects footer entries from slab index.
    ///
    /// # Arguments
    /// * `slab` - The slab to collect entries from
    /// * `use_wal_pos` - If true, use `wal_pos` for positions (rename path).
    ///                   If false, use `pos` for positions (copy path).
    fn collect_entries(&self, slab: &ActiveSlab, use_wal_pos: bool) -> (Vec<FooterEntry>, u64) {
        let mut entries = Vec::new();
        let mut max_seq_id = 0u64;

        #[cfg(test)]
        eprintln!("collect_entries: slab index has {} entries, use_wal_pos={}", slab.index.len(), use_wal_pos);

        slab.index.for_each(|key, entry| {
            // For rename path, use WAL position; for copy path, use slab position
            let pos = if use_wal_pos { entry.wal_pos } else { entry.pos };

            #[cfg(test)]
            eprintln!("collect_entries: entry {:?} flags={:016x} pos={} wal_pos={} using={}",
                key, entry.flags, entry.pos, entry.wal_pos, pos);

            // Update max sequence ID
            if entry.seq_id > max_seq_id {
                max_seq_id = entry.seq_id;
            }

            // Convert SlabEntry to FooterEntry
            let footer_entry = FooterEntry {
                key,
                key_len: entry.key_len,
                pos,
                physical_size: entry.physical_size,
                logical_size: entry.logical_size,
                flags: entry.flags,
                seq_id: entry.seq_id,
            };

            #[cfg(test)]
            if footer_entry.is_deleted() {
                eprintln!("collect_entries: tombstone {:?} flags={:016x}", key, entry.flags);
            }

            entries.push(footer_entry);
        });

        (entries, max_seq_id)
    }

    /// Builds KeyIndexEntry list from footer entries + slab.
    ///
    /// Reads user keys from the slab buffer or XL buffer to populate the
    /// KeyIndex with hash → user_key mappings.
    fn build_keyindex_entries_inner(
        entries: &[FooterEntry],
        slab: &ActiveSlab,
    ) -> Vec<KeyIndexEntry> {
        let mut ki_entries = Vec::with_capacity(entries.len());

        slab.index.for_each(|hash, slab_entry| {
            // Skip tombstones
            if slab_entry.is_deleted() {
                return;
            }
            // Skip entries not in the footer list (shouldn't happen but be safe)
            if !entries.iter().any(|fe| fe.key == hash) {
                return;
            }

            // Read key from the appropriate buffer
            let (buf, offset) = if let Some(ref xl_buf) = slab_entry.xl_buf {
                (xl_buf.as_slice(), 0usize)
            } else {
                (slab.buf.as_slice(), slab_entry.pos as usize)
            };

            let key_start = offset + HEADER_SIZE;
            let key_end = key_start + slab_entry.key_len as usize;
            if key_end <= buf.len() {
                let user_key = buf[key_start..key_end].to_vec();
                ki_entries.push(KeyIndexEntry { hash, user_key });
            }
        });

        ki_entries
    }

    /// Triggers a flush of the current slab.
    pub fn flush(&self) {
        if self.is_degraded() {
            return;
        }

        let state = self.state.lock();
        let should_rotate = state.active.is_some()
            && state.active.as_ref().unwrap().position() > record::FILE_HEADER_SIZE as i64;
        if should_rotate {
            self.prepare_rotation(state);
        }
    }

    /// Drains all pending flushes and waits for completion.
    pub fn drain(&self) {
        self.flush();

        // Wait for all pending flushes to complete
        let mut lock = self.flush_done_lock.lock();
        while self.pending_flushes.load(Ordering::Acquire) > 0 {
            self.flush_done_cond.wait(&mut lock);
        }
    }

    /// Debug: Returns (slab_position, index_entries) for debugging.
    #[cfg(test)]
    pub fn debug_slab_info(&self) -> (i64, usize) {
        let state = self.state.lock();
        match &state.active {
            Some(active) => (active.position(), active.index.len()),
            None => (-1, 0),
        }
    }

    /// Replays a WAL record directly during recovery.
    ///
    /// MUST only be called during initialization (no concurrent writers).
    /// Bypasses compression and CRC computation - record is written verbatim.
    /// This is critical for performance: WAL records are already in final form.
    pub fn replay_record(&self, key: Key, record: &Record) -> Result<()> {
        let write_size = record.encoded_size();
        let mut state = self.state.lock();

        // Ensure we have an active slab
        if state.active.is_none() {
            let buf = self.slab_pool.acquire()?;
            state.active = Some(ActiveSlab::new(buf));
        }

        // Try to allocate space in current slab
        let active = state.active.as_mut().unwrap();
        let allocation = active.alloc(write_size);

        let wpos = match allocation {
            Some((offset, _)) => offset,
            None => {
                // Current slab is full - rotate to a new slab
                // During recovery, we use a simple rotation:
                // 1. Queue old slab for flush via channel
                // 2. Create new slab
                let old_slab = state.active.take().unwrap();

                // Send to flush workers via channel (like normal rotation)
                let ticket = FlushTicket { active: old_slab };
                if self.flush_tx.send(ticket).is_err() {
                    return Err(Error::InvalidConfig {
                        message: "flush channel closed during recovery".to_string(),
                    });
                }
                self.pending_flushes.fetch_add(1, Ordering::AcqRel);

                // Create new slab
                let buf = self.slab_pool.acquire()?;
                state.active = Some(ActiveSlab::new(buf));

                // Allocate from new slab
                let active = state.active.as_mut().unwrap();
                let (offset, _) = active.alloc(write_size).ok_or_else(|| {
                    Error::InvalidConfig {
                        message: "record too large for slab buffer".to_string(),
                    }
                })?;
                offset
            }
        };

        // Write the record directly (no compression, no CRC recalc - already done in WAL)
        let active = state.active.as_ref().unwrap();
        let dst = active.buf.as_mut_slice().get_mut(wpos as usize..).ok_or_else(|| {
            Error::InvalidConfig {
                message: "invalid slab offset during replay".to_string(),
            }
        })?;
        record.encode(dst)?;

        // Update max sequence ID
        if record.header.seq_id > active.current_max_seq.load(Ordering::Acquire) {
            active.current_max_seq.store(record.header.seq_id, Ordering::Release);
        }

        // Add to slab index
        let entry = SlabEntry::from_header(&record.header, wpos);
        active.index.insert(key, entry);

        Ok(())
    }

    /// Closes the MemTable.
    ///
    /// IMPORTANT: This method properly drains all pending data before stopping
    /// flush workers. Simply sleeping and hoping workers finish is NOT safe -
    /// it can silently drop gigabytes of data sitting in the flush channel.
    ///
    /// The correct shutdown sequence is:
    /// 1. Flush the current slab (rotate it into the flush channel)
    /// 2. Drain all pending flushes (wait for channel to empty)
    /// 3. Signal workers to stop
    /// 4. Wait briefly for workers to see the stop flag
    pub fn close(&self) {
        // Step 1 & 2: Drain ensures current slab is flushed and all pending
        // flushes complete. This is CRITICAL - without it, data in the channel
        // is silently dropped when workers exit.
        self.drain();

        // Step 3: Signal workers to stop
        self.stop.store(true, Ordering::Release);

        // Step 4: Wait for workers to see the stop flag and exit
        // Workers use 100ms recv_timeout, so 150ms should be enough
        std::thread::sleep(std::time::Duration::from_millis(150));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_memtable() -> Arc<MemTable> {
        let dir = tempdir().unwrap();
        let config = Config::new(dir.path());
        // Use DurableIndex without persistence for tests (memory-only mode)
        let index = Arc::new(DurableIndex::open(None, 1 << 20).unwrap());
        let librarian = Arc::new(Librarian::new(4));

        MemTable::new(config, dir.path().to_path_buf(), index, librarian, None, None, None).unwrap()
    }

    #[test]
    fn test_memtable_basic() {
        let mt = create_test_memtable();
        let key = Key::from_bytes(b"testkey");
        let value = b"testvalue";

        mt.put(1, key, b"testkey", value).unwrap();
    }

    #[test]
    fn test_memtable_sequence_guard() {
        let mt = create_test_memtable();
        let key = Key::from_bytes(b"key1");

        // First write succeeds
        mt.put(10, key, b"key1", b"value1").unwrap();

        // Second write with higher seq succeeds
        mt.put(20, key, b"key1", b"value2").unwrap();
    }

    // =========================================================================
    // Bug regression tests
    // =========================================================================

    /// Test that pending_writes counter is properly managed and doesn't leak.
    ///
    /// If pending_writes leaks (e.g., due to early return on error without
    /// decrementing), prepare_rotation() will deadlock waiting for pending_writes
    /// to reach 0.
    #[test]
    fn test_pending_writes_balanced_after_writes() {
        let dir = tempdir().unwrap();
        let mut config = Config::new(dir.path());
        config.write_buffer_size = 1 << 20; // 1MB - small to force rotation
        config.max_inflight_slabs = 4;
        config.flush_concurrency = 2;

        let index = Arc::new(DurableIndex::open(None, 1 << 20).unwrap());
        let librarian = Arc::new(Librarian::new(4));

        let mt = MemTable::new(config, dir.path().to_path_buf(), index, librarian, None, None, None).unwrap();

        // Write enough to fill buffer and trigger multiple rotations
        let value = vec![0xAB; 100_000]; // 100KB per write
        for i in 0..20 {
            let key = Key::from_bytes(format!("key{:04}", i).as_bytes());
            mt.put((i + 1) as u64, key, format!("key{:04}", i).as_bytes(), &value).unwrap();
        }

        // Flush should not hang (would deadlock if pending_writes leaked)
        let flush_start = std::time::Instant::now();
        mt.flush();

        assert!(
            flush_start.elapsed() < std::time::Duration::from_secs(5),
            "flush took too long - possible deadlock due to pending_writes leak"
        );

        // Drain should also complete
        let drain_start = std::time::Instant::now();
        mt.drain();
        assert!(
            drain_start.elapsed() < std::time::Duration::from_secs(5),
            "drain took too long - possible deadlock due to pending_writes leak"
        );

        mt.close();
    }

    /// Test that close() does not leave pending writes in the channel.
    ///
    /// BUG: close() only sets stop flag and sleeps 150ms, but does NOT:
    /// 1. Flush the current slab
    /// 2. Wait for the flush channel to empty
    ///
    /// This test verifies the fix by checking that close() properly drains.
    #[test]
    fn test_close_drains_pending_slabs() {
        let dir = tempdir().unwrap();
        let mut config = Config::new(dir.path());
        config.write_buffer_size = 1 << 20; // 1MB
        config.max_inflight_slabs = 8;
        config.flush_concurrency = 2; // Slow workers to build up queue

        let index = Arc::new(DurableIndex::open(None, 1 << 20).unwrap());
        let librarian = Arc::new(Librarian::new(4));

        let mt = MemTable::new(config, dir.path().to_path_buf(), index, librarian, None, None, None).unwrap();

        // Write enough to create multiple pending slabs
        let value = vec![0xAB; 200_000]; // 200KB per write
        for i in 0..50 {
            let key = Key::from_bytes(format!("closekey{:04}", i).as_bytes());
            mt.put((i + 1) as u64, key, format!("closekey{:04}", i).as_bytes(), &value).unwrap();
        }

        // close() should complete in reasonable time and drain all data
        let close_start = std::time::Instant::now();
        mt.close();

        let close_duration = close_start.elapsed();
        assert!(
            close_duration < std::time::Duration::from_secs(30),
            "close() took {:.2}s - too slow, possible hang",
            close_duration.as_secs_f64()
        );
    }
}
