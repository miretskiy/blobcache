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
use crate::record::{self, FooterEntry, Record};
use crate::slab::{ActiveSlab, SlabEntry};
use crate::storage::{
    footer_entries_to_items, get_footer_path, get_segment_path, SegmentIDProvider, SegmentWriter,
};
use crate::wal::Wal;

/// Number of index shards for per-key concurrency control.
const NUM_INDEX_SHARDS: usize = 256;

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
    ) -> Result<Arc<Self>> {
        let pool_capacity = config.max_cached_slabs + config.max_inflight_slabs + 2;

        let segment_ids = SegmentIDProvider::new(&base_path, config.shards);

        let slab_pool = MmapPool::new(
            "slab",
            config.write_buffer_size,
            pool_capacity,
        );

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
        });

        // Initialize active slab
        {
            let active = mt.new_active_slab();
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
            let (slab_offset, xl_buffer) = if is_xl {
                // XL Write in CAS mode: Skip slab allocation, data goes to WAL + xl_buf
                // Reserve position for ordering (at page boundary)
                let pos = active.position();

                // Increment pending writes
                active.pending_writes.fetch_add(1, Ordering::AcqRel);

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
                let xl_buf = MmapBuffer::new(write_size);
                rec.encode(xl_buf.as_mut_slice())?;

                (pos, Some(xl_buf))
            } else {
                // Normal allocation
                #[cfg(test)]
                eprintln!("write_to_slab: about to alloc {} bytes, slab capacity={}", write_size, active.capacity());

                let allocation = active.alloc(write_size);

                #[cfg(test)]
                eprintln!("write_to_slab: allocation result = {:?}", allocation);

                if let Some((offset, _)) = allocation {
                    // Increment pending writes
                    active.pending_writes.fetch_add(1, Ordering::AcqRel);

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

                    // Write record to reserved region
                    let active_ref = self.state.lock().active.as_ref().unwrap().buf.clone();
                    rec.encode(active_ref.as_mut_slice().get_mut(offset as usize..).unwrap())?;

                    (offset, None)
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
            let shard = key.shard() as usize;
            let _lock = self.index_locks[shard].lock();

            let state = self.state.lock();
            let active = state.active.as_ref().unwrap();
            #[cfg(test)]
            let index_len_before = active.index.len();

            if let Some(existing) = active.index.get(&key) {
                if seq_id > existing.seq_id {
                    active.index.insert(key, entry);
                }
            } else {
                active.index.insert(key, entry);
            }

            #[cfg(test)]
            {
                let index_len_after = active.index.len();
                eprintln!("write_to_slab: index insert - before={}, after={}", index_len_before, index_len_after);
            }

            drop(state);

            // 7. Decrement pending writes
            let prev = self.state.lock().active.as_ref().unwrap()
                .pending_writes.fetch_sub(1, Ordering::AcqRel);
            if prev == 1 {
                // Last pending write completed
                let state = self.state.lock();
                if state.active.as_ref().unwrap().is_retired() {
                    // Signal flush can proceed
                }
            }

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

        // Send to flusher
        if !self.is_degraded() {
            // Track pending flush BEFORE sending
            self.pending_flushes.fetch_add(1, Ordering::AcqRel);
            let ticket = FlushTicket { active: old };
            if self.flush_tx.try_send(ticket).is_err() {
                // Channel full, decrement counter
                self.pending_flushes.fetch_sub(1, Ordering::AcqRel);
            }
        }

        // Allocate new slab (blocking operation)
        let new_slab = self.new_active_slab();

        // Re-acquire lock and install new slab
        let mut state = self.state.lock();
        state.active = Some(new_slab);
        state.rotating = false;
        self.rotation_cond.notify_all();
    }

    /// Creates a new active slab.
    fn new_active_slab(&self) -> ActiveSlab {
        let buf = if self.is_degraded() {
            MmapBuffer::new(self.config.write_buffer_size)
        } else {
            self.slab_pool.acquire()
        };

        let slab = ActiveSlab::new(buf);

        // Reserve space for file header
        slab.alloc(record::FILE_HEADER_SIZE);

        // Publish to librarian
        self.librarian.publish(slab.as_shared());

        slab
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
                        #[cfg(test)]
                        eprintln!("flush error: {}", e);
                        let _ = &e; // Suppress unused warning in release
                        self.degraded.store(true, Ordering::Release);
                    }
                }
                Err(channel::RecvTimeoutError::Timeout) => continue,
                Err(channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Processes a flush (writes slab to segment file).
    fn process_flush(&self, slab: ActiveSlab) -> Result<()> {
        // Ensure buffer is released back to pool when we're done
        let result = self.do_flush(&slab);
        slab.buf.unpin();

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

        // Write header
        writer.write_header()?;

        if has_xl {
            // Interleave slab data with XL buffers
            let slab_data = slab.buf.as_slice();
            let mut slab_cursor = record::FILE_HEADER_SIZE;
            let mut segment_pos = record::FILE_HEADER_SIZE as i64;

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
            // Simple path: write slab data directly
            let data = &slab.buf.as_slice()[record::FILE_HEADER_SIZE..slab_pos as usize];
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

    /// Closes the MemTable.
    ///
    /// Sets stop flag and waits briefly for flush workers to exit.
    /// Workers check stop flag every 100ms, so we wait 150ms to ensure they see it.
    pub fn close(&self) {
        self.stop.store(true, Ordering::Release);
        // Wait for workers to see the stop flag and exit
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

        MemTable::new(config, dir.path().to_path_buf(), index, librarian, None, None).unwrap()
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
}
