//! Write-Ahead Log with group commit for durability.
//!
//! # Design
//!
//! Group commit batches multiple concurrent writers into a single fsync,
//! amortizing the cost of durability across many operations.
//!
//! Uses O_DIRECT with "Pad & Advance" strategy:
//! - All writes are padded to 4KB boundaries
//! - Zero-padding at tail is treated as EOF during recovery
//! - Bypasses page cache for consistent high-throughput
//!
//! # Leader Election
//!
//! Multiple threads compete to become the "batch leader":
//! 1. Thread adds request to pending queue
//! 2. If `writer_busy` is false, thread becomes leader
//! 3. Leader swaps pending/flushing buffers (ping-pong)
//! 4. Leader releases lock, performs I/O, reacquires lock
//! 5. Leader marks all requests in batch as done, broadcasts
//!
//! This amortizes one fsync across many writers - critical for throughput.

use std::fs::{self, File, OpenOptions};
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex};

use crate::error::{Error, Result};
use crate::record::Record;
use crate::sys::{self, OpenFlags};

// =============================================================================
// Constants
// =============================================================================

/// WAL file magic: "BLOBWAL1" in little-endian ASCII.
pub const FILE_MAGIC: u64 = 0x314C_4157_424F_4C42;

/// Current WAL format version.
pub const FILE_VERSION: u32 = 1;

/// Size of the WAL file header (32 bytes).
pub const FILE_HEADER_SIZE: usize = 32;

/// Default staging buffer size for DirectIO (16MB).
pub const DEFAULT_MAX_BATCH_SIZE: usize = 16 << 20;

// =============================================================================
// FileHeader
// =============================================================================

/// Header at the start of each WAL file.
#[derive(Debug, Clone, Copy, Default)]
pub struct FileHeader {
    pub magic: u64,
    pub version: u32,
    pub flags: u32,
    pub created_at: i64,
    pub reserved: u64,
}

impl FileHeader {
    /// Creates a new header with current timestamp.
    pub fn new() -> Self {
        FileHeader {
            magic: FILE_MAGIC,
            version: FILE_VERSION,
            flags: 0,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            reserved: 0,
        }
    }

    /// Encodes the header into a buffer.
    pub fn encode(&self, buf: &mut [u8]) {
        assert!(buf.len() >= FILE_HEADER_SIZE);
        buf[0..8].copy_from_slice(&self.magic.to_le_bytes());
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.flags.to_le_bytes());
        buf[16..24].copy_from_slice(&(self.created_at as u64).to_le_bytes());
        buf[24..32].copy_from_slice(&self.reserved.to_le_bytes());
    }

    /// Decodes a header from a buffer.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < FILE_HEADER_SIZE {
            return Err(Error::BufferTooSmall {
                needed: FILE_HEADER_SIZE,
                have: buf.len(),
            });
        }

        let magic = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        if magic != FILE_MAGIC {
            return Err(Error::InvalidMagic {
                expected: FILE_MAGIC as u32,
                got: magic as u32,
            });
        }

        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        if version != FILE_VERSION {
            return Err(Error::InvalidConfig {
                message: format!("unsupported WAL version: {}", version),
            });
        }

        Ok(FileHeader {
            magic,
            version,
            flags: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            created_at: u64::from_le_bytes(buf[16..24].try_into().unwrap()) as i64,
            reserved: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        })
    }
}

// =============================================================================
// WriteResult
// =============================================================================

/// Result of a completed WAL write.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriteResult {
    /// Absolute file offset of the record Magic byte.
    pub offset: i64,
    /// Physical size (Header + Key + Value).
    pub bytes_written: i64,
    /// Size on disk including padding.
    pub bytes_aligned: i64,
}

// =============================================================================
// Config
// =============================================================================

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL files.
    pub dir: PathBuf,
    /// File flags for O_DIRECT and sync behavior.
    pub flags: OpenFlags,
    /// Maximum staging buffer size (default: 16MB).
    pub max_batch_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            dir: PathBuf::new(),
            flags: OpenFlags::default(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
        }
    }
}

// =============================================================================
// Request - Internal ticket for group commit
// =============================================================================

/// Internal request for group commit batching.
struct Request {
    /// The record to write (if not rotation).
    record: Option<Record>,
    /// If true, this is a "rotate file" command.
    is_rotation: bool,
    /// Set true when request completes.
    done: bool,
    /// Error from processing (if any).
    error: Option<Error>,
    /// Write result (valid when done && error.is_none() && !is_rotation).
    result: WriteResult,
}

impl Request {
    fn new_write(record: Record) -> Self {
        Request {
            record: Some(record),
            is_rotation: false,
            done: false,
            error: None,
            result: WriteResult::default(),
        }
    }

    fn new_rotation() -> Self {
        Request {
            record: None,
            is_rotation: true,
            done: false,
            error: None,
            result: WriteResult::default(),
        }
    }
}

// =============================================================================
// Stats
// =============================================================================

/// WAL metrics for observability.
#[derive(Debug, Default)]
pub struct WalStats {
    pub written_bytes: AtomicI64,
    pub written_records: AtomicI64,
    pub sync_count: AtomicI64,
    pub group_commits: AtomicI64,
}

// =============================================================================
// WAL State (protected by mutex)
// =============================================================================

struct WalState {
    /// Current file handle.
    file: Option<File>,
    /// Current write position.
    file_offset: i64,
    /// First SeqID of current file (0 = not yet set).
    current_first_id: u64,
    /// Max SeqID from previous closed file (sequence guard).
    last_rotated_seq: u64,
    /// Max SeqID in current file.
    current_max_seq: u64,
    /// Pending requests waiting to be flushed (ping-pong buffer 1).
    pending: Vec<*mut Request>,
    /// Cleared batch buffer for reuse (ping-pong buffer 2).
    flushing: Vec<*mut Request>,
    /// Leader election flag.
    writer_busy: bool,
    /// Reusable encode buffer (4KB aligned for O_DIRECT).
    encode_buf: sys::AlignedBuffer,
}

// Safety: Request pointers are only accessed while holding the mutex,
// and each Request lives on the stack of its owning thread until done.
unsafe impl Send for WalState {}

// =============================================================================
// WAL
// =============================================================================

/// Write-Ahead Log with group commit.
pub struct Wal {
    config: WalConfig,
    /// State protected by mutex.
    state: Mutex<WalState>,
    /// Condition variable for batch completion.
    cond: Condvar,
    /// Shutdown flag.
    closed: AtomicBool,
    /// Metrics.
    pub stats: WalStats,
}

impl Wal {
    /// Opens or creates a WAL in the given directory.
    pub fn open(config: WalConfig) -> Result<Arc<Self>> {
        fs::create_dir_all(&config.dir).map_err(|e| Error::io("create WAL directory", e))?;

        let buf_size = config.max_batch_size + FILE_HEADER_SIZE;
        let aligned_size = sys::page_align(buf_size);

        let wal = Arc::new(Wal {
            config,
            state: Mutex::new(WalState {
                file: None,
                file_offset: 0,
                current_first_id: 0,
                last_rotated_seq: 0,
                current_max_seq: 0,
                pending: Vec::with_capacity(4096),
                flushing: Vec::with_capacity(4096),
                writer_busy: false,
                encode_buf: sys::alloc_aligned(aligned_size),
            }),
            cond: Condvar::new(),
            closed: AtomicBool::new(false),
            stats: WalStats::default(),
        });

        Ok(wal)
    }

    /// Writes a record to the WAL and blocks until the batch is synced.
    /// Multiple concurrent callers batch together for a single fsync.
    pub fn write(&self, record: Record) -> Result<WriteResult> {
        let mut req = Request::new_write(record);
        self.submit(&mut req)?;
        Ok(req.result)
    }

    /// Signals the WAL to close the current file after syncing.
    /// Returns the firstSeqID of the closed file (for DeleteFile later).
    pub fn enqueue_rotation(&self) -> Result<u64> {
        // Capture currentFirstID before submitting rotation
        let closed_file_id = {
            let state = self.state.lock();
            state.current_first_id
        };

        let mut req = Request::new_rotation();
        self.submit(&mut req)?;
        Ok(closed_file_id)
    }

    /// Core submit logic with leader election.
    ///
    /// This is the heart of group commit:
    /// 1. Add request to pending queue
    /// 2. If writer_busy, wait for completion
    /// 3. Otherwise, become leader: swap buffers, process batch, wake waiters
    fn submit(&self, req: &mut Request) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        let mut state = self.state.lock();

        // Add our request to pending queue
        // Safety: req lives on caller's stack until this function returns,
        // and we only access it while holding the mutex or after done=true
        state.pending.push(req as *mut Request);

        loop {
            // Check if our request is done
            if req.done {
                return match req.error.take() {
                    Some(e) => Err(e),
                    None => Ok(()),
                };
            }

            // If another thread is flushing, wait
            if state.writer_busy {
                self.cond.wait(&mut state);
                continue;
            }

            // Become the leader
            state.writer_busy = true;

            // Ping-pong swap: take pending, put (empty) flushing in its place
            // This reuses the Vec capacity, avoiding allocation per batch
            let empty_buf = std::mem::take(&mut state.flushing);
            let mut to_flush = std::mem::replace(&mut state.pending, empty_buf);

            // Release lock during I/O
            drop(state);

            // Process the batch (I/O happens here, no lock held!)
            let batch_err = self.process_batch(&to_flush);

            // Reacquire lock
            state = self.state.lock();
            state.writer_busy = false;

            // Mark all requests as done
            for req_ptr in &to_flush {
                // Safety: each request is on caller's stack and we hold the mutex
                let r = unsafe { &mut **req_ptr };
                if !r.done {
                    if let Some(ref e) = batch_err {
                        r.error = Some(Error::InvalidConfig {
                            message: e.to_string(),
                        });
                    }
                    r.done = true;
                }
            }

            // Clear and return buffer to flushing for reuse next time
            to_flush.clear();
            state.flushing = to_flush;

            // Wake all waiters
            self.cond.notify_all();
        }
    }

    /// Process a batch of requests, handling rotation commands as barriers.
    fn process_batch(&self, batch: &[*mut Request]) -> Option<Error> {
        if batch.is_empty() {
            return None;
        }

        let mut state = self.state.lock();
        let mut i = 0;

        while i < batch.len() {
            // Find first rotation command
            let mut rotation_idx: Option<usize> = None;
            for j in i..batch.len() {
                let req = unsafe { &**batch.get_unchecked(j) };
                if req.is_rotation {
                    rotation_idx = Some(j);
                    break;
                }
            }

            match rotation_idx {
                None => {
                    // No rotation, flush all remaining records
                    if let Err(e) = self.flush_records(&mut state, &batch[i..]) {
                        return Some(e);
                    }
                    break;
                }
                Some(rot_idx) => {
                    // Flush records before rotation
                    if rot_idx > i {
                        if let Err(e) = self.flush_records(&mut state, &batch[i..rot_idx]) {
                            return Some(e);
                        }
                        // Mark these as done
                        for j in i..rot_idx {
                            let req = unsafe { &mut **batch.get_unchecked(j) };
                            req.done = true;
                        }
                    }

                    // Process rotation
                    if let Err(e) = self.close_current_file(&mut state) {
                        return Some(e);
                    }
                    let rot_req = unsafe { &mut **batch.get_unchecked(rot_idx) };
                    rot_req.done = true;

                    i = rot_idx + 1;
                }
            }
        }

        None
    }

    /// Flush a batch of records using "Pad & Advance" for O_DIRECT.
    /// If the batch exceeds buffer capacity, it is split into multiple chunks.
    /// Records larger than the buffer are handled via write_large_record (slow path).
    fn flush_records(&self, state: &mut WalState, batch: &[*mut Request]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // 1. Scan for min/max SeqID and validate sequence guard
        let mut min_seq = u64::MAX;
        let mut max_seq = 0u64;

        for req_ptr in batch {
            let req = unsafe { &**req_ptr };
            if let Some(ref record) = req.record {
                let seq = record.header.seq_id;
                if seq < min_seq {
                    min_seq = seq;
                }
                if seq > max_seq {
                    max_seq = seq;
                }
                // Sequence guard check
                if seq <= state.last_rotated_seq {
                    return Err(Error::InvalidConfig {
                        message: format!(
                            "sequence regression: {} <= {}",
                            seq, state.last_rotated_seq
                        ),
                    });
                }
            }
        }

        // Update max sequence tracker
        if max_seq > state.current_max_seq {
            state.current_max_seq = max_seq;
        }

        // 2. Ensure file is open
        if state.file.is_none() {
            self.ensure_file(state, min_seq)?;
        }

        // 3. Write records in chunks that fit the staging buffer
        let buf_cap = state.encode_buf.capacity();
        let mut total_bytes = 0usize;
        let mut idx = 0;

        while idx < batch.len() {
            // Check if we need file header (may change after oversized writes)
            let need_header = state.file_offset == 0;
            let overhead = if need_header { FILE_HEADER_SIZE } else { 0 };

            let chunk_start = idx;
            let mut chunk_size = overhead;

            while idx < batch.len() {
                let req = unsafe { &*batch[idx] };
                if let Some(ref record) = req.record {
                    let rec_size = record.encoded_size();
                    let projected_write = sys::page_align(chunk_size + rec_size);

                    if projected_write <= buf_cap {
                        // Record fits in current chunk
                        chunk_size += rec_size;
                        idx += 1;
                    } else if chunk_start == idx {
                        // Single record exceeds buffer - use slow path
                        self.write_large_record(state, batch[idx])?;
                        total_bytes += rec_size;
                        idx += 1;
                        // Reset chunk tracking - header may have been written
                        break;
                    } else {
                        // Chunk is full, write what we have
                        break;
                    }
                } else {
                    idx += 1;
                }
            }

            // Write the chunk (if we have records to write)
            if chunk_start < idx && chunk_size > overhead {
                let include_header = state.file_offset == 0;
                let bytes_written = self.write_chunk(state, &batch[chunk_start..idx], include_header)?;
                total_bytes += bytes_written;
            }
        }

        // Update stats
        self.stats.written_bytes.fetch_add(total_bytes as i64, Ordering::Relaxed);
        self.stats.written_records.fetch_add(batch.len() as i64, Ordering::Relaxed);
        self.stats.sync_count.fetch_add(1, Ordering::Relaxed);
        self.stats.group_commits.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Writes a chunk of records that fits in the staging buffer.
    /// Returns the number of payload bytes written (excluding header and padding).
    fn write_chunk(&self, state: &mut WalState, chunk: &[*mut Request], include_header: bool) -> Result<usize> {
        // Calculate payload size
        let mut payload_size = 0usize;
        for &req_ptr in chunk {
            let req = unsafe { &*req_ptr };
            if let Some(ref record) = req.record {
                payload_size += record.encoded_size();
            }
        }

        let mut total_payload = payload_size;
        if include_header {
            total_payload += FILE_HEADER_SIZE;
        }
        let write_size = sys::page_align(total_payload);

        // Track base file offset for WriteResult (before modifying state)
        let base_file_offset = if include_header {
            state.file_offset + FILE_HEADER_SIZE as i64
        } else {
            state.file_offset
        };

        // Build buffer, encode, and write - all in one scope
        {
            let buf = &mut state.encode_buf.spare_capacity_mut()[..write_size];
            let mut buf_offset = 0;

            // Write header if needed
            if include_header {
                let header = FileHeader::new();
                header.encode(&mut buf[0..FILE_HEADER_SIZE]);
                buf_offset = FILE_HEADER_SIZE;
            }

            // Serialize records and populate WriteResult
            let mut record_offset = base_file_offset;
            for &req_ptr in chunk {
                let req = unsafe { &mut *req_ptr };
                if let Some(ref record) = req.record {
                    let rec_size = record.encoded_size();
                    record.encode(&mut buf[buf_offset..]).map_err(|e| {
                        Error::io("encode record", std::io::Error::other(e.to_string()))
                    })?;

                    req.result = WriteResult {
                        offset: record_offset,
                        bytes_written: rec_size as i64,
                        bytes_aligned: write_size as i64,
                    };

                    buf_offset += rec_size;
                    record_offset += rec_size as i64;
                }
            }

            // Zero-pad tail
            for b in &mut buf[buf_offset..write_size] {
                *b = 0;
            }
        }

        // Now write using a fresh slice from encode_buf
        let file = state.file.as_mut().ok_or_else(|| {
            Error::InvalidConfig { message: "WAL file not open".to_string() }
        })?;
        let data = &state.encode_buf.spare_capacity_mut()[..write_size];
        file.write_all(data).map_err(|e| Error::io("write WAL", e))?;
        sys::sync_file(file, self.config.flags)?;
        state.file_offset += write_size as i64;

        Ok(payload_size)
    }

    /// Handles a single record larger than the staging buffer.
    /// Allocates a temporary aligned buffer for the write (slow path).
    fn write_large_record(&self, state: &mut WalState, req_ptr: *mut Request) -> Result<()> {
        let req = unsafe { &mut *req_ptr };
        let record = match &req.record {
            Some(r) => r,
            None => return Ok(()),
        };

        // Validate sequence and update max tracker
        let seq = record.header.seq_id;
        if seq <= state.last_rotated_seq {
            return Err(Error::InvalidConfig {
                message: format!("sequence regression: {} <= {}", seq, state.last_rotated_seq),
            });
        }
        if seq > state.current_max_seq {
            state.current_max_seq = seq;
        }

        // Ensure file is open
        if state.file.is_none() {
            self.ensure_file(state, seq)?;
        }

        // Determine if we need to include the file header
        let include_header = state.file_offset == 0;

        let rec_size = record.encoded_size();
        let mut total_payload = rec_size;
        if include_header {
            total_payload += FILE_HEADER_SIZE;
        }
        let write_size = sys::page_align(total_payload);

        // Allocate temporary aligned buffer
        let mut buf = sys::alloc_aligned(write_size);

        // Track file offset for WriteResult (before writing)
        let record_offset = if include_header {
            state.file_offset + FILE_HEADER_SIZE as i64
        } else {
            state.file_offset
        };

        // Write header if needed
        let mut buf_offset = 0;
        if include_header {
            let header = FileHeader::new();
            header.encode(&mut buf[0..FILE_HEADER_SIZE]);
            buf_offset = FILE_HEADER_SIZE;
        }

        // Serialize record
        record.encode(&mut buf[buf_offset..]).map_err(|e| {
            Error::io("encode record", std::io::Error::other(e.to_string()))
        })?;
        buf_offset += rec_size;

        // Zero-pad tail
        for b in &mut buf[buf_offset..write_size] {
            *b = 0;
        }

        // Populate WriteResult
        req.result = WriteResult {
            offset: record_offset,
            bytes_written: rec_size as i64,
            bytes_aligned: write_size as i64,
        };

        // Write and sync
        self.write_and_sync(state, &buf)?;

        Ok(())
    }

    /// Performs the actual write and fsync.
    fn write_and_sync(&self, state: &mut WalState, buf: &[u8]) -> Result<()> {
        let file = state.file.as_mut().ok_or_else(|| {
            Error::InvalidConfig { message: "WAL file not open".to_string() }
        })?;
        file.write_all(buf).map_err(|e| Error::io("write WAL", e))?;
        sys::sync_file(file, self.config.flags)?;
        state.file_offset += buf.len() as i64;
        Ok(())
    }

    /// Ensures a file is open for writing.
    fn ensure_file(&self, state: &mut WalState, first_seq_id: u64) -> Result<()> {
        if state.file.is_some() {
            return Ok(());
        }

        state.current_first_id = first_seq_id;
        let path = self.wal_path(first_seq_id);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::io("create WAL file", e))?;

        // Set F_NOCACHE on macOS if direct_io is requested
        #[cfg(target_os = "macos")]
        if self.config.flags.direct_io {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
            }
        }

        state.file = Some(file);
        state.file_offset = 0;
        Ok(())
    }

    /// Closes the current WAL file.
    fn close_current_file(&self, state: &mut WalState) -> Result<()> {
        if let Some(ref mut file) = state.file {
            sys::sync_file(file, self.config.flags)?;
        }

        state.file = None;
        state.file_offset = 0;
        state.current_first_id = 0;

        // Latch the guard
        if state.current_max_seq > state.last_rotated_seq {
            state.last_rotated_seq = state.current_max_seq;
        }
        state.current_max_seq = 0;

        Ok(())
    }

    /// Returns the path for a WAL file.
    fn wal_path(&self, first_seq_id: u64) -> PathBuf {
        self.config.dir.join(format!("wal-{:020}.log", first_seq_id))
    }

    /// Returns the path for a WAL file by ID.
    pub fn file_path(&self, file_id: u64) -> PathBuf {
        self.wal_path(file_id)
    }

    /// Deletes a WAL file after its slab has been flushed.
    pub fn delete_file(&self, first_seq_id: u64) -> Result<()> {
        if first_seq_id == 0 {
            return Ok(());
        }
        let path = self.wal_path(first_seq_id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io("delete WAL file", e)),
        }
    }

    /// Returns the first SeqID of the current WAL file.
    pub fn current_first_id(&self) -> u64 {
        self.state.lock().current_first_id
    }

    /// Returns the last rotated sequence ID (sequence guard).
    pub fn last_rotated_seq(&self) -> u64 {
        self.state.lock().last_rotated_seq
    }

    /// Lists WAL files in the directory.
    pub fn list_files(&self) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(&self.config.dir).map_err(|e| Error::io("read WAL dir", e))?;

        let mut files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(is_wal_file)
                    .unwrap_or(false)
            })
            .collect();

        files.sort();
        Ok(files)
    }

    /// Closes the WAL.
    pub fn close(&self) -> Result<()> {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let mut state = self.state.lock();
            self.cond.notify_all();
            self.close_current_file(&mut state)?;
        }
        Ok(())
    }
}

/// Check if filename matches WAL pattern.
fn is_wal_file(name: &str) -> bool {
    name.len() == 28 && name.starts_with("wal-") && name.ends_with(".log")
}

/// Parse sequence ID from WAL filename.
pub fn parse_wal_filename(name: &str) -> Option<u64> {
    if !is_wal_file(name) {
        return None;
    }
    name[4..24].parse().ok()
}

// =============================================================================
// WAL Recovery
// =============================================================================

use crate::record::{self, Header};

/// Trait for types that can replay WAL records during recovery.
pub trait Replayer {
    /// Replays a recovered record to the memtable.
    fn replay_record(&mut self, record: Record) -> Result<()>;

    /// Triggers a flush of the current memtable contents.
    fn flush(&mut self);

    /// Waits for all pending flushes to complete.
    fn drain(&mut self);
}

impl Wal {
    /// Lists WAL files and returns their first SeqIDs.
    pub fn list_wal_file_ids(&self) -> Result<Vec<u64>> {
        let files = self.list_files()?;
        let mut ids = Vec::with_capacity(files.len());
        for path in files {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(id) = parse_wal_filename(name) {
                    ids.push(id);
                }
            }
        }
        Ok(ids)
    }

    /// Scans a WAL file to find the maximum sequence ID.
    pub fn scan_max_seq_id(path: &Path) -> Result<u64> {
        let file = File::open(path).map_err(|e| Error::io("open WAL file", e))?;
        let file_size = file.metadata().map_err(|e| Error::io("stat WAL file", e))?.len() as i64;

        if file_size < FILE_HEADER_SIZE as i64 {
            return Ok(0);
        }

        let mut max_seq = 0u64;
        let mut pos = FILE_HEADER_SIZE as i64;
        let mut header_buf = [0u8; record::HEADER_SIZE];

        #[cfg(unix)]
        use std::os::unix::fs::FileExt;

        while pos < file_size {
            // Read record header
            #[cfg(unix)]
            let n = file
                .read_at(&mut header_buf, pos as u64)
                .map_err(|e| Error::io("read WAL record header", e))?;
            #[cfg(not(unix))]
            let n = 0;

            if n < record::HEADER_SIZE {
                break;
            }

            let hdr = match Header::decode(&header_buf) {
                Ok(h) if h.is_valid() => h,
                _ => {
                    // O_DIRECT padding - skip to next 4KB boundary
                    let next_block = sys::page_align(pos as usize + 1) as i64;
                    if next_block >= file_size {
                        break;
                    }
                    pos = next_block;
                    continue;
                }
            };

            if hdr.seq_id > max_seq {
                max_seq = hdr.seq_id;
            }

            // Advance past this record
            let payload_size = hdr.key_len as i64 + hdr.physical_size;
            pos += record::HEADER_SIZE as i64 + payload_size;
        }

        Ok(max_seq)
    }

    /// Recovers records from a single WAL file, calling apply_fn for each record.
    fn recover_file<F>(&self, path: &Path, mut apply_fn: F) -> Result<()>
    where
        F: FnMut(Record) -> Result<()>,
    {
        let file = File::open(path).map_err(|e| Error::io("open WAL file", e))?;
        let file_size = file.metadata().map_err(|e| Error::io("stat WAL file", e))?.len() as i64;

        if file_size < FILE_HEADER_SIZE as i64 {
            return Ok(());
        }

        // Validate header
        let mut header_buf = [0u8; FILE_HEADER_SIZE];

        #[cfg(unix)]
        use std::os::unix::fs::FileExt;

        #[cfg(unix)]
        file.read_at(&mut header_buf, 0)
            .map_err(|e| Error::io("read WAL header", e))?;

        FileHeader::decode(&header_buf)?;

        let mut pos = FILE_HEADER_SIZE as i64;
        let mut rec_header_buf = [0u8; record::HEADER_SIZE];

        while pos < file_size {
            // Read record header
            #[cfg(unix)]
            let n = file
                .read_at(&mut rec_header_buf, pos as u64)
                .map_err(|e| Error::io("read WAL record header", e))?;
            #[cfg(not(unix))]
            let n = 0;

            if n < record::HEADER_SIZE {
                break;
            }

            let hdr = match Header::decode(&rec_header_buf) {
                Ok(h) if h.is_valid() => h,
                _ => {
                    // O_DIRECT padding - skip to next 4KB boundary
                    let next_block = sys::page_align(pos as usize + 1) as i64;
                    if next_block >= file_size {
                        break;
                    }
                    pos = next_block;
                    continue;
                }
            };

            // Read full record
            let payload_size = hdr.key_len as usize + hdr.physical_size as usize;
            let full_size = record::HEADER_SIZE + payload_size;

            // Sanity check
            if full_size as i64 > file_size - pos {
                let next_block = sys::page_align(pos as usize + 1) as i64;
                if next_block >= file_size {
                    break;
                }
                pos = next_block;
                continue;
            }

            let mut full_buf = vec![0u8; full_size];
            #[cfg(unix)]
            if file.read_at(&mut full_buf, pos as u64).is_err() {
                break;
            }

            // Decode with CRC verification
            match record::Record::decode(&full_buf, true) {
                Ok(rec) => {
                    // Record is already the right type
                    apply_fn(rec)?;
                    pos += full_size as i64;
                }
                Err(_) => {
                    // CRC mismatch - skip to next block boundary
                    pos = sys::page_align(pos as usize + 1) as i64;
                }
            }
        }

        Ok(())
    }

    /// Recovers all WAL files that need recovery.
    ///
    /// For each WAL file:
    /// 1. Check if already committed (using is_committed callback)
    /// 2. If committed, delete the file
    /// 3. If not, replay all records via Replayer, then flush
    ///
    /// Returns true if any files were replayed.
    pub fn recover<F>(&self, replayer: &mut dyn Replayer, is_committed: F) -> Result<bool>
    where
        F: Fn(u64) -> bool,
    {
        let files = self.list_files()?;
        let mut recovered = false;

        for path in files {
            let first_id = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => match parse_wal_filename(name) {
                    Some(id) => id,
                    None => continue,
                },
                None => continue,
            };

            // Check if this WAL's data is already in a committed segment
            if is_committed(first_id) {
                let _ = self.delete_file(first_id);
                continue;
            }

            // Replay all records from this file
            self.recover_file(&path, |rec| replayer.replay_record(rec))?;

            recovered = true;

            // Flush after each file
            replayer.flush();
        }

        // Wait for all flushes to complete
        replayer.drain();

        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_header_roundtrip() {
        let header = FileHeader::new();
        let mut buf = [0u8; FILE_HEADER_SIZE];
        header.encode(&mut buf);

        let decoded = FileHeader::decode(&buf).unwrap();
        assert_eq!(decoded.magic, FILE_MAGIC);
        assert_eq!(decoded.version, FILE_VERSION);
    }

    #[test]
    fn test_wal_filename_parsing() {
        assert!(is_wal_file("wal-00000000000000000001.log"));
        assert!(!is_wal_file("wal-1.log"));
        assert!(!is_wal_file("other.txt"));

        assert_eq!(
            parse_wal_filename("wal-00000000000000000042.log"),
            Some(42)
        );
    }

    #[test]
    fn test_wal_write() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            flags: OpenFlags::default(),
            max_batch_size: 1 << 20,
        };

        let wal = Wal::open(config).unwrap();

        let record = Record::new(1, b"key".to_vec(), b"value".to_vec(), 5);
        let result = wal.write(record).unwrap();

        assert!(result.offset > 0);
        assert!(result.bytes_written > 0);

        wal.close().unwrap();
    }

    #[test]
    fn test_wal_sequence_guard() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            flags: OpenFlags::default(),
            max_batch_size: 1 << 20,
        };

        let wal = Wal::open(config).unwrap();

        // Write record with seq 10
        let record = Record::new(10, b"key".to_vec(), b"value".to_vec(), 5);
        wal.write(record).unwrap();

        // Rotate
        wal.enqueue_rotation().unwrap();

        // Write with seq 5 should fail (regression)
        let record = Record::new(5, b"key2".to_vec(), b"value2".to_vec(), 6);
        let result = wal.write(record);
        assert!(result.is_err());

        // Write with seq 15 should succeed
        let record = Record::new(15, b"key3".to_vec(), b"value3".to_vec(), 6);
        wal.write(record).unwrap();

        wal.close().unwrap();
    }

    #[test]
    fn test_wal_rotation() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            flags: OpenFlags::default(),
            max_batch_size: 1 << 20,
        };

        let wal = Wal::open(config).unwrap();

        // Write first record
        let record = Record::new(1, b"key1".to_vec(), b"value1".to_vec(), 6);
        wal.write(record).unwrap();

        // Rotate
        let old_id = wal.enqueue_rotation().unwrap();
        assert_eq!(old_id, 1);

        // Write second record (new file)
        let record = Record::new(2, b"key2".to_vec(), b"value2".to_vec(), 6);
        wal.write(record).unwrap();

        // Should have 2 WAL files
        let files = wal.list_files().unwrap();
        assert_eq!(files.len(), 2);

        wal.close().unwrap();
    }

    #[test]
    fn test_wal_list_files() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            flags: OpenFlags::default(),
            max_batch_size: 1 << 20,
        };

        let wal = Wal::open(config).unwrap();

        // Initially empty
        let files = wal.list_files().unwrap();
        assert!(files.is_empty());

        // Write creates file
        let record = Record::new(1, b"key".to_vec(), b"value".to_vec(), 5);
        wal.write(record).unwrap();

        let files = wal.list_files().unwrap();
        assert_eq!(files.len(), 1);

        wal.close().unwrap();
    }

    #[test]
    fn test_wal_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let config = WalConfig {
            dir: dir.path().to_path_buf(),
            flags: OpenFlags::default(),
            max_batch_size: 1 << 20,
        };

        let wal = Arc::new(Wal::open(config).unwrap());
        let mut handles = vec![];

        // Spawn multiple writer threads
        // Note: SeqIDs start at 1, not 0, to avoid conflict with initial lastRotatedSeq=0
        for t in 0..4 {
            let w = Arc::clone(&wal);
            handles.push(thread::spawn(move || {
                for i in 1..=100 {
                    let seq = (t * 1000 + i) as u64;
                    let record = Record::new(
                        seq,
                        format!("key-{}-{}", t, i).into_bytes(),
                        format!("value-{}-{}", t, i).into_bytes(),
                        10,
                    );
                    w.write(record).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Check stats - should have batched writes (fewer syncs than records)
        let records = wal.stats.written_records.load(Ordering::Relaxed);
        let syncs = wal.stats.sync_count.load(Ordering::Relaxed);

        assert_eq!(records, 400);
        // Group commit should batch, so syncs < records
        assert!(syncs < records, "Expected batching: {} syncs for {} records", syncs, records);

        wal.close().unwrap();
    }
}
