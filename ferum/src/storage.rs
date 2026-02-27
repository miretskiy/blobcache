//! Storage layer for persisted segments.
//!
//! Provides:
//! - `SegmentIDProvider`: Atomic allocation of unique segment IDs
//! - `SegmentWriter`: Creates and writes to segment files
//! - `Archivist`: Read-only access to persisted segments

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::buffer_pool::BufferPool;
use crate::compression;
use crate::error::{Error, Result};
use crate::index::Item;
use crate::iosched::{IOScheduler, PreadScheduler};
use crate::record::{self, FooterEntry, Header, HEADER_SIZE};
use crate::sys::{self, OpenFlags};

/// Segment file extension.
pub const SEGMENT_EXTENSION: &str = ".seg";

/// Index segment file extension (footer snapshot).
pub const INDEX_SEGMENT_EXTENSION: &str = ".iseg";

// =============================================================================
// SegmentIDProvider
// =============================================================================

/// Allocates unique segment IDs atomically.
///
/// Used by both MemTable (normal writes) and Compaction to avoid conflicts.
pub struct SegmentIDProvider {
    counter: AtomicU32,
}

impl SegmentIDProvider {
    /// Creates a new provider initialized from the highest existing segment.
    pub fn new(base_path: &Path, shards: u32) -> Self {
        let max_id = scan_max_segment_id(base_path, shards);
        SegmentIDProvider {
            counter: AtomicU32::new(max_id),
        }
    }

    /// Atomically allocates the next segment ID.
    pub fn next(&self) -> u32 {
        self.counter.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Returns the most recently allocated segment ID.
    /// Used by compaction to determine the "cooling period" boundary.
    pub fn current(&self) -> u32 {
        self.counter.load(Ordering::Acquire)
    }
}

/// Scans the segments directory and returns the highest segment ID found.
fn scan_max_segment_id(base_path: &Path, shards: u32) -> u32 {
    let segments_dir = base_path.join("segments");
    let num_shards = shards.max(1);
    let mut max_id = 0u32;

    for shard in 0..num_shards {
        let shard_dir = segments_dir.join(format!("{:04}", shard));
        let entries = match fs::read_dir(&shard_dir) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if !name_str.ends_with(SEGMENT_EXTENSION) {
                continue;
            }

            // Parse segment ID from filename (e.g., "123.seg")
            if let Some(id_str) = name_str.strip_suffix(SEGMENT_EXTENSION) {
                if let Ok(id) = id_str.parse::<u32>() {
                    max_id = max_id.max(id);
                }
            }
        }
    }

    max_id
}

// =============================================================================
// Path Utilities
// =============================================================================

/// Returns the path for a segment file.
pub fn get_segment_path(base_path: &Path, shards: u32, segment_id: u32) -> PathBuf {
    let shard_no = segment_id % shards.max(1);
    base_path
        .join("segments")
        .join(format!("{:04}", shard_no))
        .join(format!("{}{}", segment_id, SEGMENT_EXTENSION))
}

/// Returns the path for a segment's footer file (.iseg).
pub fn get_footer_path(base_path: &Path, shards: u32, segment_id: u32) -> PathBuf {
    let mut path = get_segment_path(base_path, shards, segment_id);
    let mut name = path.file_name().unwrap().to_os_string();
    name.push(INDEX_SEGMENT_EXTENSION);
    path.set_file_name(name);
    path
}

/// Deletes segment and footer files for the given segment ID.
pub fn delete_segment_files(base_path: &Path, shards: u32, segment_id: u32) -> Result<()> {
    let seg_path = get_segment_path(base_path, shards, segment_id);
    let footer_path = get_footer_path(base_path, shards, segment_id);

    let mut first_error: Option<Error> = None;

    if let Err(e) = fs::remove_file(&seg_path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            first_error = Some(Error::io("delete segment file", e));
        }
    }

    if let Err(e) = fs::remove_file(&footer_path) {
        if e.kind() != std::io::ErrorKind::NotFound && first_error.is_none() {
            first_error = Some(Error::io("delete segment footer", e));
        }
    }

    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// =============================================================================
// Footer Entry to Index Item Conversion
// =============================================================================

/// Converts a single footer entry to an index item.
fn footer_entry_to_item(entry: &FooterEntry, segment_id: u32) -> Result<Item> {
    let physical_len = HEADER_SIZE as i64 + entry.key_len as i64 + entry.physical_size;
    if physical_len > u32::MAX as i64 || entry.pos > u32::MAX as i64 {
        return Err(Error::InvalidConfig {
            message: format!(
                "entry has physicalLen={}, pos={} (exceeds u32)",
                physical_len, entry.pos
            ),
        });
    }

    let mut item = Item::new(
        entry.key,
        segment_id,
        entry.pos as u32,
        physical_len as u32,
    );
    item.set_compression(entry.compression());

    // Copy deleted flag (tombstone)
    if entry.is_deleted() {
        #[cfg(test)]
        eprintln!("footer_entry_to_item: marking {:?} as deleted (flags={:016x})", entry.key, entry.flags);
        item.set_deleted();
    }

    Ok(item)
}

/// Converts footer entries to index items.
pub fn footer_entries_to_items(segment_id: u32, entries: &[FooterEntry]) -> Result<Vec<Item>> {
    let mut items = Vec::with_capacity(entries.len());

    for entry in entries {
        items.push(footer_entry_to_item(entry, segment_id)?);
    }

    Ok(items)
}

// =============================================================================
// Index Recovery
// =============================================================================

/// Entry paired with its segment ID for sorting during recovery.
struct RecoveryEntry {
    entry: FooterEntry,
    segment_id: u32,
}

/// Recovers the index by scanning all segment footer files (.iseg).
///
/// This is called on startup to rebuild the in-memory index from persisted data.
/// Returns the items found and the max sequence ID seen.
///
/// IMPORTANT: Items are sorted by seq_id ascending before returning, so when
/// inserted into the index, newer entries (higher seq_id) overwrite older ones.
/// This ensures tombstones from deletes properly override earlier puts.
pub fn recover_index_from_footers(base_path: &Path, shards: u32) -> Result<(Vec<Item>, u64)> {
    #[cfg(test)]
    eprintln!("recover_index_from_footers: scanning {} shards in {:?}", shards, base_path);

    let mut all_entries: Vec<RecoveryEntry> = Vec::new();
    let mut max_seq_id = 0u64;

    // Scan all shard directories for .iseg files
    // Segment files are in: base_path/segments/{:04}/
    for shard in 0..shards {
        let shard_dir = base_path.join("segments").join(format!("{:04}", shard));
        #[cfg(test)]
        eprintln!("recover_index_from_footers: checking shard dir {:?} exists={}", shard_dir, shard_dir.exists());
        if !shard_dir.exists() {
            continue;
        }

        let dir_entries = fs::read_dir(&shard_dir)
            .map_err(|e| Error::io("read shard directory", e))?;

        for dir_entry in dir_entries {
            let dir_entry = dir_entry.map_err(|e| Error::io("read directory entry", e))?;
            let path = dir_entry.path();

            // Only process .iseg files
            #[cfg(test)]
            eprintln!("recover_index_from_footers: found file {:?}", path);

            if let Some(ext) = path.extension() {
                if ext != "iseg" {
                    #[cfg(test)]
                    eprintln!("recover_index_from_footers: skipping - not iseg");
                    continue;
                }
            } else {
                #[cfg(test)]
                eprintln!("recover_index_from_footers: skipping - no extension");
                continue;
            }

            #[cfg(test)]
            eprintln!("recover_index_from_footers: processing iseg file {:?}", path);

            // Parse segment ID from filename: "123.seg.iseg" -> 123
            let segment_id = match parse_segment_id_from_footer(&path) {
                Some(id) => id,
                None => continue,
            };

            // Read footer file
            let footer_data = fs::read(&path)
                .map_err(|e| Error::io("read footer file", e))?;

            // Decode footer entries
            let entries = record::decode_footer(&footer_data)?;

            // Track max sequence ID and collect entries with segment_id
            for entry in entries {
                if entry.seq_id > max_seq_id {
                    max_seq_id = entry.seq_id;
                }
                all_entries.push(RecoveryEntry { entry, segment_id });
            }
        }
    }

    // Sort by seq_id ascending - this ensures when we insert into index,
    // newer entries (higher seq_id) overwrite older ones for the same key.
    // This is critical for tombstones to properly override earlier puts.
    all_entries.sort_by_key(|e| e.entry.seq_id);

    // Convert to Items
    let mut all_items = Vec::with_capacity(all_entries.len());
    for re in all_entries {
        let item = footer_entry_to_item(&re.entry, re.segment_id)?;
        all_items.push(item);
    }

    Ok((all_items, max_seq_id))
}

/// Parses segment ID from footer filename.
/// Expected format: "123.seg.iseg" -> Some(123)
fn parse_segment_id_from_footer(path: &Path) -> Option<u32> {
    let name = path.file_stem()?.to_str()?;  // "123.seg"
    let base = name.strip_suffix(".seg")?;    // "123"
    base.parse().ok()
}

// =============================================================================
// SegmentWriter
// =============================================================================

/// Manages I/O for a single segment file being written.
pub struct SegmentWriter {
    segment_id: u32,
    base_path: PathBuf,
    shards: u32,
    file: Option<File>,
    #[allow(dead_code)] // Stored for future use in sync operations
    flags: OpenFlags,
}

impl SegmentWriter {
    /// Creates a segment file and returns a writer for it.
    pub fn create(
        segment_id: u32,
        base_path: &Path,
        shards: u32,
        size: i64,
    ) -> Result<Self> {
        Self::create_with_flags(segment_id, base_path, shards, size, OpenFlags::buffered())
    }

    /// Creates a segment file with specific I/O flags.
    pub fn create_with_flags(
        segment_id: u32,
        base_path: &Path,
        shards: u32,
        size: i64,
        flags: OpenFlags,
    ) -> Result<Self> {
        let path = get_segment_path(base_path, shards, segment_id);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| Error::io("create segment directory", e))?;
        }

        // Create file with specified flags (Direct I/O or buffered)
        let file = sys::create_file(&path, flags)?;

        // Pre-allocate space
        if size > 0 {
            if let Err(e) = sys::fallocate(&file, size) {
                // Log but don't fail - fallocate is an optimization
                eprintln!("warning: fallocate failed for segment {}: {}", segment_id, e);
            }
        }

        Ok(SegmentWriter {
            segment_id,
            base_path: base_path.to_path_buf(),
            shards,
            file: Some(file),
            flags,
        })
    }

    /// Returns the path for this segment's data file.
    pub fn path(&self) -> PathBuf {
        get_segment_path(&self.base_path, self.shards, self.segment_id)
    }

    /// Returns the path for this segment's footer file (.iseg).
    pub fn footer_path(&self) -> PathBuf {
        get_footer_path(&self.base_path, self.shards, self.segment_id)
    }

    /// Returns the segment ID.
    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    /// Returns the underlying file handle for writing.
    pub fn file(&self) -> Option<&File> {
        self.file.as_ref()
    }

    /// Returns a mutable reference to the file handle.
    pub fn file_mut(&mut self) -> Option<&mut File> {
        self.file.as_mut()
    }

    /// Writes the segment file header.
    pub fn write_header(&mut self) -> Result<()> {
        if let Some(ref mut file) = self.file {
            let header_bytes = record::file_header_bytes();
            file.write_all(&header_bytes)
                .map_err(|e| Error::io("write segment header", e))?;
        }
        Ok(())
    }

    /// Writes data at the current position.
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        if let Some(ref mut file) = self.file {
            file.write_all(data)
                .map_err(|e| Error::io("write segment data", e))?;
            Ok(data.len())
        } else {
            Err(Error::io("write to closed segment", std::io::Error::other(
                "segment writer closed",
            )))
        }
    }

    /// Writes the footer file for crash recovery.
    pub fn write_footer(&self, entries: &[FooterEntry]) -> Result<()> {
        let footer_path = self.footer_path();
        #[cfg(test)]
        eprintln!("write_footer: path={:?}, entries={}", footer_path, entries.len());

        // Encode footer
        let encoded = record::encode_footer(entries);

        // Write atomically via temp file
        let temp_path = footer_path.with_extension("tmp");
        let mut file = File::create(&temp_path)
            .map_err(|e| Error::io("create footer temp file", e))?;
        file.write_all(&encoded)
            .map_err(|e| Error::io("write footer data", e))?;
        file.sync_all()
            .map_err(|e| Error::io("sync footer file", e))?;
        drop(file);

        fs::rename(&temp_path, &footer_path)
            .map_err(|e| Error::io("rename footer file", e))?;

        #[cfg(test)]
        eprintln!("write_footer: created {:?} exists={}", footer_path, footer_path.exists());

        Ok(())
    }

    /// Syncs and closes the segment file.
    pub fn close(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            file.sync_all()
                .map_err(|e| Error::io("sync segment file", e))?;
        }
        Ok(())
    }
}

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = self.close();
        }
    }
}

// =============================================================================
// Archivist
// =============================================================================

/// Read result from Archivist.
pub struct ReadResult {
    /// The decompressed value data.
    pub value: Vec<u8>,
    /// The stored key bytes (for collision detection).
    pub stored_key: Vec<u8>,
}

/// Manages read-only access to persisted segments.
///
/// Uses the Index Item contract: Offset points to record start,
/// PhysicalLen = 42 + KeyLen + PhysSize.
pub struct Archivist {
    base_path: PathBuf,
    shards: u32,
    /// Cached file handles: segmentID -> File
    cache: RwLock<HashMap<u32, File>>,
    /// Whether to use fadvise hints.
    use_fadvise: bool,
    /// Whether to verify checksums on read.
    verify_checksum: bool,
    /// I/O scheduler for segment reads.
    sched: Arc<dyn IOScheduler>,
    /// Aligned buffer pool (avoids per-read mmap under concurrent load).
    pool: Arc<BufferPool>,
}

impl Archivist {
    /// Creates a new Archivist with the default PreadScheduler.
    pub fn new(base_path: &Path, shards: u32) -> Self {
        Archivist {
            base_path: base_path.to_path_buf(),
            shards,
            cache: RwLock::new(HashMap::new()),
            use_fadvise: true,
            verify_checksum: false,
            sched: Arc::new(PreadScheduler::new()),
            pool: BufferPool::new(),
        }
    }

    /// Creates an Archivist with a custom I/O scheduler.
    pub fn with_scheduler(base_path: &Path, shards: u32, sched: Arc<dyn IOScheduler>) -> Self {
        Archivist {
            base_path: base_path.to_path_buf(),
            shards,
            cache: RwLock::new(HashMap::new()),
            use_fadvise: true,
            verify_checksum: false,
            sched,
            pool: BufferPool::new(),
        }
    }

    /// Enables or disables fadvise hints.
    pub fn set_fadvise(&mut self, enabled: bool) {
        self.use_fadvise = enabled;
    }

    /// Enables or disables checksum verification on read.
    pub fn set_verify_checksum(&mut self, enabled: bool) {
        self.verify_checksum = enabled;
    }

    /// Returns I/O scheduler statistics.
    pub fn io_stats(&self) -> crate::iosched::IOStats {
        self.sched.stats()
    }

    /// Reads a blob from a segment using the configured I/O scheduler.
    ///
    /// Uses `pread(2)` (or io_uring on Linux) with an aligned read buffer.
    /// The `expected_key` is used to verify the stored key matches
    /// (detects 128-bit hash collisions).
    pub fn read_blob(&self, item: &Item, expected_key: &[u8]) -> Result<ReadResult> {
        let file = self.get_segment_file(item.segment_id)?;
        let fd = file.as_raw_fd();

        // Advisory hint to kernel before reading (errors logged but not fatal)
        if self.use_fadvise {
            if let Err(e) = sys::fadvise(&file, item.offset as i64, item.physical_len as i64) {
                eprintln!(
                    "warning: fadvise failed for segment {}: {}",
                    item.segment_id, e
                );
            }
        }

        // Align the read for Direct I/O compatibility and better kernel prefetch.
        let (aligned_off, aligned_len) = sys::align_range(item.offset as u64, item.physical_len as usize);

        // Acquire a pooled aligned buffer (avoids per-read mmap under load).
        let mut pooled = self.pool.acquire(aligned_len);

        let n = self.sched.read_at(fd, &mut pooled, aligned_off)
            .map_err(|e| Error::io("pread segment", e))?;

        // The actual record starts at lead bytes into the aligned buffer.
        let lead = (item.offset as u64 - aligned_off) as usize;
        let rec_end = lead + item.physical_len as usize;

        if n < rec_end {
            return Err(Error::io(
                "read segment",
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("read {} bytes, needed {}", n, rec_end),
                ),
            ));
        }

        let buf = &pooled[lead..rec_end];

        // Parse header
        let header = Header::decode(&buf[..HEADER_SIZE])?;

        // Verify stored key matches expected
        let key_end = HEADER_SIZE + header.key_len as usize;
        let stored_key = buf[HEADER_SIZE..key_end].to_vec();
        if stored_key != expected_key {
            return Err(Error::KeyMismatch);
        }

        // Extract value data
        let value_data = &buf[key_end..];

        // Handle decompression
        let value = if item.is_compressed() {
            let codec = item.compression();
            let mut decompressed = vec![0u8; header.logical_size as usize];
            compression::decompress(codec, &mut decompressed, value_data)?;
            decompressed
        } else {
            value_data.to_vec()
        };

        // Optional integrity verification
        if self.verify_checksum && header.has_valid_crc() {
            let stored_crc = header.crc();
            let computed_crc = record::compute_crc(&stored_key, &value);
            if stored_crc != computed_crc {
                return Err(Error::PayloadCrcMismatch {
                    expected: stored_crc,
                    computed: computed_crc,
                });
            }
        }

        Ok(ReadResult { value, stored_key })
    }

    /// Reads raw record bytes from a segment (no interpretation).
    ///
    /// Used by compaction for copying blobs without parsing.
    pub fn read_blob_raw(&self, item: &Item) -> Result<Vec<u8>> {
        let file = self.get_segment_file(item.segment_id)?;
        let fd = file.as_raw_fd();

        let (aligned_off, aligned_len) = sys::align_range(item.offset as u64, item.physical_len as usize);
        let mut pooled = self.pool.acquire(aligned_len);

        let n = self.sched.read_at(fd, &mut pooled, aligned_off)
            .map_err(|e| Error::io("pread segment raw", e))?;

        let lead = (item.offset as u64 - aligned_off) as usize;
        let rec_end = lead + item.physical_len as usize;
        if n < rec_end {
            return Err(Error::io(
                "read segment raw",
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read"),
            ));
        }

        Ok(pooled[lead..rec_end].to_vec())
    }

    /// Releases disk space for an evicted blob via hole punching.
    pub fn hole_punch(&self, segment_id: u32, offset: u32, physical_len: u32) -> Result<i64> {
        let file = self.get_segment_file(segment_id)?;
        sys::punch_hole(&file, offset as i64, physical_len as i64)
    }

    /// Closes and removes a segment's cached file handle.
    pub fn drop_segment_cache(&self, segment_id: u32) {
        self.cache.write().remove(&segment_id);
    }

    /// Gets or opens a segment file.
    fn get_segment_file(&self, segment_id: u32) -> Result<File> {
        // Check cache first (read lock)
        {
            let cache = self.cache.read();
            if let Some(file) = cache.get(&segment_id) {
                // Clone the file handle (creates new fd pointing to same file)
                return file.try_clone()
                    .map_err(|e| Error::io("clone segment file handle", e));
            }
        }

        // Open the file
        let path = get_segment_path(&self.base_path, self.shards, segment_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| Error::io("open segment file", e))?;

        // Cache the handle (write lock)
        {
            let mut cache = self.cache.write();
            // Check again in case another thread added it
            use std::collections::hash_map::Entry;
            if let Entry::Vacant(e) = cache.entry(segment_id) {
                e.insert(file.try_clone()
                    .map_err(|e| Error::io("clone segment file handle", e))?);
            }
        }

        Ok(file)
    }

    /// Closes all cached segment files.
    pub fn close(&self) -> Result<()> {
        self.cache.write().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::Key;
    use tempfile::tempdir;

    #[test]
    fn test_segment_id_provider() {
        let dir = tempdir().unwrap();
        let provider = SegmentIDProvider::new(dir.path(), 4);

        assert_eq!(provider.current(), 0);
        assert_eq!(provider.next(), 1);
        assert_eq!(provider.next(), 2);
        assert_eq!(provider.current(), 2);
    }

    #[test]
    fn test_get_segment_path() {
        let base = Path::new("/data/cache");
        let path = get_segment_path(base, 4, 123);
        assert!(path.to_string_lossy().contains("segments"));
        assert!(path.to_string_lossy().contains("0003")); // 123 % 4 = 3
        assert!(path.to_string_lossy().ends_with("123.seg"));
    }

    #[test]
    fn test_get_footer_path() {
        let base = Path::new("/data/cache");
        let path = get_footer_path(base, 4, 123);
        assert!(path.to_string_lossy().ends_with("123.seg.iseg"));
    }

    #[test]
    fn test_segment_writer() {
        let dir = tempdir().unwrap();

        // Create segments directory structure
        let shard_dir = dir.path().join("segments").join("0000");
        fs::create_dir_all(&shard_dir).unwrap();

        let mut writer = SegmentWriter::create(1, dir.path(), 4, 4096).unwrap();
        assert_eq!(writer.segment_id(), 1);

        writer.write_header().unwrap();
        writer.write(b"test data").unwrap();
        writer.close().unwrap();

        // Verify file exists
        let path = get_segment_path(dir.path(), 4, 1);
        assert!(path.exists());
    }

    #[test]
    fn test_footer_entries_to_items() {
        let entries = vec![
            FooterEntry {
                key: Key::from_bytes(b"key1"),
                flags: 0,
                seq_id: 1,
                key_len: 4,
                physical_size: 100,
                logical_size: 100,
                pos: 0,
            },
            FooterEntry {
                key: Key::from_bytes(b"key2"),
                flags: 0,
                seq_id: 2,
                key_len: 4,
                physical_size: 200,
                logical_size: 200,
                pos: 146, // 42 + 4 + 100
            },
        ];

        let items = footer_entries_to_items(5, &entries).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].segment_id, 5);
        assert_eq!(items[0].offset, 0);
        assert_eq!(items[0].physical_len, 42 + 4 + 100);
        assert_eq!(items[1].offset, 146);
    }

    #[test]
    fn test_archivist_basic() {
        let dir = tempdir().unwrap();
        let archivist = Archivist::new(dir.path(), 4);

        // Trying to read non-existent segment should fail
        let item = Item::new(Key::from_bytes(b"test"), 999, 0, 100);
        let result = archivist.read_blob(&item, b"test");
        assert!(result.is_err());
    }
}
