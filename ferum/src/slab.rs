//! Slab data structures for the MemTable.
//!
//! Provides:
//! - `SlabEntry`: Per-key metadata stored in the slab index
//! - `SharedSlab`: Read-only view of a populated slab
//! - `ActiveSlab`: Write-side of a slab being filled

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use std::ops::Deref;

use crate::compression::{self, Codec};
use crate::error::BlobErrno;
use crate::key::Key;
use crate::mempool::MmapBuffer;
use crate::record::{Header, HEADER_SIZE};
use crate::sys;

// =============================================================================
// PinnedBlob
// =============================================================================

/// Zero-copy handle to blob data pinned in an mmap'd buffer.
///
/// For uncompressed blobs: points directly into the mmap arena (no copy).
/// For compressed blobs: owns a decompressed Vec<u8>.
///
/// The data is valid for as long as `PinnedBlob` is held. Drop it to release
/// the reference to the underlying mmap buffer.
pub struct PinnedBlob {
    inner: PinnedInner,
    /// The stored key bytes for collision detection (always a copy, keys are small).
    pub stored_key: Vec<u8>,
}

enum PinnedInner {
    /// Uncompressed: raw pointer + len pinning an Arc<MmapBuffer>.
    Pinned {
        ptr: *const u8,
        len: usize,
        /// Keeps the mmap buffer alive for the lifetime of this blob.
        _pin: Arc<MmapBuffer>,
    },
    /// Compressed or decompressed copy.
    Owned(Vec<u8>),
}

// SAFETY: The Arc<MmapBuffer> in `_pin` owns the underlying memory. The raw
// pointer is valid for at least as long as PinnedBlob is alive. We never write
// through the pointer, so sharing across threads is safe.
unsafe impl Send for PinnedBlob {}
unsafe impl Sync for PinnedBlob {}

impl PinnedBlob {
    /// Creates a zero-copy view into an mmap buffer.
    pub(crate) fn pinned(ptr: *const u8, len: usize, pin: Arc<MmapBuffer>) -> Self {
        PinnedBlob {
            inner: PinnedInner::Pinned { ptr, len, _pin: pin },
            stored_key: Vec::new(),
        }
    }

    /// Creates an owned (decompressed) blob.
    pub(crate) fn owned(data: Vec<u8>) -> Self {
        PinnedBlob {
            inner: PinnedInner::Owned(data),
            stored_key: Vec::new(),
        }
    }

    pub(crate) fn with_key(mut self, key: Vec<u8>) -> Self {
        self.stored_key = key;
        self
    }

    /// Copies the data into an owned Vec<u8>.
    pub fn to_owned_vec(&self) -> Vec<u8> {
        self.deref().to_vec()
    }
}

impl Deref for PinnedBlob {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.inner {
            PinnedInner::Pinned { ptr, len, .. } => {
                // SAFETY: _pin keeps the mmap alive, ptr is valid for `len` bytes.
                unsafe { std::slice::from_raw_parts(*ptr, *len) }
            }
            PinnedInner::Owned(v) => v.as_slice(),
        }
    }
}

impl AsRef<[u8]> for PinnedBlob {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl PartialEq<[u8]> for PinnedBlob {
    fn eq(&self, other: &[u8]) -> bool {
        self.deref() == other
    }
}

impl PartialEq<Vec<u8>> for PinnedBlob {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.deref() == other.as_slice()
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for PinnedBlob {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.deref() == *other
    }
}

impl std::fmt::Debug for PinnedBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedBlob")
            .field("len", &self.deref().len())
            .field("stored_key", &self.stored_key)
            .finish()
    }
}

// =============================================================================
// SlabEntry
// =============================================================================

/// Per-key entry stored in the slab's in-memory index.
///
/// Embeds record header information for quick access to compression/error flags.
#[derive(Debug, Clone, Default)]
pub struct SlabEntry {
    /// Flags from record header (compression, errno, CRC).
    pub flags: u64,
    /// Sequence ID of this write.
    pub seq_id: u64,
    /// Key length (for reading key bytes from buffer).
    pub key_len: u16,
    /// Physical size on disk (possibly compressed).
    pub physical_size: i64,
    /// Logical (uncompressed) size.
    pub logical_size: i64,
    /// Byte offset within slab buffer.
    pub pos: i64,
    /// Byte offset within WAL file (when WAL enabled).
    pub wal_pos: i64,
    /// For XL (extra large) writes that exceed buffer size.
    /// If set, this entry's data is in a separate XL buffer.
    pub xl_buf: Option<Arc<MmapBuffer>>,
}

impl SlabEntry {
    /// Creates an entry from a record header and position.
    pub fn from_header(header: &Header, pos: i64) -> Self {
        SlabEntry {
            flags: header.flags,
            seq_id: header.seq_id,
            key_len: header.key_len,
            physical_size: header.physical_size,
            logical_size: header.logical_size,
            pos,
            wal_pos: 0,
            xl_buf: None,
        }
    }

    /// Returns the compression codec.
    #[inline]
    pub fn compression(&self) -> Codec {
        Codec::from_raw(((self.flags & 0xF000_0000_0000_0000) >> 60) as u8)
    }

    /// Returns true if the entry is compressed.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.compression() != Codec::None
    }

    /// Returns the error code.
    #[inline]
    pub fn errno(&self) -> BlobErrno {
        BlobErrno::from_raw(((self.flags >> 34) & 0x1F) as u8)
    }

    /// Returns true if there's an error.
    #[inline]
    pub fn has_error(&self) -> bool {
        !self.errno().is_ok()
    }

    /// Returns true if this is a tombstone (deleted entry).
    #[inline]
    pub fn is_deleted(&self) -> bool {
        // Deleted flag is bit 33 (same as record::FLAG_DELETED)
        const FLAG_DELETED: u64 = 1 << 33;
        (self.flags & FLAG_DELETED) != 0
    }
}

// =============================================================================
// SlabIndex
// =============================================================================

/// A concurrent hash map for slab entries.
///
/// Uses a RwLock for simplicity. For production, consider a sharded design
/// like the Go xmap.Map.
pub struct SlabIndex {
    map: RwLock<HashMap<Key, SlabEntry>>,
}

impl SlabIndex {
    /// Creates a new empty index.
    pub fn new() -> Self {
        SlabIndex {
            map: RwLock::new(HashMap::new()),
        }
    }

    /// Gets an entry by key.
    pub fn get(&self, key: &Key) -> Option<SlabEntry> {
        self.map.read().get(key).cloned()
    }

    /// Inserts or updates an entry.
    pub fn insert(&self, key: Key, entry: SlabEntry) {
        self.map.write().insert(key, entry);
    }

    /// Removes an entry.
    pub fn delete(&self, key: &Key) {
        self.map.write().remove(key);
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }

    /// Iterates over all entries, returning (key, entry) pairs.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(Key, &SlabEntry),
    {
        let map = self.map.read();
        for (key, entry) in map.iter() {
            f(*key, entry);
        }
    }
}

impl Default for SlabIndex {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// SharedSlab
// =============================================================================

/// A read-only view of a populated slab buffer and its index.
#[derive(Clone)]
pub struct SharedSlab {
    /// The underlying buffer.
    pub buf: Arc<MmapBuffer>,
    /// The key-to-entry index.
    pub index: Arc<SlabIndex>,
}

impl SharedSlab {
    /// Creates a new shared slab.
    pub fn new(buf: Arc<MmapBuffer>, index: Arc<SlabIndex>) -> Self {
        SharedSlab { buf, index }
    }

    /// Acquires data for a key, returning a zero-copy `PinnedBlob`.
    ///
    /// For uncompressed blobs the returned blob holds a raw pointer into the
    /// mmap buffer (no copy). For compressed blobs the returned blob owns the
    /// decompressed bytes.
    ///
    /// Returns:
    /// - `Ok(Some(blob))`: Found with data
    /// - `Ok(None)`: Not found or tombstone
    /// - `Err(errno)`: Found but has error flag set
    pub fn acquire(&self, key: &Key) -> Result<Option<PinnedBlob>, BlobErrno> {
        // 1. Lock-free lookup
        let entry = match self.index.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        // 2. Check for tombstone (deleted entry)
        if entry.is_deleted() {
            return Ok(None);
        }

        // 3. Check for existing error
        if entry.has_error() {
            return Err(entry.errno());
        }

        // 4. Determine which buffer to read from.
        // For XL entries, xl_buf contains the full record starting at offset 0.
        // For normal entries, pos points to the record start in the slab buffer.
        let (buf, offset) = if let Some(ref xl_buf) = entry.xl_buf {
            (Arc::clone(xl_buf), 0i64)
        } else {
            (Arc::clone(&self.buf), entry.pos)
        };

        // 5. Extract key bytes (always copied — small and needed for collision detection)
        let key_start = offset as usize + HEADER_SIZE;
        let key_end = key_start + entry.key_len as usize;
        let stored_key = buf.as_slice()[key_start..key_end].to_vec();

        // 6. Build PinnedBlob — zero-copy for uncompressed, owned copy for compressed.
        let value_start = key_end;
        let value_end = key_end + entry.physical_size as usize;

        let blob = if entry.is_compressed() {
            let physical_data = &buf.as_slice()[value_start..value_end];
            let mut decompressed = vec![0u8; entry.logical_size as usize];
            match compression::decompress(entry.compression(), &mut decompressed, physical_data) {
                Ok(()) => PinnedBlob::owned(decompressed).with_key(stored_key),
                Err(_) => return Err(BlobErrno::Decompression),
            }
        } else {
            // Zero-copy: pin a raw pointer into the mmap arena.
            let slice = &buf.as_slice()[value_start..value_end];
            let ptr = slice.as_ptr();
            let len = slice.len();
            PinnedBlob::pinned(ptr, len, buf).with_key(stored_key)
        };

        Ok(Some(blob))
    }

    /// Invalidates a key from the index.
    ///
    /// Used by Delete to prevent serving stale data.
    pub fn invalidate(&self, key: &Key) {
        self.index.delete(key);
    }
}

// =============================================================================
// ActiveSlab
// =============================================================================

/// A slab that is actively being written to.
///
/// Contains the write position and pending write tracking.
pub struct ActiveSlab {
    /// The underlying buffer.
    pub buf: Arc<MmapBuffer>,
    /// The key-to-entry index.
    pub index: Arc<SlabIndex>,
    /// Current write position in the buffer.
    pub write_pos: AtomicI64,
    /// Number of pending writes (writers that have reserved but not committed).
    /// Wrapped in Arc so writers can hold a reference across lock releases.
    pub pending_writes: Arc<AtomicI64>,
    /// Whether this slab has been retired (no longer accepting writes).
    pub retired: AtomicBool,
    /// ID of the WAL file containing this slab's records.
    pub wal_file_id: AtomicU64,
    /// Highest sequence ID written to this slab.
    pub current_max_seq: AtomicU64,
    /// Cumulative size of XL (extra large) buffers.
    pub xl_size: AtomicI64,
}

impl ActiveSlab {
    /// Creates a new active slab from a buffer.
    pub fn new(buf: Arc<MmapBuffer>) -> Self {
        ActiveSlab {
            buf,
            index: Arc::new(SlabIndex::new()),
            write_pos: AtomicI64::new(0),
            pending_writes: Arc::new(AtomicI64::new(0)),
            retired: AtomicBool::new(false),
            wal_file_id: AtomicU64::new(0),
            current_max_seq: AtomicU64::new(0),
            xl_size: AtomicI64::new(0),
        }
    }

    /// Reserves `n` bytes in the slab.
    ///
    /// Returns `(offset, end)` where offset is the start position and end is after the reservation.
    /// Returns `None` if there isn't enough capacity.
    pub fn alloc(&self, n: usize) -> Option<(i64, i64)> {
        loop {
            let pos = self.write_pos.load(Ordering::Acquire);
            let end = pos + n as i64;
            if end > self.buf.capacity() as i64 {
                return None;
            }
            if self
                .write_pos
                .compare_exchange_weak(pos, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some((pos, end));
            }
        }
    }

    /// Returns a mutable slice for writing at the given offset.
    ///
    /// # Safety
    ///
    /// Caller must ensure exclusive access to this region.
    pub fn slice_mut(&self, offset: i64, len: usize) -> &mut [u8] {
        &mut self.buf.as_mut_slice()[offset as usize..offset as usize + len]
    }

    /// Aligns the write position to the next page boundary.
    pub fn align_to_page(&self) -> i64 {
        loop {
            let pos = self.write_pos.load(Ordering::Acquire);
            let aligned = sys::page_align(pos as usize) as i64;
            if self
                .write_pos
                .compare_exchange_weak(pos, aligned, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return aligned;
            }
        }
    }

    /// Returns the current write position.
    #[inline]
    pub fn position(&self) -> i64 {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Returns the buffer capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Returns the remaining capacity.
    #[inline]
    pub fn remaining(&self) -> usize {
        let pos = self.write_pos.load(Ordering::Acquire) as usize;
        self.buf.capacity().saturating_sub(pos)
    }

    /// Marks the slab as retired.
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    /// Returns true if the slab is retired.
    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    /// Converts to a SharedSlab for read-only access.
    pub fn as_shared(&self) -> SharedSlab {
        SharedSlab {
            buf: Arc::clone(&self.buf),
            index: Arc::clone(&self.index),
        }
    }

    /// Creates a flush ticket (reference to the buffer for flushing).
    /// Arc::clone safely increments ref count - no try_inc needed.
    pub fn flush_ticket(&self) -> FlushTicket {
        FlushTicket {
            buf: Arc::clone(&self.buf),
        }
    }
}

// =============================================================================
// FlushTicket
// =============================================================================

/// A ticket representing a reference to a slab buffer for flushing.
pub struct FlushTicket {
    buf: Arc<MmapBuffer>,
}

impl FlushTicket {
    /// Returns the buffer for writing to disk.
    pub fn buffer(&self) -> &Arc<MmapBuffer> {
        &self.buf
    }

    /// Redeems (releases) the ticket.
    /// The Arc is dropped when FlushTicket is dropped, releasing the reference.
    pub fn redeem(self) {
        // Arc dropped implicitly - no explicit unpin needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_entry_from_header() {
        let mut header = Header::new(123, 10, 100, 200);
        header.set_compression(Codec::Zstd);

        let entry = SlabEntry::from_header(&header, 1000);
        assert_eq!(entry.seq_id, 123);
        assert_eq!(entry.key_len, 10);
        assert_eq!(entry.physical_size, 100);
        assert_eq!(entry.logical_size, 200);
        assert_eq!(entry.pos, 1000);
        assert_eq!(entry.compression(), Codec::Zstd);
    }

    #[test]
    fn test_slab_index() {
        let index = SlabIndex::new();
        let key = Key::from_bytes(b"test");
        let entry = SlabEntry {
            seq_id: 1,
            pos: 100,
            ..Default::default()
        };

        assert!(index.get(&key).is_none());

        index.insert(key, entry);
        let retrieved = index.get(&key).unwrap();
        assert_eq!(retrieved.seq_id, 1);
        assert_eq!(retrieved.pos, 100);

        index.delete(&key);
        assert!(index.get(&key).is_none());
    }

    #[test]
    fn test_active_slab_alloc() {
        let buf = MmapBuffer::new(4096).unwrap();
        let slab = ActiveSlab::new(buf);

        // First allocation
        let (offset1, end1) = slab.alloc(100).unwrap();
        assert_eq!(offset1, 0);
        assert_eq!(end1, 100);

        // Second allocation
        let (offset2, end2) = slab.alloc(200).unwrap();
        assert_eq!(offset2, 100);
        assert_eq!(end2, 300);

        // Check position
        assert_eq!(slab.position(), 300);
    }

    #[test]
    fn test_active_slab_full() {
        let buf = MmapBuffer::new(4096).unwrap();
        let slab = ActiveSlab::new(buf);

        // Allocate most of the buffer
        slab.alloc(4000).unwrap();

        // Try to allocate more than remaining
        assert!(slab.alloc(1000).is_none());

        // Can still allocate what's left
        assert!(slab.alloc(96).is_some());
    }

    #[test]
    fn test_active_slab_align() {
        let buf = MmapBuffer::new(8192).unwrap();
        let slab = ActiveSlab::new(buf);

        slab.alloc(100).unwrap();
        assert_eq!(slab.position(), 100);

        let aligned = slab.align_to_page();
        assert_eq!(aligned, 4096);
        assert_eq!(slab.position(), 4096);
    }
}
