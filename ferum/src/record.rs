//! Unified binary record format for WAL and Segment files.
//!
//! # Record Layout v2 (42-byte header + variable payload)
//!
//! ```text
//! [Magic:4][HeaderCRC:4][Flags:8][SeqID:8][KeyLen:2][PhysicalSize:8][LogicalSize:8][Key][Value]
//! ```
//!
//! # Design Philosophy
//!
//! - **Header-First**: Magic at start enables single-seek reads and hole detection
//! - **HeaderCRC**: Protects against "allocate-on-corrupt-size" panics
//! - **Mandatory Key Verification**: Every Get() compares disk key with requested key
//! - **CRC in Flags**: Bits 31-0 contain CRC32 of Key+Value for integrity
//!
//! # Safety Invariant
//!
//! Readers MUST verify HeaderCRC before trusting PhysSize to allocate memory.

use crate::compression::Codec;
use crate::error::{BlobErrno, Error, Result};

// =============================================================================
// Constants
// =============================================================================

/// Fixed size of the record header in bytes (v2 format).
pub const HEADER_SIZE: usize = 42;

/// Magic number for valid records (0xB10BCAFE - "BlobCafe").
pub const RECORD_MAGIC: u32 = 0xB10B_CAFE;

/// Magic number identifying a hole (punched or padding).
pub const HOLE_MAGIC: u32 = 0x0000_0000;

/// Maximum key length (uint16 max).
pub const MAX_KEY_LEN: usize = 65535;

// Header field offsets
const OFF_MAGIC: usize = 0;
const OFF_HEADER_CRC: usize = 4;
const OFF_FLAGS: usize = 8;
const OFF_SEQ_ID: usize = 16;
const OFF_KEY_LEN: usize = 24;
const OFF_PHYSICAL_SIZE: usize = 26;
const OFF_LOGICAL_SIZE: usize = 34;

// HeaderCRC calculation range
const HEADER_CRC_START: usize = 8; // Skip Magic(4) + HeaderCRC(4)
const HEADER_CRC_LEN: usize = 34; // Flags(8) + SeqID(8) + KeyLen(2) + PhysicalSize(8) + LogicalSize(8)

// =============================================================================
// Flags Bit Layout
// =============================================================================

/// Compression type in bits 63-60 (4 bits, 16 values).
const FLAG_COMPRESSION_SHIFT: u32 = 60;
const FLAG_COMPRESSION_MASK: u64 = 0xF << FLAG_COMPRESSION_SHIFT;

/// BlobErrno in bits 38-34 (5 bits, 32 values).
const FLAG_ERRNO_SHIFT: u32 = 34;
const FLAG_ERRNO_MASK: u64 = 0x1F << FLAG_ERRNO_SHIFT;

/// Tombstone marker (bit 33).
const FLAG_DELETED: u64 = 1 << 33;

/// CRC not set or invalid (bit 32).
const FLAG_INVALID_CRC: u64 = 1 << 32;

/// CRC32 in bits 31-0.
const FLAG_CRC_MASK: u64 = 0xFFFF_FFFF;

// =============================================================================
// Header
// =============================================================================

/// Record header (42 bytes, v2 format).
///
/// Use [`Header::encode`] and [`Header::decode`] for serialization.
#[derive(Debug, Clone, Copy, Default)]
pub struct Header {
    /// Magic number: 0xB10BCAFE=valid, 0x00000000=hole
    pub magic: u32,
    /// CRC32 of bytes 8-41 (Flags through LogicalSize)
    pub header_crc: u32,
    /// Metadata, status, and CRC32 of payload
    pub flags: u64,
    /// Monotonic sequence ID
    pub seq_id: u64,
    /// Key length in bytes
    pub key_len: u16,
    /// Value length on disk (possibly compressed)
    pub physical_size: i64,
    /// Original uncompressed value length
    pub logical_size: i64,
}

impl Header {
    /// Creates a new valid header with the given parameters.
    pub fn new(seq_id: u64, key_len: u16, physical_size: i64, logical_size: i64) -> Self {
        Header {
            magic: RECORD_MAGIC,
            header_crc: 0, // Computed during encode
            flags: FLAG_INVALID_CRC,
            seq_id,
            key_len,
            physical_size,
            logical_size,
        }
    }

    /// Returns the total size of key + value.
    #[inline]
    pub fn payload_size(&self) -> usize {
        self.key_len as usize + self.physical_size as usize
    }

    /// Returns the total record size (header + payload).
    #[inline]
    pub fn total_size(&self) -> usize {
        HEADER_SIZE + self.payload_size()
    }

    /// Returns true if the magic byte indicates a valid record.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.magic == RECORD_MAGIC
    }

    /// Returns true if the magic byte indicates a hole.
    #[inline]
    pub fn is_hole(&self) -> bool {
        self.magic == HOLE_MAGIC
    }

    /// Returns true if the deleted flag is set.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        (self.flags & FLAG_DELETED) != 0
    }

    /// Sets the deleted flag.
    pub fn set_deleted(&mut self) {
        self.flags |= FLAG_DELETED;
    }

    /// Returns the CRC32 checksum from flags.
    #[inline]
    pub fn crc(&self) -> u32 {
        (self.flags & FLAG_CRC_MASK) as u32
    }

    /// Sets the CRC32 checksum in flags and clears InvalidCRC.
    pub fn set_crc(&mut self, crc: u32) {
        self.flags = (self.flags & !(FLAG_CRC_MASK | FLAG_INVALID_CRC)) | (crc as u64);
    }

    /// Returns true if CRC is set (InvalidCRC flag is clear).
    #[inline]
    pub fn has_valid_crc(&self) -> bool {
        (self.flags & FLAG_INVALID_CRC) == 0
    }

    /// Returns the compression codec from flags.
    #[inline]
    pub fn compression(&self) -> Codec {
        Codec::from_raw(((self.flags & FLAG_COMPRESSION_MASK) >> FLAG_COMPRESSION_SHIFT) as u8)
    }

    /// Sets the compression codec in flags.
    pub fn set_compression(&mut self, codec: Codec) {
        self.flags =
            (self.flags & !FLAG_COMPRESSION_MASK) | ((codec as u64) << FLAG_COMPRESSION_SHIFT);
    }

    /// Returns true if compression is enabled.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.compression() != Codec::None
    }

    /// Returns the error code from flags.
    #[inline]
    pub fn errno(&self) -> BlobErrno {
        BlobErrno::from_raw(((self.flags & FLAG_ERRNO_MASK) >> FLAG_ERRNO_SHIFT) as u8)
    }

    /// Sets the error code in flags.
    pub fn set_errno(&mut self, errno: BlobErrno) {
        self.flags = (self.flags & !FLAG_ERRNO_MASK) | (((errno as u64) & 0x1F) << FLAG_ERRNO_SHIFT);
    }

    /// Returns true if the record has a non-zero error code.
    #[inline]
    pub fn has_error(&self) -> bool {
        !self.errno().is_ok()
    }

    /// Encodes the header into dst (must be at least HEADER_SIZE bytes).
    /// The HeaderCRC is computed automatically over bytes 8-41.
    pub fn encode(&self, dst: &mut [u8]) -> Result<usize> {
        if dst.len() < HEADER_SIZE {
            return Err(Error::BufferTooSmall {
                needed: HEADER_SIZE,
                have: dst.len(),
            });
        }

        // Write Magic
        dst[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&self.magic.to_le_bytes());

        // Write header fields (bytes 8-41, covered by HeaderCRC)
        dst[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&self.flags.to_le_bytes());
        dst[OFF_SEQ_ID..OFF_SEQ_ID + 8].copy_from_slice(&self.seq_id.to_le_bytes());
        dst[OFF_KEY_LEN..OFF_KEY_LEN + 2].copy_from_slice(&self.key_len.to_le_bytes());
        dst[OFF_PHYSICAL_SIZE..OFF_PHYSICAL_SIZE + 8]
            .copy_from_slice(&(self.physical_size as u64).to_le_bytes());
        dst[OFF_LOGICAL_SIZE..OFF_LOGICAL_SIZE + 8]
            .copy_from_slice(&(self.logical_size as u64).to_le_bytes());

        // Compute and write HeaderCRC over bytes 8-41 (34 bytes)
        let header_crc =
            crc32fast::hash(&dst[HEADER_CRC_START..HEADER_CRC_START + HEADER_CRC_LEN]);
        dst[OFF_HEADER_CRC..OFF_HEADER_CRC + 4].copy_from_slice(&header_crc.to_le_bytes());

        Ok(HEADER_SIZE)
    }

    /// Decodes and verifies a header from src (must be at least HEADER_SIZE bytes).
    /// Returns ErrHeaderCRCMismatch if the HeaderCRC does not match.
    ///
    /// # Safety Invariant
    ///
    /// This verification MUST occur before trusting physical_size for memory allocation.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < HEADER_SIZE {
            return Err(Error::BufferTooSmall {
                needed: HEADER_SIZE,
                have: src.len(),
            });
        }

        // Verify HeaderCRC before trusting any size fields
        let stored_crc = u32::from_le_bytes(src[OFF_HEADER_CRC..OFF_HEADER_CRC + 4].try_into().unwrap());
        let computed_crc =
            crc32fast::hash(&src[HEADER_CRC_START..HEADER_CRC_START + HEADER_CRC_LEN]);

        if stored_crc != computed_crc {
            return Err(Error::HeaderCrcMismatch {
                expected: stored_crc,
                computed: computed_crc,
            });
        }

        Ok(Header {
            magic: u32::from_le_bytes(src[OFF_MAGIC..OFF_MAGIC + 4].try_into().unwrap()),
            header_crc: stored_crc,
            flags: u64::from_le_bytes(src[OFF_FLAGS..OFF_FLAGS + 8].try_into().unwrap()),
            seq_id: u64::from_le_bytes(src[OFF_SEQ_ID..OFF_SEQ_ID + 8].try_into().unwrap()),
            key_len: u16::from_le_bytes(src[OFF_KEY_LEN..OFF_KEY_LEN + 2].try_into().unwrap()),
            physical_size: u64::from_le_bytes(
                src[OFF_PHYSICAL_SIZE..OFF_PHYSICAL_SIZE + 8]
                    .try_into()
                    .unwrap(),
            ) as i64,
            logical_size: u64::from_le_bytes(
                src[OFF_LOGICAL_SIZE..OFF_LOGICAL_SIZE + 8]
                    .try_into()
                    .unwrap(),
            ) as i64,
        })
    }
}

// =============================================================================
// Record
// =============================================================================

/// Complete on-disk record: header + key + value.
#[derive(Debug, Clone)]
pub struct Record {
    /// The 42-byte header.
    pub header: Header,
    /// Original key bytes (hashed to 128-bit XXH3 for index lookup).
    pub key: Vec<u8>,
    /// Value bytes (possibly compressed; physical_size bytes on disk).
    pub value: Vec<u8>,
}

impl Record {
    /// Creates a new Record with header fields populated from key/value.
    /// logical_size is the original uncompressed value size.
    /// The CRC is computed over key+value and stored in the header.
    pub fn new(seq_id: u64, key: Vec<u8>, value: Vec<u8>, logical_size: i64) -> Self {
        let mut header = Header::new(
            seq_id,
            key.len() as u16,
            value.len() as i64,
            logical_size,
        );

        // Compute and set CRC
        let crc = compute_crc(&key, &value);
        header.set_crc(crc);

        Record { header, key, value }
    }

    /// Returns the total bytes needed to serialize this record.
    #[inline]
    pub fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.key.len() + self.value.len()
    }

    /// Encodes the full record (header + key + value) into dst.
    /// Returns the number of bytes written.
    pub fn encode(&self, dst: &mut [u8]) -> Result<usize> {
        let total_size = self.encoded_size();
        if dst.len() < total_size {
            return Err(Error::BufferTooSmall {
                needed: total_size,
                have: dst.len(),
            });
        }

        // Encode header
        self.header.encode(dst)?;

        // Copy key and value
        let key_end = HEADER_SIZE + self.key.len();
        dst[HEADER_SIZE..key_end].copy_from_slice(&self.key);
        dst[key_end..key_end + self.value.len()].copy_from_slice(&self.value);

        Ok(total_size)
    }

    /// Decodes a record from src.
    /// If verify_crc is true and the header has a valid CRC, validates the checksum.
    pub fn decode(src: &[u8], verify_crc: bool) -> Result<Self> {
        let header = Header::decode(src)?;

        if !header.is_valid() {
            if header.is_hole() {
                return Err(Error::Hole { offset: 0 });
            }
            return Err(Error::InvalidMagic {
                expected: RECORD_MAGIC,
                got: header.magic,
            });
        }

        let total_size = header.total_size();
        if src.len() < total_size {
            return Err(Error::BufferTooSmall {
                needed: total_size,
                have: src.len(),
            });
        }

        let key_start = HEADER_SIZE;
        let key_end = key_start + header.key_len as usize;
        let value_end = key_end + header.physical_size as usize;

        let key = src[key_start..key_end].to_vec();
        let value = src[key_end..value_end].to_vec();

        // Verify CRC if requested and header has valid CRC
        if verify_crc && header.has_valid_crc() {
            let expected = header.crc();
            let computed = compute_crc(&key, &value);
            if expected != computed {
                return Err(Error::PayloadCrcMismatch { expected, computed });
            }
        }

        Ok(Record { header, key, value })
    }
}

/// Computes CRC32 (IEEE) over key and value.
#[inline]
pub fn compute_crc(key: &[u8], value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    hasher.update(value);
    hasher.finalize()
}

/// Verifies CRC over key+value matches expected.
pub fn verify_crc(key: &[u8], value: &[u8], expected: u32) -> Result<()> {
    let computed = compute_crc(key, value);
    if computed != expected {
        return Err(Error::PayloadCrcMismatch { expected, computed });
    }
    Ok(())
}

// =============================================================================
// File Header
// =============================================================================

/// File header magic: "BLOB" in ASCII (0x424C4F42).
pub const FILE_MAGIC: u32 = 0x424C_4F42;

/// File format version.
pub const FILE_VERSION: u32 = 2;

/// Size of the file header in bytes (32 bytes to match WAL header size).
///
/// This ensures consistent segment format whether created via:
/// - flush_via_rename (WAL file becomes segment)
/// - flush_via_copy (new segment file written)
pub const FILE_HEADER_SIZE: usize = 32;

/// Segment file header structure (32 bytes).
///
/// Layout:
/// - magic: u32 (4 bytes) - "BLOB"
/// - version: u32 (4 bytes)
/// - flags: u32 (4 bytes) - reserved
/// - _padding: u32 (4 bytes)
/// - created_at: i64 (8 bytes) - creation timestamp (nanos since epoch)
/// - reserved: u64 (8 bytes)
#[derive(Debug, Clone, Copy, Default)]
pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub flags: u32,
    pub created_at: i64,
}

impl FileHeader {
    /// Creates a new header with current timestamp.
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        FileHeader {
            magic: FILE_MAGIC,
            version: FILE_VERSION,
            flags: 0,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        }
    }

    /// Encodes the header into a buffer.
    pub fn encode(&self, buf: &mut [u8]) {
        assert!(buf.len() >= FILE_HEADER_SIZE);
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.flags.to_le_bytes());
        buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // padding
        buf[16..24].copy_from_slice(&(self.created_at as u64).to_le_bytes());
        buf[24..32].copy_from_slice(&0u64.to_le_bytes()); // reserved
    }

    /// Decodes a header from a buffer.
    pub fn decode(buf: &[u8]) -> crate::error::Result<Self> {
        use crate::error::Error;

        if buf.len() < FILE_HEADER_SIZE {
            return Err(Error::BufferTooSmall {
                needed: FILE_HEADER_SIZE,
                have: buf.len(),
            });
        }

        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != FILE_MAGIC {
            return Err(Error::InvalidMagic {
                expected: FILE_MAGIC,
                got: magic,
            });
        }

        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != FILE_VERSION {
            return Err(Error::InvalidConfig {
                message: format!("unsupported segment version: {}", version),
            });
        }

        Ok(FileHeader {
            magic,
            version,
            flags: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            created_at: u64::from_le_bytes(buf[16..24].try_into().unwrap()) as i64,
        })
    }
}

/// Returns the file header bytes for segment files.
pub fn file_header_bytes() -> [u8; FILE_HEADER_SIZE] {
    let mut buf = [0u8; FILE_HEADER_SIZE];
    FileHeader::new().encode(&mut buf);
    buf
}

// =============================================================================
// Footer Entry
// =============================================================================

use crate::key::Key;

/// Entry in a segment footer file (.iseg).
///
/// Footer files provide crash recovery by storing a snapshot of index data
/// for each segment.
#[derive(Debug, Clone, Default)]
pub struct FooterEntry {
    /// The 128-bit XXH3 hash of the key.
    pub key: Key,
    /// Flags from record header.
    pub flags: u64,
    /// Sequence ID of this write.
    pub seq_id: u64,
    /// Key length in bytes.
    pub key_len: u16,
    /// Physical size on disk (possibly compressed).
    pub physical_size: i64,
    /// Logical (uncompressed) size.
    pub logical_size: i64,
    /// Byte offset within segment file.
    pub pos: i64,
}

impl FooterEntry {
    /// Returns the compression codec from flags.
    pub fn compression(&self) -> Codec {
        Codec::from_raw(((self.flags & 0xF000_0000_0000_0000) >> 60) as u8)
    }

    /// Returns true if the deleted flag is set (tombstone).
    #[inline]
    pub fn is_deleted(&self) -> bool {
        (self.flags & FLAG_DELETED) != 0
    }
}

/// Size of each footer entry when encoded (58 bytes).
pub const FOOTER_ENTRY_SIZE: usize = 16 + 8 + 8 + 2 + 8 + 8 + 8; // key(16) + flags(8) + seq(8) + keylen(2) + phys(8) + log(8) + pos(8)

/// Encodes footer entries for persistence.
pub fn encode_footer(entries: &[FooterEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * FOOTER_ENTRY_SIZE + 4);

    // Write entry count
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Write each entry
    for entry in entries {
        buf.extend_from_slice(&entry.key.to_bytes());
        buf.extend_from_slice(&entry.flags.to_le_bytes());
        buf.extend_from_slice(&entry.seq_id.to_le_bytes());
        buf.extend_from_slice(&entry.key_len.to_le_bytes());
        buf.extend_from_slice(&(entry.physical_size as u64).to_le_bytes());
        buf.extend_from_slice(&(entry.logical_size as u64).to_le_bytes());
        buf.extend_from_slice(&(entry.pos as u64).to_le_bytes());
    }

    buf
}

/// Decodes footer entries from a buffer.
pub fn decode_footer(src: &[u8]) -> Result<Vec<FooterEntry>> {
    if src.len() < 4 {
        return Err(Error::BufferTooSmall { needed: 4, have: src.len() });
    }

    let count = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
    let expected_len = 4 + count * FOOTER_ENTRY_SIZE;

    if src.len() < expected_len {
        return Err(Error::BufferTooSmall { needed: expected_len, have: src.len() });
    }

    let mut entries = Vec::with_capacity(count);
    let mut offset = 4;

    for _ in 0..count {
        // Load raw 16-byte hash (NOT hash the bytes - they're already a hash!)
        let key_bytes: [u8; 16] = src[offset..offset + 16].try_into().unwrap();
        let key = Key::decode(&key_bytes);
        offset += 16;

        let flags = u64::from_le_bytes(src[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let seq_id = u64::from_le_bytes(src[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let key_len = u16::from_le_bytes(src[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let physical_size = u64::from_le_bytes(src[offset..offset + 8].try_into().unwrap()) as i64;
        offset += 8;

        let logical_size = u64::from_le_bytes(src[offset..offset + 8].try_into().unwrap()) as i64;
        offset += 8;

        let pos = u64::from_le_bytes(src[offset..offset + 8].try_into().unwrap()) as i64;
        offset += 8;

        entries.push(FooterEntry {
            key,
            flags,
            seq_id,
            key_len,
            physical_size,
            logical_size,
            pos,
        });
    }

    Ok(entries)
}

// =============================================================================
// Segment Footer (for crash recovery)
// =============================================================================

/// Magic number for segment tail.
pub const SEGMENT_TAIL_MAGIC: u64 = 0xB10BCA4E_B10BCA4E;

/// Size of segment tail (DataLen:8 + Checksum:4 + Magic:8 = 20 bytes).
pub const SEGMENT_TAIL_SIZE: usize = 20;

/// Segment footer static header size (Version:4 + Pad:4 + SegmentID:8 + CTime:8 + MinSeqID:8 + MaxSeqID:8 + RecordCount:8).
pub const SEGMENT_FOOTER_STATIC_SIZE: usize = 48;

/// Footer entry size in Go format (64 bytes).
pub const GO_FOOTER_ENTRY_SIZE: usize = 64;

/// Segment footer containing index of all entries in a segment.
/// Written at segment close for O(1) crash recovery.
#[derive(Debug, Clone, Default)]
pub struct SegmentFooter {
    /// Format version.
    pub version: u32,
    /// Segment ID.
    pub segment_id: u32,
    /// Creation timestamp (unix seconds).
    pub ctime: i64,
    /// Minimum sequence ID in this segment.
    pub min_seq_id: u64,
    /// Maximum sequence ID in this segment.
    pub max_seq_id: u64,
    /// Entries in this segment.
    pub entries: Vec<FooterEntry>,
}

/// Segment tail - the last 20 bytes of a segment file.
#[derive(Debug, Clone, Default)]
pub struct SegmentTail {
    /// Length of the segment footer data (not including padding).
    pub data_len: i64,
    /// CRC32 of the segment footer data.
    pub checksum: u32,
}

impl SegmentFooter {
    /// Returns the exact byte size needed to encode this footer.
    pub fn data_size(&self) -> usize {
        SEGMENT_FOOTER_STATIC_SIZE + self.entries.len() * GO_FOOTER_ENTRY_SIZE
    }

    /// Encodes the segment footer to a buffer.
    pub fn encode(&self, dst: &mut [u8]) -> Result<usize> {
        let data_size = self.data_size();
        if dst.len() < data_size {
            return Err(Error::BufferTooSmall { needed: data_size, have: dst.len() });
        }

        // Version (4 bytes)
        dst[0..4].copy_from_slice(&1u32.to_le_bytes());
        // Padding (4 bytes)
        dst[4..8].fill(0);
        // SegmentID (8 bytes)
        dst[8..16].copy_from_slice(&(self.segment_id as u64).to_le_bytes());
        // CTime (8 bytes)
        dst[16..24].copy_from_slice(&(self.ctime as u64).to_le_bytes());
        // MinSeqID (8 bytes)
        dst[24..32].copy_from_slice(&self.min_seq_id.to_le_bytes());
        // MaxSeqID (8 bytes)
        dst[32..40].copy_from_slice(&self.max_seq_id.to_le_bytes());
        // RecordCount (8 bytes)
        dst[40..48].copy_from_slice(&(self.entries.len() as u64).to_le_bytes());

        // Entries
        let mut offset = SEGMENT_FOOTER_STATIC_SIZE;
        for entry in &self.entries {
            encode_go_footer_entry(&mut dst[offset..offset + GO_FOOTER_ENTRY_SIZE], entry);
            offset += GO_FOOTER_ENTRY_SIZE;
        }

        Ok(data_size)
    }

    /// Decodes a segment footer from a buffer.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < SEGMENT_FOOTER_STATIC_SIZE {
            return Err(Error::BufferTooSmall {
                needed: SEGMENT_FOOTER_STATIC_SIZE,
                have: src.len(),
            });
        }

        let version = u32::from_le_bytes(src[0..4].try_into().unwrap());
        // Skip padding bytes 4-7
        let segment_id = u64::from_le_bytes(src[8..16].try_into().unwrap()) as u32;
        let ctime = u64::from_le_bytes(src[16..24].try_into().unwrap()) as i64;
        let min_seq_id = u64::from_le_bytes(src[24..32].try_into().unwrap());
        let max_seq_id = u64::from_le_bytes(src[32..40].try_into().unwrap());
        let record_count = u64::from_le_bytes(src[40..48].try_into().unwrap()) as usize;

        let entries_data = &src[SEGMENT_FOOTER_STATIC_SIZE..];
        let expected_entries_size = record_count * GO_FOOTER_ENTRY_SIZE;

        if entries_data.len() < expected_entries_size {
            return Err(Error::BufferTooSmall {
                needed: SEGMENT_FOOTER_STATIC_SIZE + expected_entries_size,
                have: src.len(),
            });
        }

        let mut entries = Vec::with_capacity(record_count);
        for i in 0..record_count {
            let offset = i * GO_FOOTER_ENTRY_SIZE;
            let entry = decode_go_footer_entry(&entries_data[offset..offset + GO_FOOTER_ENTRY_SIZE])?;
            entries.push(entry);
        }

        Ok(SegmentFooter {
            version,
            segment_id,
            ctime,
            min_seq_id,
            max_seq_id,
            entries,
        })
    }
}

/// Encodes a footer entry in Go's 64-byte format.
/// Wire format: Key.Lo(8) + Key.Hi(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8) + KeyLen(2) + Pad(6)
fn encode_go_footer_entry(dst: &mut [u8], entry: &FooterEntry) {
    dst[0..8].copy_from_slice(&entry.key.lo().to_le_bytes());
    dst[8..16].copy_from_slice(&entry.key.hi().to_le_bytes());
    dst[16..24].copy_from_slice(&(entry.pos as u64).to_le_bytes());
    dst[24..32].copy_from_slice(&(entry.logical_size as u64).to_le_bytes());
    dst[32..40].copy_from_slice(&(entry.physical_size as u64).to_le_bytes());
    dst[40..48].copy_from_slice(&entry.seq_id.to_le_bytes());
    dst[48..56].copy_from_slice(&entry.flags.to_le_bytes());
    dst[56..58].copy_from_slice(&entry.key_len.to_le_bytes());
    dst[58..64].fill(0); // Padding
}

/// Decodes a footer entry from Go's 64-byte format.
fn decode_go_footer_entry(src: &[u8]) -> Result<FooterEntry> {
    if src.len() < GO_FOOTER_ENTRY_SIZE {
        return Err(Error::BufferTooSmall { needed: GO_FOOTER_ENTRY_SIZE, have: src.len() });
    }

    let lo = u64::from_le_bytes(src[0..8].try_into().unwrap());
    let hi = u64::from_le_bytes(src[8..16].try_into().unwrap());
    let pos = u64::from_le_bytes(src[16..24].try_into().unwrap()) as i64;
    let logical_size = u64::from_le_bytes(src[24..32].try_into().unwrap()) as i64;
    let physical_size = u64::from_le_bytes(src[32..40].try_into().unwrap()) as i64;
    let seq_id = u64::from_le_bytes(src[40..48].try_into().unwrap());
    let flags = u64::from_le_bytes(src[48..56].try_into().unwrap());
    let key_len = u16::from_le_bytes(src[56..58].try_into().unwrap());

    Ok(FooterEntry {
        key: Key::from_parts(hi, lo),
        pos,
        logical_size,
        physical_size,
        seq_id,
        flags,
        key_len,
    })
}

impl SegmentTail {
    /// Decodes a segment tail from the last 20 bytes.
    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < SEGMENT_TAIL_SIZE {
            return Err(Error::BufferTooSmall { needed: SEGMENT_TAIL_SIZE, have: src.len() });
        }

        let data_len = u64::from_le_bytes(src[0..8].try_into().unwrap()) as i64;
        let checksum = u32::from_le_bytes(src[8..12].try_into().unwrap());
        let magic = u64::from_le_bytes(src[12..20].try_into().unwrap());

        if magic != SEGMENT_TAIL_MAGIC {
            return Err(Error::InvalidMagic {
                expected: SEGMENT_TAIL_MAGIC as u32,
                got: (magic & 0xFFFFFFFF) as u32,
            });
        }

        Ok(SegmentTail { data_len, checksum })
    }

    /// Encodes a segment tail.
    pub fn encode(&self, dst: &mut [u8]) -> Result<usize> {
        if dst.len() < SEGMENT_TAIL_SIZE {
            return Err(Error::BufferTooSmall { needed: SEGMENT_TAIL_SIZE, have: dst.len() });
        }

        dst[0..8].copy_from_slice(&(self.data_len as u64).to_le_bytes());
        dst[8..12].copy_from_slice(&self.checksum.to_le_bytes());
        dst[12..20].copy_from_slice(&SEGMENT_TAIL_MAGIC.to_le_bytes());

        Ok(SEGMENT_TAIL_SIZE)
    }
}

/// Reads and validates a segment footer from a file.
/// Returns the footer and the byte offset where the footer block starts.
pub fn read_segment_footer<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    file_size: u64,
    expected_segment_id: Option<u32>,
) -> Result<(SegmentFooter, u64)> {
    use std::io::SeekFrom;

    if file_size < SEGMENT_TAIL_SIZE as u64 {
        return Err(Error::Corruption {
            message: "file too small for segment tail".to_string(),
        });
    }

    // 1. Read tail from end
    let tail_pos = file_size - SEGMENT_TAIL_SIZE as u64;
    reader.seek(SeekFrom::Start(tail_pos)).map_err(|e| Error::io("seek to tail", e))?;

    let mut tail_buf = [0u8; SEGMENT_TAIL_SIZE];
    reader.read_exact(&mut tail_buf).map_err(|e| Error::io("read tail", e))?;

    let tail = SegmentTail::decode(&tail_buf)?;

    // 2. Calculate footer block start (4KB aligned)
    let physical_size = round_to_page(tail.data_len as u64 + SEGMENT_TAIL_SIZE as u64);
    let footer_block_start = file_size - physical_size;

    // 3. Read footer data
    reader.seek(SeekFrom::Start(footer_block_start)).map_err(|e| Error::io("seek to footer", e))?;

    let mut footer_buf = vec![0u8; tail.data_len as usize];
    reader.read_exact(&mut footer_buf).map_err(|e| Error::io("read footer", e))?;

    // 4. Validate checksum
    let computed_checksum = crc32fast::hash(&footer_buf);
    if computed_checksum != tail.checksum {
        return Err(Error::Corruption {
            message: format!(
                "segment footer checksum mismatch: expected {:08x}, got {:08x}",
                tail.checksum, computed_checksum
            ),
        });
    }

    // 5. Decode footer
    let footer = SegmentFooter::decode(&footer_buf)?;

    // 6. Validate segment ID if provided
    if let Some(expected) = expected_segment_id {
        if footer.segment_id != expected {
            return Err(Error::Corruption {
                message: format!(
                    "segment ID mismatch: expected {}, got {}",
                    expected, footer.segment_id
                ),
            });
        }
    }

    Ok((footer, footer_block_start))
}

/// Rounds size up to 4KB page boundary.
fn round_to_page(size: u64) -> u64 {
    const PAGE_SIZE: u64 = 4096;
    (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encode_decode() {
        let mut header = Header::new(12345, 10, 100, 200);
        header.set_compression(Codec::Zstd);
        header.set_crc(0xDEADBEEF);

        let mut buf = [0u8; HEADER_SIZE];
        header.encode(&mut buf).unwrap();

        let decoded = Header::decode(&buf).unwrap();
        assert_eq!(decoded.magic, RECORD_MAGIC);
        assert_eq!(decoded.seq_id, 12345);
        assert_eq!(decoded.key_len, 10);
        assert_eq!(decoded.physical_size, 100);
        assert_eq!(decoded.logical_size, 200);
        assert_eq!(decoded.compression(), Codec::Zstd);
        assert_eq!(decoded.crc(), 0xDEADBEEF);
        assert!(decoded.has_valid_crc());
    }

    #[test]
    fn test_header_crc_mismatch() {
        let header = Header::new(1, 10, 100, 100);
        let mut buf = [0u8; HEADER_SIZE];
        header.encode(&mut buf).unwrap();

        // Corrupt a byte in the CRC-protected region
        buf[OFF_FLAGS] ^= 0xFF;

        let result = Header::decode(&buf);
        assert!(matches!(result, Err(Error::HeaderCrcMismatch { .. })));
    }

    #[test]
    fn test_record_encode_decode() {
        let record = Record::new(1, b"test-key".to_vec(), b"test-value".to_vec(), 10);

        let mut buf = vec![0u8; record.encoded_size()];
        record.encode(&mut buf).unwrap();

        let decoded = Record::decode(&buf, true).unwrap();
        assert_eq!(decoded.key, b"test-key");
        assert_eq!(decoded.value, b"test-value");
        assert_eq!(decoded.header.seq_id, 1);
    }

    #[test]
    fn test_record_crc_verification() {
        let record = Record::new(1, b"key".to_vec(), b"value".to_vec(), 5);

        let mut buf = vec![0u8; record.encoded_size()];
        record.encode(&mut buf).unwrap();

        // Corrupt the value
        let value_start = HEADER_SIZE + 3; // After "key"
        buf[value_start] ^= 0xFF;

        let result = Record::decode(&buf, true);
        assert!(matches!(result, Err(Error::PayloadCrcMismatch { .. })));
    }

    #[test]
    fn test_header_flags() {
        let mut header = Header::default();

        // Test deleted flag
        assert!(!header.is_deleted());
        header.set_deleted();
        assert!(header.is_deleted());

        // Test compression
        header.set_compression(Codec::Lz4);
        assert_eq!(header.compression(), Codec::Lz4);
        assert!(header.is_compressed());

        // Test errno
        header.set_errno(BlobErrno::IoRead);
        assert_eq!(header.errno(), BlobErrno::IoRead);
        assert!(header.has_error());
    }

    #[test]
    fn test_hole_detection() {
        let buf = [0u8; HEADER_SIZE];
        // All zeros = hole magic
        let result = Header::decode(&buf);
        // Should fail CRC check since all-zero CRC won't match computed CRC
        assert!(result.is_err());
    }

    #[test]
    fn test_header_sizes() {
        let header = Header::new(0, 10, 100, 100);
        assert_eq!(header.payload_size(), 110);
        assert_eq!(header.total_size(), HEADER_SIZE + 110);
    }
}
