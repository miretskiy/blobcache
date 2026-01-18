package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
	"github.com/zeebo/xxh3"
)

// Key is the 128-bit XXH3 hash of a blob key.
type Key = xxh3.Uint128

// File header constants.
// All blobcache data files (segments, WAL) start with a common file header,
// making them self-describing units. This enables:
// - Resilience: scan for magic to re-sync after corruption
// - Unified format: WAL files can be renamed to segments directly
const (
	// FileHeaderSize is the size of the file header (magic + version).
	FileHeaderSize = 8

	// FileMagic identifies a blobcache data file.
	FileMagic uint32 = 0xB10BC453

	// FileVersion is the current file format version.
	FileVersion uint32 = 0x00000001
)

// FileHeaderBytes is the fixed 8-byte file header (magic + version).
var FileHeaderBytes = [FileHeaderSize]byte{
	0x53, 0xC4, 0x0B, 0xB1, // FileMagic (little-endian)
	0x01, 0x00, 0x00, 0x00, // FileVersion (little-endian)
}

// Segment file format definitions.
//
// Structure:
//
//	[ BLOCK 1: Header(8B) + Records... ] [ BLOCK 2: Header(8B) + Records... ] ... [ FOOTER ]
//
// Each block starts with a header (magic + version). The footer is written
// only on clean close and enables O(1) index rebuild. If missing (crash),
// recovery falls back to linear record scanning.

// File header errors.
var (
	ErrInvalidFileMagic = errors.New("record: invalid file magic")
	ErrInvalidVersion   = errors.New("record: unsupported version")
)

// ValidateFileHeader checks if src starts with a valid file header.
func ValidateFileHeader(src []byte) error {
	if len(src) < FileHeaderSize {
		return ErrBufferTooSmall
	}
	magic := binary.LittleEndian.Uint32(src[0:4])
	version := binary.LittleEndian.Uint32(src[4:8])

	if magic != FileMagic {
		return ErrInvalidFileMagic
	}
	if version != FileVersion {
		return ErrInvalidVersion
	}
	return nil
}

// =============================================================================
// Segment Footer Types (replaces metadata.BlobRecord/SegmentRecord)
// =============================================================================
//
// These types are used for:
// 1. Segment footer serialization (written when segment closes)
// 2. Persistent index storage in bitcask (segmentID -> SegmentFooter)
// 3. Recovery (rebuilding index from segment footers)
//
// TODO(future): Consider removing FooterEntry in favor of scanning record.Header
// directly from segment files. This would eliminate the need for a separate
// footer format and simplify the codebase.
//
// TODO(future): Add min/max SeqID to SegmentFooter for efficient range queries
// and compaction decisions.

// Envelope serialization constants.
const (
	// FooterEntrySize is the encoded size of a single FooterEntry (64 bytes).
	// Wire format: Key.Lo(8) + Key.Hi(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8) + KeyLen(2) + Pad(6)
	FooterEntrySize = 64

	// FooterStaticSize is the header before entries (48 bytes).
	// Wire format: Version(4) + Pad(4) + SegmentID(8) + CTime(8) + MinSeqID(8) + MaxSeqID(8) + RecordCount(8)
	FooterStaticSize = 48

	// TailSize is the 20-byte tail at the end of segment files.
	// Wire format: EnvelopeDataLen(8) + Checksum(4) + Magic(8)
	TailSize = 20

	// TailMagic identifies a valid segment tail.
	TailMagic = 0xB10BCA4EB10BCA4E

	// FooterVersion is the current segment footer format version.
	FooterVersion = 1
)

// FooterEntry represents a single blob's location and metadata within a segment.
// Like a Unix inode, it indexes content by its hash rather than by name.
//
// Used in segment file envelopes for O(1) index recovery on startup.
// Index recovery is a DR operation -- needed only if the index is corrupt.
// The in-memory index uses the leaner index.Item (32 bytes) for hot paths.
type FooterEntry struct {
	Key          Key    // 128-bit XXH3 content hash
	Pos          int64  // Byte offset within segment (to record header)
	LogicalSize  int64  // Original uncompressed value size
	PhysicalSize int64  // On-disk size of value (possibly compressed)
	SeqID        uint64 // Monotonic sequence ID for ordering
	Flags        uint64 // Compression, status, checksum (same layout as Header.Flags)
	KeyLen       uint16 // Key length in bytes (for computing total record size)
}

// SegmentFooter is the index manifest for a single segment file.
// Written at segment close, it enables O(1) index reconstruction on recovery.
type SegmentFooter struct {
	Version     uint32 // Footer format version (for forward compatibility)
	SegmentID   int64
	CTime       int64  // Unix timestamp (seconds)
	MinSeqID    uint64 // Lowest SeqID in this segment (compaction decisions)
	MaxSeqID    uint64 // Highest SeqID in this segment (WAL recovery checkpoint)
	RecordCount int64  // Number of records (validation: must match len(Entries))
	Entries     []FooterEntry

	// IndexKey is the persistence layer key (not serialized to segment files).
	// Populated when reading from persistent index, nil for new envelopes.
	IndexKey []byte
}

// SegmentTail is the fixed 20-byte structure at the end of every segment file.
// It provides the offset and checksum needed to locate the SegmentFooter.
type SegmentTail struct {
	DataLen  int64  // Length of the SegmentFooter data (not including padding)
	Checksum uint32 // CRC32 of the SegmentFooter data
}

// --- FooterEntry Methods ---
// Flag accessors mirror record.Header for consistency across the codebase.

// Compression returns the compression codec from flags.
func (e *FooterEntry) Compression() compression.Codex {
	return compression.Codex((e.Flags & FlagCompressionMask) >> FlagCompressionShift)
}

// SetCompression sets the compression codec in flags.
func (e *FooterEntry) SetCompression(c compression.Codex) {
	e.Flags = (e.Flags &^ FlagCompressionMask) | (uint64(c) << FlagCompressionShift)
}

// IsCompressed returns true if compression is enabled.
func (e *FooterEntry) IsCompressed() bool {
	return e.Compression() != compression.CodexNone
}

// IsDeleted returns true if the deleted flag is set.
func (e *FooterEntry) IsDeleted() bool {
	return (e.Flags & FlagDeleted) != 0
}

// SetDeleted sets the deleted flag.
func (e *FooterEntry) SetDeleted() {
	e.Flags |= FlagDeleted
}

// Checksum returns the CRC32 checksum from flags.
func (e *FooterEntry) Checksum() uint32 {
	return uint32(e.Flags & FlagCRCMask)
}

// HasChecksum returns true if checksum is valid (InvalidCRC flag is clear).
func (e *FooterEntry) HasChecksum() bool {
	return (e.Flags & FlagInvalidCRC) == 0
}

// Errno returns the error code from flags.
func (e *FooterEntry) Errno() base.BlobErrno {
	return base.BlobErrno((e.Flags & FlagErrnoMask) >> FlagErrnoShift)
}

// SetErrno sets the error code in flags.
func (e *FooterEntry) SetErrno(errno base.BlobErrno) {
	e.Flags = (e.Flags &^ FlagErrnoMask) | (uint64(errno&0x1F) << FlagErrnoShift)
}

// HasError returns true if the entry has a non-zero error code.
func (e *FooterEntry) HasError() bool {
	return e.Errno() != base.ErrNone
}

// CompressionRatio returns physical/logical size ratio.
func (e *FooterEntry) CompressionRatio() float64 {
	if e.LogicalSize == 0 {
		return 0
	}
	return float64(e.PhysicalSize) / float64(e.LogicalSize)
}

// --- FooterEntry Serialization ---

// EncodeFooterEntry writes an encoded FooterEntry to dst.
// dst must be at least FooterEntrySize (64) bytes.
// Wire format: Key.Lo(8) + Key.Hi(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8) + KeyLen(2) + Pad(6)
func EncodeFooterEntry(dst []byte, e FooterEntry) {
	_ = dst[FooterEntrySize-1] // Bounds check hint
	binary.LittleEndian.PutUint64(dst[0:8], e.Key.Lo)
	binary.LittleEndian.PutUint64(dst[8:16], e.Key.Hi)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(e.Pos))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(e.LogicalSize))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(e.PhysicalSize))
	binary.LittleEndian.PutUint64(dst[40:48], e.SeqID)
	binary.LittleEndian.PutUint64(dst[48:56], e.Flags)
	binary.LittleEndian.PutUint16(dst[56:58], e.KeyLen)
	// Bytes 58-63 are padding (already zeroed by caller)
}

// AppendInode appends an encoded FooterEntry to dst.
// Convenience wrapper around EncodeFooterEntry for tests.
func AppendInode(dst []byte, e FooterEntry) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, FooterEntrySize)...)
	EncodeFooterEntry(dst[start:], e)
	return dst
}

// DecodeInode decodes an FooterEntry from src.
func DecodeInode(src []byte) (FooterEntry, error) {
	if len(src) < FooterEntrySize {
		return FooterEntry{}, ErrBufferTooSmall
	}
	return FooterEntry{
		Key: Key{
			Lo: binary.LittleEndian.Uint64(src[0:8]),
			Hi: binary.LittleEndian.Uint64(src[8:16]),
		},
		Pos:          int64(binary.LittleEndian.Uint64(src[16:24])),
		LogicalSize:  int64(binary.LittleEndian.Uint64(src[24:32])),
		PhysicalSize: int64(binary.LittleEndian.Uint64(src[32:40])),
		SeqID:        binary.LittleEndian.Uint64(src[40:48]),
		Flags:        binary.LittleEndian.Uint64(src[48:56]),
		KeyLen:       binary.LittleEndian.Uint16(src[56:58]),
		// Bytes 58-63 are padding, ignored on decode
	}, nil
}

// --- SegmentFooter Serialization ---

// SegmentFooterDataSize returns the exact byte size needed to encode a SegmentFooter.
// This is the logical size (not page-aligned).
func SegmentFooterDataSize(numEntries int) int {
	return FooterStaticSize + (numEntries * FooterEntrySize)
}

// EncodeSegmentFooter writes an encoded SegmentFooter to dst.
// dst must be at least SegmentFooterDataSize(len(sf.Entries)) bytes.
// Wire format: Version(4) + Pad(4) + SegmentID(8) + CTime(8) + MinSeqID(8) + MaxSeqID(8) + RecordCount(8) + [Entries...]
func EncodeSegmentFooter(dst []byte, sf SegmentFooter) {
	dataSize := SegmentFooterDataSize(len(sf.Entries))
	_ = dst[dataSize-1] // Bounds check hint

	// Always write the current version when encoding
	binary.LittleEndian.PutUint32(dst[0:4], FooterVersion)
	// Bytes 4-7 are padding (already zeroed by caller)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(sf.SegmentID))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(sf.CTime))
	binary.LittleEndian.PutUint64(dst[24:32], sf.MinSeqID)
	binary.LittleEndian.PutUint64(dst[32:40], sf.MaxSeqID)
	binary.LittleEndian.PutUint64(dst[40:48], uint64(len(sf.Entries))) // RecordCount

	offset := FooterStaticSize
	for i := range sf.Entries {
		EncodeFooterEntry(dst[offset:], sf.Entries[i])
		offset += FooterEntrySize
	}
}

// AppendSegmentFooter appends an encoded SegmentFooter to dst.
// Convenience wrapper around EncodeSegmentFooter for tests.
func AppendSegmentFooter(dst []byte, sf SegmentFooter) []byte {
	start := len(dst)
	dataSize := SegmentFooterDataSize(len(sf.Entries))
	dst = append(dst, make([]byte, dataSize)...)
	EncodeSegmentFooter(dst[start:], sf)
	return dst
}

// DecodeSegmentFooter decodes a SegmentFooter from src.
func DecodeSegmentFooter(src []byte) (SegmentFooter, error) {
	if len(src) < FooterStaticSize {
		return SegmentFooter{}, ErrBufferTooSmall
	}

	// Read and validate version
	version := binary.LittleEndian.Uint32(src[0:4])
	if version > FooterVersion {
		return SegmentFooter{}, fmt.Errorf("%w: footer version %d > supported %d", ErrInvalidVersion, version, FooterVersion)
	}

	// Bytes 4-7 are padding, skip
	segmentID := int64(binary.LittleEndian.Uint64(src[8:16]))
	ctime := int64(binary.LittleEndian.Uint64(src[16:24]))
	minSeqID := binary.LittleEndian.Uint64(src[24:32])
	maxSeqID := binary.LittleEndian.Uint64(src[32:40])
	recordCount := int64(binary.LittleEndian.Uint64(src[40:48]))

	entriesData := src[FooterStaticSize:]
	if len(entriesData)%FooterEntrySize != 0 {
		return SegmentFooter{}, errors.New("record: invalid segment envelope size")
	}

	numEntries := len(entriesData) / FooterEntrySize
	if int64(numEntries) != recordCount {
		return SegmentFooter{}, fmt.Errorf("record: record count mismatch: header=%d, actual=%d", recordCount, numEntries)
	}

	entries := make([]FooterEntry, numEntries)
	for i := 0; i < numEntries; i++ {
		offset := i * FooterEntrySize
		e, err := DecodeInode(entriesData[offset : offset+FooterEntrySize])
		if err != nil {
			return SegmentFooter{}, err
		}
		entries[i] = e
	}

	return SegmentFooter{
		Version:     version,
		SegmentID:   segmentID,
		CTime:       ctime,
		MinSeqID:    minSeqID,
		MaxSeqID:    maxSeqID,
		RecordCount: recordCount,
		Entries:     entries,
	}, nil
}

// --- Segment Tail ---
// These functions handle the 20-byte tail at the end of segment files.

// DecodeSegmentTail decodes the 20-byte tail from segment file end.
func DecodeSegmentTail(src []byte) (SegmentTail, error) {
	if len(src) < TailSize {
		return SegmentTail{}, ErrBufferTooSmall
	}

	dataLen := int64(binary.LittleEndian.Uint64(src[0:8]))
	checksum := binary.LittleEndian.Uint32(src[8:12])
	magic := binary.LittleEndian.Uint64(src[12:20])

	if magic != TailMagic {
		return SegmentTail{}, errors.New("record: invalid segment tail magic")
	}

	return SegmentTail{
		DataLen:  dataLen,
		Checksum: checksum,
	}, nil
}

// EncodeSegmentTail writes the 20-byte tail to dst.
// dst must be at least TailSize (20) bytes.
func EncodeSegmentTail(dst []byte, tail SegmentTail) {
	_ = dst[TailSize-1] // Bounds check hint
	binary.LittleEndian.PutUint64(dst[0:8], uint64(tail.DataLen))
	binary.LittleEndian.PutUint32(dst[8:12], tail.Checksum)
	binary.LittleEndian.PutUint64(dst[12:20], TailMagic)
}

// AppendSegmentTail appends the 20-byte tail.
// Convenience wrapper around EncodeSegmentTail.
func AppendSegmentTail(dst []byte, tail SegmentTail) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, TailSize)...)
	EncodeSegmentTail(dst[start:], tail)
	return dst
}

// --- Helper Functions ---

// SegmentFooterAlignedSize returns the 4KB-aligned size needed for a segment envelope.
func SegmentFooterAlignedSize(numEntries int) int64 {
	logicalSize := int64(FooterStaticSize + (numEntries * FooterEntrySize) + TailSize)
	return roundToPage(logicalSize)
}

// AppendFooterBlock serializes a SegmentFooter with CRC and tail into a
// 4KB-aligned buffer. Structure: [SegmentFooter][Alignment Gap (Zeros)][Tail]
func AppendFooterBlock(buf []byte, sf SegmentFooter) []byte {
	envelopeDataSize := SegmentFooterDataSize(len(sf.Entries))
	physicalSize := int(roundToPage(int64(envelopeDataSize + TailSize)))

	// 1. Prepare the buffer with full physical size.
	if cap(buf) < physicalSize {
		buf = make([]byte, physicalSize)
	} else {
		buf = buf[:physicalSize]
		clear(buf)
	}

	// 2. Encode footer directly at the start (gap is already zeroed).
	EncodeSegmentFooter(buf, sf)

	// 3. Compute checksum of the envelope data ONLY.
	checksum := crc32.ChecksumIEEE(buf[:envelopeDataSize])

	// 4. Encode tail at the VERY END of the physical block.
	tailOffset := physicalSize - TailSize
	EncodeSegmentTail(buf[tailOffset:], SegmentTail{
		DataLen:  int64(envelopeDataSize),
		Checksum: checksum,
	})

	return buf
}

func roundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) &^ (pageSize - 1)
}

// ReadFooterBlock reads and validates a segment envelope from file.
// Returns the SegmentFooter, the envelope block start offset, and any error.
func ReadFooterBlock(
	file interface {
		ReadAt([]byte, int64) (int, error)
	},
	fileSize int64,
	segmentID int64,
) (SegmentFooter, int64, error) {
	if fileSize < int64(TailSize) {
		return SegmentFooter{}, 0, errors.New("record: file too small for tail")
	}

	// 1. Read tail from the end
	tailBuf := make([]byte, TailSize)
	tailPos := fileSize - int64(TailSize)
	if _, err := file.ReadAt(tailBuf, tailPos); err != nil {
		return SegmentFooter{}, 0, errors.New("record: failed to read tail: " + err.Error())
	}

	tail, err := DecodeSegmentTail(tailBuf)
	if err != nil {
		return SegmentFooter{}, 0, errors.New("record: invalid tail: " + err.Error())
	}

	// 2. Calculate the start of the entire aligned envelope block
	physicalSize := roundToPage(tail.DataLen + int64(TailSize))
	envelopeBlockStart := fileSize - physicalSize

	if envelopeBlockStart < 0 {
		return SegmentFooter{}, 0, errors.New("record: invalid envelope block geometry")
	}

	// 3. Read envelope from the START of the physical block
	envelopeBuf := make([]byte, tail.DataLen)
	if _, err := file.ReadAt(envelopeBuf, envelopeBlockStart); err != nil {
		return SegmentFooter{}, 0, errors.New("record: failed to read envelope: " + err.Error())
	}

	// 4. Validate checksum
	computedChecksum := crc32.ChecksumIEEE(envelopeBuf)
	if computedChecksum != tail.Checksum {
		return SegmentFooter{}, 0, errors.New("record: checksum mismatch")
	}

	// 5. Decode
	envelope, err := DecodeSegmentFooter(envelopeBuf)
	if err != nil {
		return SegmentFooter{}, 0, fmt.Errorf("record: envelope decode failed: %w", err)
	}

	if segmentID != -1 && envelope.SegmentID != segmentID {
		return SegmentFooter{}, 0, errors.New("record: segment ID mismatch")
	}

	return envelope, envelopeBlockStart, nil
}
