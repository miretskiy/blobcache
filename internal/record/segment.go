package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
)

// Segment file constants.
const (
	// FileHeaderSize is the size of the segment file header.
	FileHeaderSize = 8

	// TrailerSize is the fixed size of the segment trailer at EOF.
	TrailerSize = 40

	// FileMagic identifies a blobcache segment file.
	FileMagic uint32 = 0xB10BC453

	// FileVersion is the current segment format version.
	FileVersion uint32 = 0x00000001

	// SealMagic indicates a cleanly closed segment.
	SealMagic uint64 = 0xCA5ECA5ECA5ECA5E
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
//	[ FILE HEADER (8B) ] [ RECORD STREAM ] [ ENVELOPE (var) ] [ TRAILER (40B) ]
//
// The Envelope is written only on clean close and enables O(1) index rebuild.
// If missing (crash), recovery falls back to linear record scanning.

// Segment errors.
var (
	ErrInvalidFileMagic = errors.New("segment: invalid file magic")
	ErrInvalidVersion   = errors.New("segment: unsupported version")
	ErrNotSealed        = errors.New("segment: not cleanly closed")
	ErrInvalidTrailer   = errors.New("segment: invalid trailer")
)

// ValidFileHeader checks if src starts with a valid file header.
func ValidFileHeader(src []byte) error {
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

// Trailer is the 40-byte structure at the end of every segment file.
// It provides O(1) access to the envelope for fast index rebuild.
//
// Layout: [Magic:8][EnvelopeOffset:8][EnvelopeSize:8][SealMagic:8][Reserved:8]
type Trailer struct {
	Magic          uint64 // FileMagic extended to 8 bytes
	EnvelopeOffset int64  // Byte offset where envelope starts
	EnvelopeSize   int64  // Size of envelope in bytes
	SealMagic      uint64 // SealMagic if cleanly closed, 0 otherwise
	Reserved       uint64 // Reserved for future use
}

// IsSealed returns true if the segment was cleanly closed.
func (t *Trailer) IsSealed() bool {
	return t.SealMagic == SealMagic
}

// HasEnvelope returns true if envelope is present.
func (t *Trailer) HasEnvelope() bool {
	return t.EnvelopeOffset > 0 && t.EnvelopeSize > 0
}

// Valid checks the trailer magic.
func (t *Trailer) Valid() error {
	if t.Magic != uint64(FileMagic) {
		return ErrInvalidTrailer
	}
	return nil
}

// Encode writes the trailer to dst.
func (t *Trailer) Encode(dst []byte) (int, error) {
	if len(dst) < TrailerSize {
		return 0, ErrBufferTooSmall
	}
	binary.LittleEndian.PutUint64(dst[0:8], t.Magic)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(t.EnvelopeOffset))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(t.EnvelopeSize))
	binary.LittleEndian.PutUint64(dst[24:32], t.SealMagic)
	binary.LittleEndian.PutUint64(dst[32:40], t.Reserved)
	return TrailerSize, nil
}

// DecodeTrailer reads a trailer from src.
func DecodeTrailer(src []byte) (Trailer, error) {
	if len(src) < TrailerSize {
		return Trailer{}, ErrBufferTooSmall
	}
	return Trailer{
		Magic:          binary.LittleEndian.Uint64(src[0:8]),
		EnvelopeOffset: int64(binary.LittleEndian.Uint64(src[8:16])),
		EnvelopeSize:   int64(binary.LittleEndian.Uint64(src[16:24])),
		SealMagic:      binary.LittleEndian.Uint64(src[24:32]),
		Reserved:       binary.LittleEndian.Uint64(src[32:40]),
	}, nil
}

// AppendTrailer appends an encoded trailer to dst.
func AppendTrailer(dst []byte, t Trailer) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, t.Magic)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(t.EnvelopeOffset))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(t.EnvelopeSize))
	dst = binary.LittleEndian.AppendUint64(dst, t.SealMagic)
	dst = binary.LittleEndian.AppendUint64(dst, t.Reserved)
	return dst
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

// Footer serialization constants.
const (
	// FooterEntrySize is the encoded size of a single FooterEntry.
	// Wire format: Hash(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8)
	FooterEntrySize = 48

	// SegmentFooterHeaderSize is the header before entries.
	// Wire format: SegmentID(8) + CTime(8)
	SegmentFooterHeaderSize = 16

	// LegacyFooterSize is the legacy 20-byte footer at segment end.
	// Wire format: RecordDataLen(8) + Checksum(4) + Magic(8)
	LegacyFooterSize = 20

	// LegacySegmentMagic identifies legacy segment footer format.
	// BRIDGE: This matches metadata.segmentMagic for backward compatibility.
	LegacySegmentMagic = 0xB10BCA4EB10BCA4E
)

// FooterEntry represents a single blob's metadata in the segment footer.
// This is the persistent index entry format.
//
// BRIDGE: Wire format matches metadata.BlobRecord exactly for backward compatibility.
// The struct layout differs (we embed Header capabilities) but serialization is identical.
//
// TODO(future): Consider embedding record.Header directly once we change the wire format.
type FooterEntry struct {
	Hash         uint64 // Key hash (xxhash)
	Pos          int64  // Byte offset within segment (to record header)
	LogicalSize  int64  // Original uncompressed value size
	PhysicalSize int64  // On-disk size (possibly compressed)
	SeqID        uint64 // Monotonic sequence ID for ordering
	Flags        uint64 // Compression, status, checksum (same layout as Header.Flags)
}

// SegmentFooter contains metadata for all blobs in a segment.
// Used for persistent index storage and recovery.
//
// BRIDGE: Wire format matches metadata.SegmentRecord for backward compatibility.
//
// TODO(future): Add MinSeqID, MaxSeqID for efficient compaction decisions.
// TODO(future): Add BlobCount, TotalBytes for statistics without iterating.
type SegmentFooter struct {
	SegmentID int64
	CTime     int64 // Unix timestamp (seconds)
	Entries   []FooterEntry

	// IndexKey is the bitcask key for this record (not serialized).
	// BRIDGE: Used by persistence layer for deletion tracking.
	// Populated when reading from bitcask, nil when creating new records.
	IndexKey []byte
}

// LegacySegmentTail is the 20-byte structure at the very end of segment files.
// It allows locating the SegmentFooter data.
//
// BRIDGE: Matches metadata.SegmentFooter for backward compatibility.
type LegacySegmentTail struct {
	DataLen  int64  // Length of the SegmentFooter data (not including padding)
	Checksum uint32 // CRC32 of the SegmentFooter data
}

// --- FooterEntry Methods ---
// These mirror the methods on record.Header for flag manipulation.
// BRIDGE: Maintains API compatibility with metadata.BlobRecord.

// Compression returns the compression codec from flags.
func (e *FooterEntry) Compression() compression.Codex {
	return compression.Codex((e.Flags & FlagCompressionMask) >> FlagCompressionShift)
}

// SetCompression sets the compression codec in flags.
func (e *FooterEntry) SetCompression(c uint8) {
	e.Flags = (e.Flags &^ FlagCompressionMask) | (uint64(c) << FlagCompressionShift)
}

// IsCompressed returns true if compression is enabled.
func (e *FooterEntry) IsCompressed() bool {
	return e.Compression() != 0
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

// AppendFooterEntry appends an encoded FooterEntry to dst.
// Wire format (48 bytes): Hash(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8)
func AppendFooterEntry(dst []byte, e FooterEntry) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, e.Hash)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Pos))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.LogicalSize))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.PhysicalSize))
	dst = binary.LittleEndian.AppendUint64(dst, e.SeqID)
	dst = binary.LittleEndian.AppendUint64(dst, e.Flags)
	return dst
}

// DecodeFooterEntry decodes a FooterEntry from src.
func DecodeFooterEntry(src []byte) (FooterEntry, error) {
	if len(src) < FooterEntrySize {
		return FooterEntry{}, ErrBufferTooSmall
	}
	return FooterEntry{
		Hash:         binary.LittleEndian.Uint64(src[0:8]),
		Pos:          int64(binary.LittleEndian.Uint64(src[8:16])),
		LogicalSize:  int64(binary.LittleEndian.Uint64(src[16:24])),
		PhysicalSize: int64(binary.LittleEndian.Uint64(src[24:32])),
		SeqID:        binary.LittleEndian.Uint64(src[32:40]),
		Flags:        binary.LittleEndian.Uint64(src[40:48]),
	}, nil
}

// --- SegmentFooter Serialization ---

// AppendSegmentFooter appends an encoded SegmentFooter to dst.
// Wire format: SegmentID(8) + CTime(8) + [Entries...]
func AppendSegmentFooter(dst []byte, sf SegmentFooter) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(sf.SegmentID))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(sf.CTime))
	for i := range sf.Entries {
		dst = AppendFooterEntry(dst, sf.Entries[i])
	}
	return dst
}

// DecodeSegmentFooter decodes a SegmentFooter from src.
func DecodeSegmentFooter(src []byte) (SegmentFooter, error) {
	if len(src) < SegmentFooterHeaderSize {
		return SegmentFooter{}, ErrBufferTooSmall
	}

	segmentID := int64(binary.LittleEndian.Uint64(src[0:8]))
	ctime := int64(binary.LittleEndian.Uint64(src[8:16]))

	entriesData := src[SegmentFooterHeaderSize:]
	if len(entriesData)%FooterEntrySize != 0 {
		return SegmentFooter{}, errors.New("record: invalid segment footer size")
	}

	numEntries := len(entriesData) / FooterEntrySize
	entries := make([]FooterEntry, numEntries)
	for i := 0; i < numEntries; i++ {
		offset := i * FooterEntrySize
		e, err := DecodeFooterEntry(entriesData[offset : offset+FooterEntrySize])
		if err != nil {
			return SegmentFooter{}, err
		}
		entries[i] = e
	}

	return SegmentFooter{
		SegmentID: segmentID,
		CTime:     ctime,
		Entries:   entries,
	}, nil
}

// --- Legacy Segment File Footer ---
// These functions handle the 20-byte tail at the end of segment files.

// DecodeLegacySegmentTail decodes the 20-byte tail from segment file end.
func DecodeLegacySegmentTail(src []byte) (LegacySegmentTail, error) {
	if len(src) < LegacyFooterSize {
		return LegacySegmentTail{}, ErrBufferTooSmall
	}

	dataLen := int64(binary.LittleEndian.Uint64(src[0:8]))
	checksum := binary.LittleEndian.Uint32(src[8:12])
	magic := binary.LittleEndian.Uint64(src[12:20])

	if magic != LegacySegmentMagic {
		return LegacySegmentTail{}, errors.New("record: invalid segment tail magic")
	}

	return LegacySegmentTail{
		DataLen:  dataLen,
		Checksum: checksum,
	}, nil
}

// AppendLegacySegmentTail appends the 20-byte tail.
func AppendLegacySegmentTail(dst []byte, tail LegacySegmentTail) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(tail.DataLen))
	dst = binary.LittleEndian.AppendUint32(dst, tail.Checksum)
	dst = binary.LittleEndian.AppendUint64(dst, LegacySegmentMagic)
	return dst
}

// --- Helper Functions ---

// SegmentFooterPhysicalSize returns the 4KB-aligned size needed for a segment footer.
func SegmentFooterPhysicalSize(numEntries int) int64 {
	logicalSize := int64(SegmentFooterHeaderSize + (numEntries * FooterEntrySize) + LegacyFooterSize)
	return roundToPage(logicalSize)
}

// AppendSegmentFooterWithTail serializes a SegmentFooter with CRC and legacy tail into a
// 4KB-aligned buffer. Structure: [SegmentFooter][Alignment Gap (Zeros)][LegacyTail]
//
// BRIDGE: Matches metadata.AppendSegmentRecordWithFooter for backward compatibility.
func AppendSegmentFooterWithTail(buf []byte, sf SegmentFooter) []byte {
	recordDataSize := SegmentFooterHeaderSize + (len(sf.Entries) * FooterEntrySize)
	logicalSize := int64(recordDataSize + LegacyFooterSize)
	physicalSize := roundToPage(logicalSize)

	// 1. Prepare the buffer.
	if int64(cap(buf)) < physicalSize {
		buf = make([]byte, physicalSize)
	} else {
		buf = buf[:physicalSize]
		clear(buf)
	}

	// 2. Serialize footer at the VERY START of the physical block.
	_ = AppendSegmentFooter(buf[:0], sf)

	// 3. Compute Checksum of the record data ONLY.
	checksum := crc32.ChecksumIEEE(buf[:recordDataSize])

	// 4. Place Legacy Tail at the VERY END of the physical block.
	tailOffset := physicalSize - int64(LegacyFooterSize)
	binary.LittleEndian.PutUint64(buf[tailOffset:tailOffset+8], uint64(recordDataSize))
	binary.LittleEndian.PutUint32(buf[tailOffset+8:tailOffset+12], checksum)
	binary.LittleEndian.PutUint64(buf[tailOffset+12:tailOffset+20], LegacySegmentMagic)

	return buf
}

func roundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) &^ (pageSize - 1)
}

// ReadSegmentFooterFromFile reads and validates segment footer from file.
// Returns the SegmentFooter, the metadata block start offset, and any error.
//
// BRIDGE: Matches metadata.ReadSegmentFooterFromFile for backward compatibility.
func ReadSegmentFooterFromFile(
		file interface {
	ReadAt([]byte, int64) (int, error)
}, fileSize int64, segmentID int64,
) (SegmentFooter, int64, error) {
	if fileSize < int64(LegacyFooterSize) {
		return SegmentFooter{}, 0, errors.New("record: file too small for footer")
	}

	// 1. Read legacy tail from the end
	tailBuf := make([]byte, LegacyFooterSize)
	tailPos := fileSize - int64(LegacyFooterSize)
	if _, err := file.ReadAt(tailBuf, tailPos); err != nil {
		return SegmentFooter{}, 0, errors.New("record: failed to read footer: " + err.Error())
	}

	tail, err := DecodeLegacySegmentTail(tailBuf)
	if err != nil {
		return SegmentFooter{}, 0, errors.New("record: invalid footer: " + err.Error())
	}

	// 2. Calculate the start of the entire Aligned Metadata Block
	physicalSize := roundToPage(tail.DataLen + int64(LegacyFooterSize))
	metadataBlockStart := fileSize - physicalSize

	if metadataBlockStart < 0 {
		return SegmentFooter{}, 0, errors.New("record: invalid metadata block geometry")
	}

	// 3. Read segment footer from the START of the physical block
	footerBuf := make([]byte, tail.DataLen)
	if _, err := file.ReadAt(footerBuf, metadataBlockStart); err != nil {
		return SegmentFooter{}, 0, errors.New("record: failed to read segment footer: " + err.Error())
	}

	// 4. Validate checksum
	computedChecksum := crc32.ChecksumIEEE(footerBuf)
	if computedChecksum != tail.Checksum {
		return SegmentFooter{}, 0, errors.New("record: checksum mismatch")
	}

	// 5. Decode
	footer, err := DecodeSegmentFooter(footerBuf)
	if err != nil {
		return SegmentFooter{}, 0, errors.New("record: segment footer validation failed: " + err.Error())
	}

	if segmentID != -1 && footer.SegmentID != segmentID {
		return SegmentFooter{}, 0, errors.New("record: segment ID mismatch")
	}

	return footer, metadataBlockStart, nil
}
