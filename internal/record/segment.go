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
// 2. Persistent index storage in bitcask (segmentID -> SegmentEnvelope)
// 3. Recovery (rebuilding index from segment footers)
//
// TODO(future): Consider removing Inode in favor of scanning record.Header
// directly from segment files. This would eliminate the need for a separate
// footer format and simplify the codebase.
//
// TODO(future): Add min/max SeqID to SegmentEnvelope for efficient range queries
// and compaction decisions.

// Envelope serialization constants.
const (
	// InodeSize is the encoded size of a single Inode (64 bytes).
	// Wire format: Key.Lo(8) + Key.Hi(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8) + KeyLen(2) + Pad(6)
	InodeSize = 64

	// SegmentEnvelopeHeaderSize is the header before entries (40 bytes).
	// Wire format: SegmentID(8) + CTime(8) + MinSeqID(8) + MaxSeqID(8) + RecordCount(8)
	SegmentEnvelopeHeaderSize = 40

	// TailSize is the 20-byte tail at the end of segment files.
	// Wire format: EnvelopeDataLen(8) + Checksum(4) + Magic(8)
	TailSize = 20

	// TailMagic identifies a valid segment tail.
	TailMagic = 0xB10BCA4EB10BCA4E
)

// Inode represents a single blob's location and metadata within a segment.
// Like a Unix inode, it indexes content by its hash rather than by name.
//
// Used in segment file envelopes for O(1) index recovery on startup.
// The in-memory index uses the leaner index.Item (32 bytes) for hot paths.
type Inode struct {
	Key          Key    // 128-bit XXH3 content hash
	Pos          int64  // Byte offset within segment (to record header)
	LogicalSize  int64  // Original uncompressed value size
	PhysicalSize int64  // On-disk size of value (possibly compressed)
	SeqID        uint64 // Monotonic sequence ID for ordering
	Flags        uint64 // Compression, status, checksum (same layout as Header.Flags)
	KeyLen       uint16 // Key length in bytes (for computing total record size)
}

// SegmentEnvelope is the index manifest for a single segment file.
// Written at segment close, it enables O(1) index reconstruction on recovery.
type SegmentEnvelope struct {
	SegmentID   int64
	CTime       int64  // Unix timestamp (seconds)
	MinSeqID    uint64 // Lowest SeqID in this segment (compaction decisions)
	MaxSeqID    uint64 // Highest SeqID in this segment (WAL recovery checkpoint)
	RecordCount int64  // Number of records (validation: must match len(Entries))
	Entries     []Inode

	// IndexKey is the persistence layer key (not serialized to segment files).
	// Populated when reading from persistent index, nil for new envelopes.
	IndexKey []byte
}

// SegmentTail is the fixed 20-byte structure at the end of every segment file.
// It provides the offset and checksum needed to locate the SegmentEnvelope.
type SegmentTail struct {
	DataLen  int64  // Length of the SegmentEnvelope data (not including padding)
	Checksum uint32 // CRC32 of the SegmentEnvelope data
}

// --- Inode Methods ---
// Flag accessors mirror record.Header for consistency across the codebase.

// Compression returns the compression codec from flags.
func (e *Inode) Compression() compression.Codex {
	return compression.Codex((e.Flags & FlagCompressionMask) >> FlagCompressionShift)
}

// SetCompression sets the compression codec in flags.
func (e *Inode) SetCompression(c compression.Codex) {
	e.Flags = (e.Flags &^ FlagCompressionMask) | (uint64(c) << FlagCompressionShift)
}

// IsCompressed returns true if compression is enabled.
func (e *Inode) IsCompressed() bool {
	return e.Compression() != compression.CodexNone
}

// IsDeleted returns true if the deleted flag is set.
func (e *Inode) IsDeleted() bool {
	return (e.Flags & FlagDeleted) != 0
}

// SetDeleted sets the deleted flag.
func (e *Inode) SetDeleted() {
	e.Flags |= FlagDeleted
}

// Checksum returns the CRC32 checksum from flags.
func (e *Inode) Checksum() uint32 {
	return uint32(e.Flags & FlagCRCMask)
}

// HasChecksum returns true if checksum is valid (InvalidCRC flag is clear).
func (e *Inode) HasChecksum() bool {
	return (e.Flags & FlagInvalidCRC) == 0
}

// Errno returns the error code from flags.
func (e *Inode) Errno() base.BlobErrno {
	return base.BlobErrno((e.Flags & FlagErrnoMask) >> FlagErrnoShift)
}

// SetErrno sets the error code in flags.
func (e *Inode) SetErrno(errno base.BlobErrno) {
	e.Flags = (e.Flags &^ FlagErrnoMask) | (uint64(errno&0x1F) << FlagErrnoShift)
}

// HasError returns true if the entry has a non-zero error code.
func (e *Inode) HasError() bool {
	return e.Errno() != base.ErrNone
}

// CompressionRatio returns physical/logical size ratio.
func (e *Inode) CompressionRatio() float64 {
	if e.LogicalSize == 0 {
		return 0
	}
	return float64(e.PhysicalSize) / float64(e.LogicalSize)
}

// --- Inode Serialization ---

// AppendInode appends an encoded Inode to dst.
// Wire format (64 bytes): Key.Lo(8) + Key.Hi(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + SeqID(8) + Flags(8) + KeyLen(2) + Pad(6)
func AppendInode(dst []byte, e Inode) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, e.Key.Lo)
	dst = binary.LittleEndian.AppendUint64(dst, e.Key.Hi)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Pos))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.LogicalSize))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.PhysicalSize))
	dst = binary.LittleEndian.AppendUint64(dst, e.SeqID)
	dst = binary.LittleEndian.AppendUint64(dst, e.Flags)
	dst = binary.LittleEndian.AppendUint16(dst, e.KeyLen)
	dst = append(dst, 0, 0, 0, 0, 0, 0) // 6 bytes padding for alignment
	return dst
}

// DecodeInode decodes an Inode from src.
func DecodeInode(src []byte) (Inode, error) {
	if len(src) < InodeSize {
		return Inode{}, ErrBufferTooSmall
	}
	return Inode{
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

// --- SegmentEnvelope Serialization ---

// AppendSegmentEnvelope appends an encoded SegmentEnvelope to dst.
// Wire format: SegmentID(8) + CTime(8) + MinSeqID(8) + MaxSeqID(8) + RecordCount(8) + [Entries...]
func AppendSegmentEnvelope(dst []byte, sf SegmentEnvelope) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(sf.SegmentID))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(sf.CTime))
	dst = binary.LittleEndian.AppendUint64(dst, sf.MinSeqID)
	dst = binary.LittleEndian.AppendUint64(dst, sf.MaxSeqID)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(len(sf.Entries))) // RecordCount
	for i := range sf.Entries {
		dst = AppendInode(dst, sf.Entries[i])
	}
	return dst
}

// DecodeSegmentEnvelope decodes a SegmentEnvelope from src.
func DecodeSegmentEnvelope(src []byte) (SegmentEnvelope, error) {
	if len(src) < SegmentEnvelopeHeaderSize {
		return SegmentEnvelope{}, ErrBufferTooSmall
	}

	segmentID := int64(binary.LittleEndian.Uint64(src[0:8]))
	ctime := int64(binary.LittleEndian.Uint64(src[8:16]))
	minSeqID := binary.LittleEndian.Uint64(src[16:24])
	maxSeqID := binary.LittleEndian.Uint64(src[24:32])
	recordCount := int64(binary.LittleEndian.Uint64(src[32:40]))

	entriesData := src[SegmentEnvelopeHeaderSize:]
	if len(entriesData)%InodeSize != 0 {
		return SegmentEnvelope{}, errors.New("record: invalid segment envelope size")
	}

	numEntries := len(entriesData) / InodeSize
	if int64(numEntries) != recordCount {
		return SegmentEnvelope{}, fmt.Errorf("record: record count mismatch: header=%d, actual=%d", recordCount, numEntries)
	}

	entries := make([]Inode, numEntries)
	for i := 0; i < numEntries; i++ {
		offset := i * InodeSize
		e, err := DecodeInode(entriesData[offset : offset+InodeSize])
		if err != nil {
			return SegmentEnvelope{}, err
		}
		entries[i] = e
	}

	return SegmentEnvelope{
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

// AppendSegmentTail appends the 20-byte tail.
func AppendSegmentTail(dst []byte, tail SegmentTail) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(tail.DataLen))
	dst = binary.LittleEndian.AppendUint32(dst, tail.Checksum)
	dst = binary.LittleEndian.AppendUint64(dst, TailMagic)
	return dst
}

// --- Helper Functions ---

// SegmentEnvelopePhysicalSize returns the 4KB-aligned size needed for a segment envelope.
func SegmentEnvelopePhysicalSize(numEntries int) int64 {
	logicalSize := int64(SegmentEnvelopeHeaderSize + (numEntries * InodeSize) + TailSize)
	return roundToPage(logicalSize)
}

// AppendSegmentEnvelopeWithTail serializes a SegmentEnvelope with CRC and tail into a
// 4KB-aligned buffer. Structure: [SegmentEnvelope][Alignment Gap (Zeros)][Tail]
func AppendSegmentEnvelopeWithTail(buf []byte, sf SegmentEnvelope) []byte {
	envelopeDataSize := SegmentEnvelopeHeaderSize + (len(sf.Entries) * InodeSize)
	logicalSize := int64(envelopeDataSize + TailSize)
	physicalSize := roundToPage(logicalSize)

	// 1. Prepare the buffer.
	if int64(cap(buf)) < physicalSize {
		buf = make([]byte, physicalSize)
	} else {
		buf = buf[:physicalSize]
		clear(buf)
	}

	// 2. Serialize envelope at the VERY START of the physical block.
	_ = AppendSegmentEnvelope(buf[:0], sf)

	// 3. Compute checksum of the envelope data ONLY.
	checksum := crc32.ChecksumIEEE(buf[:envelopeDataSize])

	// 4. Place tail at the VERY END of the physical block.
	tailOffset := physicalSize - int64(TailSize)
	binary.LittleEndian.PutUint64(buf[tailOffset:tailOffset+8], uint64(envelopeDataSize))
	binary.LittleEndian.PutUint32(buf[tailOffset+8:tailOffset+12], checksum)
	binary.LittleEndian.PutUint64(buf[tailOffset+12:tailOffset+20], TailMagic)

	return buf
}

func roundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) &^ (pageSize - 1)
}

// ReadSegmentEnvelopeFromFile reads and validates a segment envelope from file.
// Returns the SegmentEnvelope, the envelope block start offset, and any error.
func ReadSegmentEnvelopeFromFile(
	file interface {
		ReadAt([]byte, int64) (int, error)
	},
	fileSize int64,
	segmentID int64,
) (SegmentEnvelope, int64, error) {
	if fileSize < int64(TailSize) {
		return SegmentEnvelope{}, 0, errors.New("record: file too small for tail")
	}

	// 1. Read tail from the end
	tailBuf := make([]byte, TailSize)
	tailPos := fileSize - int64(TailSize)
	if _, err := file.ReadAt(tailBuf, tailPos); err != nil {
		return SegmentEnvelope{}, 0, errors.New("record: failed to read tail: " + err.Error())
	}

	tail, err := DecodeSegmentTail(tailBuf)
	if err != nil {
		return SegmentEnvelope{}, 0, errors.New("record: invalid tail: " + err.Error())
	}

	// 2. Calculate the start of the entire aligned envelope block
	physicalSize := roundToPage(tail.DataLen + int64(TailSize))
	envelopeBlockStart := fileSize - physicalSize

	if envelopeBlockStart < 0 {
		return SegmentEnvelope{}, 0, errors.New("record: invalid envelope block geometry")
	}

	// 3. Read envelope from the START of the physical block
	envelopeBuf := make([]byte, tail.DataLen)
	if _, err := file.ReadAt(envelopeBuf, envelopeBlockStart); err != nil {
		return SegmentEnvelope{}, 0, errors.New("record: failed to read envelope: " + err.Error())
	}

	// 4. Validate checksum
	computedChecksum := crc32.ChecksumIEEE(envelopeBuf)
	if computedChecksum != tail.Checksum {
		return SegmentEnvelope{}, 0, errors.New("record: checksum mismatch")
	}

	// 5. Decode
	envelope, err := DecodeSegmentEnvelope(envelopeBuf)
	if err != nil {
		return SegmentEnvelope{}, 0, errors.New("record: envelope decode failed: " + err.Error())
	}

	if segmentID != -1 && envelope.SegmentID != segmentID {
		return SegmentEnvelope{}, 0, errors.New("record: segment ID mismatch")
	}

	return envelope, envelopeBlockStart, nil
}
