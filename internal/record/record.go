// Package record defines the unified binary record format used by both WAL and
// Segment files. This enables consistent tooling, recovery logic, and data migration.
//
// Record Layout (35-byte header + variable payload):
//
//	[Magic:1][Flags:8][SeqID:8][KeyLen:2][PhysicalSize:8][LogicalSize:8][Key][Value]
//
// Design Philosophy:
//   - Header-First: Magic at start enables single-seek reads and hole detection
//   - Mandatory Key Verification: Every Get() compares disk key with requested key
//   - CRC in Flags: Bits 31-0 contain CRC32 of Key+Value for integrity
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
)

// Record format constants.
const (
	// HeaderSize is the fixed size of the record header in bytes.
	HeaderSize = 35 // Magic(1) + Flags(8) + SeqID(8) + KeyLen(2) + PhysicalSize(8) + LogicalSize(8)

	// Magic bytes for record types.
	RecordMagic byte = 0xBB // Valid record anchor
	HoleMagic   byte = 0x00 // Punched hole / padding

	// MaxKeyLen is the maximum key length (uint16 max).
	MaxKeyLen = 1<<16 - 1 // 65535 bytes
)

// Header field offsets within the 35-byte header.
const (
	offMagic        = 0
	offFlags        = 1
	offSeqID        = 9
	offKeyLen       = 17
	offPhysicalSize = 19
	offLogicalSize  = 27
)

// Flags bit layout (reused from metadata.BlobRecord for compatibility).
const (
	// Compression type in bits 63-60 (4 bits, 16 values).
	FlagCompressionShift = 60
	FlagCompressionMask  = uint64(0xF) << FlagCompressionShift

	// BlobErrno in bits 38-34 (5 bits, 32 values).
	FlagErrnoShift = 34
	FlagErrnoMask  = uint64(0x1F) << FlagErrnoShift

	// Status flags.
	FlagDeleted    = uint64(1) << 33 // Tombstone marker
	FlagInvalidCRC = uint64(1) << 32 // CRC not set or invalid

	// CRC32 in bits 31-0.
	FlagCRCMask = uint64(0xFFFFFFFF)
)

// Errors returned by record operations.
var (
	ErrBufferTooSmall = errors.New("record: buffer too small")
	ErrInvalidMagic   = errors.New("record: invalid magic byte")
	ErrHole           = errors.New("record: hole detected")
	ErrCRCMismatch    = errors.New("record: CRC mismatch")
	ErrKeyMismatch    = errors.New("record: key mismatch (possible hash collision)")
	ErrBoundsCheck    = errors.New("record: length exceeds bounds")
)

// Header represents the fixed 35-byte record header.
// Use Encode/Decode methods for serialization.
type Header struct {
	Magic        byte   // 0xBB=valid, 0x00=hole
	Flags        uint64 // Metadata, status, and CRC32
	SeqID        uint64 // Monotonic sequence ID
	KeyLen       uint16 // Key length in bytes
	PhysicalSize int64  // Value length on disk (possibly compressed)
	LogicalSize  int64  // Original uncompressed value length
}

// PayloadSize returns the total size of key + value.
func (h *Header) PayloadSize() int {
	return int(h.KeyLen) + int(h.PhysicalSize)
}

// TotalSize returns the total record size (header + payload).
func (h *Header) TotalSize() int {
	return HeaderSize + h.PayloadSize()
}

// IsValid returns true if the magic byte indicates a valid record.
func (h *Header) IsValid() bool {
	return h.Magic == RecordMagic
}

// IsHole returns true if the magic byte indicates a hole.
func (h *Header) IsHole() bool {
	return h.Magic == HoleMagic
}

// IsDeleted returns true if the deleted flag is set.
func (h *Header) IsDeleted() bool {
	return (h.Flags & FlagDeleted) != 0
}

// SetDeleted sets the deleted flag.
func (h *Header) SetDeleted() {
	h.Flags |= FlagDeleted
}

// CRC returns the CRC32 checksum from flags.
func (h *Header) CRC() uint32 {
	return uint32(h.Flags & FlagCRCMask)
}

// SetCRC sets the CRC32 checksum in flags and clears InvalidCRC.
func (h *Header) SetCRC(crc uint32) {
	h.Flags = (h.Flags &^ (FlagCRCMask | FlagInvalidCRC)) | uint64(crc)
}

// HasValidCRC returns true if CRC is set (InvalidCRC flag is clear).
func (h *Header) HasValidCRC() bool {
	return (h.Flags & FlagInvalidCRC) == 0
}

// Compression returns the compression codec from flags.
func (h *Header) Compression() compression.Codex {
	return compression.Codex((h.Flags & FlagCompressionMask) >> FlagCompressionShift)
}

// SetCompression sets the compression codec in flags.
func (h *Header) SetCompression(c compression.Codex) {
	h.Flags = (h.Flags &^ FlagCompressionMask) | (uint64(c) << FlagCompressionShift)
}

// IsCompressed returns true if compression is enabled.
func (h *Header) IsCompressed() bool {
	return h.Compression() != compression.CodexNone
}

// Errno returns the error code from flags.
func (h *Header) Errno() base.BlobErrno {
	return base.BlobErrno((h.Flags & FlagErrnoMask) >> FlagErrnoShift)
}

// SetErrno sets the error code in flags.
func (h *Header) SetErrno(e base.BlobErrno) {
	h.Flags = (h.Flags &^ FlagErrnoMask) | (uint64(e&0x1F) << FlagErrnoShift)
}

// HasError returns true if the record has a non-zero error code.
func (h *Header) HasError() bool {
	return h.Errno() != base.ErrNone
}

// Encode writes the header to dst (must be at least HeaderSize bytes).
// Returns the number of bytes written (always HeaderSize) or error.
func (h *Header) Encode(dst []byte) (int, error) {
	if len(dst) < HeaderSize {
		return 0, ErrBufferTooSmall
	}

	dst[offMagic] = h.Magic
	binary.LittleEndian.PutUint64(dst[offFlags:], h.Flags)
	binary.LittleEndian.PutUint64(dst[offSeqID:], h.SeqID)
	binary.LittleEndian.PutUint16(dst[offKeyLen:], h.KeyLen)
	binary.LittleEndian.PutUint64(dst[offPhysicalSize:], uint64(h.PhysicalSize))
	binary.LittleEndian.PutUint64(dst[offLogicalSize:], uint64(h.LogicalSize))

	return HeaderSize, nil
}

// DecodeHeader reads a header from src (must be at least HeaderSize bytes).
func DecodeHeader(src []byte) (Header, error) {
	if len(src) < HeaderSize {
		return Header{}, ErrBufferTooSmall
	}

	return Header{
		Magic:        src[offMagic],
		Flags:        binary.LittleEndian.Uint64(src[offFlags:]),
		SeqID:        binary.LittleEndian.Uint64(src[offSeqID:]),
		KeyLen:       binary.LittleEndian.Uint16(src[offKeyLen:]),
		PhysicalSize: int64(binary.LittleEndian.Uint64(src[offPhysicalSize:])),
		LogicalSize:  int64(binary.LittleEndian.Uint64(src[offLogicalSize:])),
	}, nil
}

// AppendHeader appends an encoded header to dst.
func AppendHeader(dst []byte, h Header) []byte {
	dst = append(dst, h.Magic)
	dst = binary.LittleEndian.AppendUint64(dst, h.Flags)
	dst = binary.LittleEndian.AppendUint64(dst, h.SeqID)
	dst = binary.LittleEndian.AppendUint16(dst, h.KeyLen)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(h.PhysicalSize))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(h.LogicalSize))
	return dst
}

// ComputeCRC computes CRC32 (IEEE) over key and value.
func ComputeCRC(key, value []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(value)
	return h.Sum32()
}

// VerifyCRC computes CRC over key+value and compares with expected.
func VerifyCRC(key, value []byte, expected uint32) error {
	if ComputeCRC(key, value) != expected {
		return ErrCRCMismatch
	}
	return nil
}

// =============================================================================
// Record - Unified Header + Key + Value
// =============================================================================

// Record represents a complete on-disk record: header + key + value.
// Use AppendRecord for serialization and DecodeRecord for deserialization.
type Record struct {
	Header
	Key   []byte // Original key bytes (hashed to 128-bit XXH3 for index lookup)
	Value []byte // Value bytes (possibly compressed; PhysicalSize bytes on disk)
}

// EncodedSize returns the total bytes needed to serialize this record.
func (r *Record) EncodedSize() int {
	return HeaderSize + len(r.Key) + len(r.Value)
}

// NewRecord creates a Record with header fields populated from key/value.
// logicalSize is the original uncompressed value size (same as len(value) if uncompressed).
// The CRC is computed over key+value and stored in the header.
func NewRecord(seqID uint64, key, value []byte, logicalSize int64) Record {
	r := Record{
		Header: Header{
			Magic:        RecordMagic,
			Flags:        FlagInvalidCRC, // Will be cleared by SetCRC
			SeqID:        seqID,
			KeyLen:       uint16(len(key)),
			PhysicalSize: int64(len(value)),
			LogicalSize:  logicalSize,
		},
		Key:   key,
		Value: value,
	}
	r.SetCRC(ComputeCRC(key, value))
	return r
}

// AppendRecord appends the full record (header + key + value) to dst.
// The Header.KeyLen and Header.PhysicalSize must already be set correctly.
func AppendRecord(dst []byte, r Record) []byte {
	dst = AppendHeader(dst, r.Header)
	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)
	return dst
}

// DecodeRecord decodes a record from src.
// If verifyCRC is true and the header has a valid CRC, validates the checksum.
// Returns ErrCRCMismatch if checksum validation fails.
func DecodeRecord(src []byte, verifyCRC bool) (Record, error) {
	hdr, err := DecodeHeader(src)
	if err != nil {
		return Record{}, err
	}

	if !hdr.IsValid() {
		if hdr.IsHole() {
			return Record{}, ErrHole
		}
		return Record{}, ErrInvalidMagic
	}

	totalSize := hdr.TotalSize()
	if len(src) < totalSize {
		return Record{}, ErrBufferTooSmall
	}

	keyStart := HeaderSize
	keyEnd := keyStart + int(hdr.KeyLen)
	valueEnd := keyEnd + int(hdr.PhysicalSize)

	key := src[keyStart:keyEnd]
	value := src[keyEnd:valueEnd]

	// Verify CRC if requested and header has valid CRC
	if verifyCRC && hdr.HasValidCRC() {
		if err := VerifyCRC(key, value, hdr.CRC()); err != nil {
			return Record{}, err
		}
	}

	return Record{
		Header: hdr,
		Key:    key,
		Value:  value,
	}, nil
}
