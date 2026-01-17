package wal

import (
	"encoding/binary"
	"errors"
)

// WAL file header constants.
const (
	// FileMagic identifies a WAL file: "BLOBWAL1" in ASCII
	FileMagic uint64 = 0x314C4157424F4C42 // "BLOBWAL1" little-endian

	// FileVersion is the current WAL format version.
	FileVersion uint32 = 1

	// FileHeaderSize is the size of the WAL file header in bytes.
	// Layout: Magic(8) + Version(4) + Flags(4) + CreatedAt(8) + Reserved(8) = 32 bytes
	FileHeaderSize = 32
)

// Errors for WAL header operations.
var (
	ErrInvalidMagic   = errors.New("wal: invalid file magic")
	ErrInvalidVersion = errors.New("wal: unsupported version")
	ErrHeaderTooSmall = errors.New("wal: header buffer too small")
)

// FileHeader is the header at the start of each WAL file.
type FileHeader struct {
	Magic     uint64 // Must be FileMagic
	Version   uint32 // Format version
	Flags     uint32 // Reserved for future use
	CreatedAt int64  // Unix timestamp (nanoseconds)
	Reserved  uint64 // Reserved for future use
}

// Encode serializes the header to a 32-byte slice.
func (h *FileHeader) Encode() []byte {
	buf := make([]byte, FileHeaderSize)
	h.EncodeTo(buf)
	return buf
}

// EncodeTo serializes the header into the provided buffer.
// Buffer must be at least FileHeaderSize (32) bytes.
func (h *FileHeader) EncodeTo(buf []byte) {
	_ = buf[:FileHeaderSize] // Bounds check hint
	binary.LittleEndian.PutUint64(buf[0:8], h.Magic)
	binary.LittleEndian.PutUint32(buf[8:12], h.Version)
	binary.LittleEndian.PutUint32(buf[12:16], h.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.CreatedAt))
	binary.LittleEndian.PutUint64(buf[24:32], h.Reserved)
}

// DecodeFileHeader reads a FileHeader from src.
func DecodeFileHeader(src []byte) (FileHeader, error) {
	if len(src) < FileHeaderSize {
		return FileHeader{}, ErrHeaderTooSmall
	}

	h := FileHeader{
		Magic:     binary.LittleEndian.Uint64(src[0:8]),
		Version:   binary.LittleEndian.Uint32(src[8:12]),
		Flags:     binary.LittleEndian.Uint32(src[12:16]),
		CreatedAt: int64(binary.LittleEndian.Uint64(src[16:24])),
		Reserved:  binary.LittleEndian.Uint64(src[24:32]),
	}

	if h.Magic != FileMagic {
		return FileHeader{}, ErrInvalidMagic
	}
	if h.Version != FileVersion {
		return FileHeader{}, ErrInvalidVersion
	}

	return h, nil
}
