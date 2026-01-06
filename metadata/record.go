package metadata

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
	
	"github.com/miretskiy/blobcache/compression"
)

const (
	segmentMagic = 0xB10BCA4EB10BCA4E
	
	// EncodedBlobRecordSize is now 40 bytes:
	// Hash(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + Flags(8)
	EncodedBlobRecordSize   = 40
	SegmentRecordHeaderSize = 16
	SegmentFooterSize       = 20
	
	// Checksum and Status Flags (Middle bits)
	InvalidChecksumFlag uint64 = 1 << 32
	DeletedFlag         uint64 = 1 << 33
	
	// Compression Schema (Top 4 bits: 60-63)
	compressionShift = 60
	compressionMask  = uint64(0xF) << compressionShift
	
	// InvalidChecksum is a sentinel value for a checksum that's not set.
	InvalidChecksum = InvalidChecksumFlag
)

// BlobRecord represents a single entry in the SegmentRecord.
// It tracks both logical and physical sizes to enable zero-allocation reads
// and high-fidelity storage metrics.
// Flags layout:
// [63-60]: CompressionType
// [59-34]: Reserved
// [33]:    DeletedFlag
// [32]:    InvalidChecksumFlag
// [31-00]: CRC32 Checksum
type BlobRecord struct {
	Hash         uint64 // xxhash of key
	Pos          int64  // Offset within segment
	LogicalSize  int64  // Original uncompressed size for pre-allocation
	PhysicalSize int64  // Actual size on disk (compressed)
	Flags        uint64 // Metadata, status, and checksum flags
}

// --- Compression Methods ---

// Compression returns the codec used for this record.
func (b *BlobRecord) Compression() compression.Codex {
	return compression.Codex((b.Flags & compressionMask) >> compressionShift)
}

// SetCompression sets the codec in the flags.
func (b *BlobRecord) SetCompression(c compression.Codex) {
	b.Flags = (b.Flags &^ compressionMask) | (uint64(c) << compressionShift)
}

// IsCompressed returns true if the codec is not CodexNone.
func (b *BlobRecord) IsCompressed() bool {
	return b.Compression() != compression.CodexNone
}

// CompressionRatio calculates the efficiency of the stored blob.
func (b *BlobRecord) CompressionRatio() float64 {
	if b.LogicalSize == 0 {
		return 0
	}
	return float64(b.PhysicalSize) / float64(b.LogicalSize)
}

// --- Status Methods ---

func (b *BlobRecord) IsDeleted() bool {
	return (b.Flags & DeletedFlag) != 0
}

func (b *BlobRecord) SetDeleted() {
	b.Flags |= DeletedFlag
}

func (b *BlobRecord) Checksum() uint32 {
	return uint32(b.Flags & 0xFFFFFFFF)
}

func (b *BlobRecord) HasChecksum() bool {
	return (b.Flags & InvalidChecksumFlag) == 0
}

// --- Serialization Logic ---

type SegmentRecord struct {
	Records   []BlobRecord
	SegmentID int64
	CTime     time.Time
	IndexKey  []byte
}

type SegmentFooter struct {
	Len      int64
	Checksum uint32
}

func AppendBlobRecord(buf []byte, rec BlobRecord) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, rec.Hash)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Pos))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.LogicalSize))  // Added
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.PhysicalSize)) // Added
	buf = binary.LittleEndian.AppendUint64(buf, rec.Flags)
	return buf
}

func DecodeBlobRecord(buf []byte) (BlobRecord, error) {
	if len(buf) < EncodedBlobRecordSize {
		return BlobRecord{}, fmt.Errorf("buffer too small for blob record")
	}
	return BlobRecord{
		Hash:         binary.LittleEndian.Uint64(buf[0:8]),
		Pos:          int64(binary.LittleEndian.Uint64(buf[8:16])),
		LogicalSize:  int64(binary.LittleEndian.Uint64(buf[16:24])), // Added
		PhysicalSize: int64(binary.LittleEndian.Uint64(buf[24:32])), // Added
		Flags:        binary.LittleEndian.Uint64(buf[32:40]),
	}, nil
}

// AppendSegmentRecord appends a segment record to buffer
func AppendSegmentRecord(buf []byte, sr SegmentRecord) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sr.SegmentID))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sr.CTime.Unix()))
	
	for i := range sr.Records {
		buf = AppendBlobRecord(buf, sr.Records[i])
	}
	
	return buf
}

// DecodeSegmentRecord decodes a segment record from buffer
func DecodeSegmentRecord(buf []byte) (SegmentRecord, error) {
	if len(buf) < SegmentRecordHeaderSize {
		return SegmentRecord{}, fmt.Errorf("buffer too small for segment record header")
	}
	
	segmentID := int64(binary.LittleEndian.Uint64(buf[0:8]))
	ctime := int64(binary.LittleEndian.Uint64(buf[8:16]))
	
	recordsSize := len(buf) - SegmentRecordHeaderSize
	if recordsSize%EncodedBlobRecordSize != 0 {
		return SegmentRecord{}, fmt.Errorf("invalid buffer size: not a multiple of %d", EncodedBlobRecordSize)
	}
	numRecords := recordsSize / EncodedBlobRecordSize
	
	records := make([]BlobRecord, numRecords)
	offset := SegmentRecordHeaderSize
	for i := 0; i < int(numRecords); i++ {
		rec, err := DecodeBlobRecord(buf[offset : offset+EncodedBlobRecordSize])
		if err != nil {
			return SegmentRecord{}, err
		}
		records[i] = rec
		offset += EncodedBlobRecordSize
	}
	
	return SegmentRecord{
		Records:   records,
		SegmentID: segmentID,
		CTime:     time.Unix(ctime, 0),
	}, nil
}

// DecodeSegmentFooter decodes a segment footer from buffer
func DecodeSegmentFooter(buf []byte) (SegmentFooter, error) {
	if len(buf) < SegmentFooterSize {
		return SegmentFooter{}, fmt.Errorf("buffer too small for footer")
	}
	
	segmentLen := int64(binary.LittleEndian.Uint64(buf[0:8]))
	checksum := binary.LittleEndian.Uint32(buf[8:12])
	magic := binary.LittleEndian.Uint64(buf[12:20])
	
	if magic != segmentMagic {
		return SegmentFooter{}, fmt.Errorf("invalid footer magic: %x", magic)
	}
	
	return SegmentFooter{
		Len:      segmentLen,
		Checksum: checksum,
	}, nil
}

// AppendSegmentRecordWithFooter appends SegmentRecord with checksumming footer
// Structure: [SegmentRecord][Alignment Gap (Zeros)][Footer]
func AppendSegmentRecordWithFooter(buf []byte, sr SegmentRecord) []byte {
	recordDataSize := SegmentRecordHeaderSize + (len(sr.Records) * EncodedBlobRecordSize)
	logicalSize := int64(recordDataSize + SegmentFooterSize)
	physicalSize := roundToPage(logicalSize)
	
	// 1. Prepare the buffer.
	if int64(cap(buf)) < physicalSize {
		buf = make([]byte, physicalSize)
	} else {
		buf = buf[:physicalSize]
		for i := range buf {
			buf[i] = 0
		}
	}
	
	// 2. Serialize Record at the VERY START of the physical block.
	_ = AppendSegmentRecord(buf[:0], sr)
	
	// 3. Compute Checksum of the record data ONLY.
	checksum := crc32.ChecksumIEEE(buf[:recordDataSize])
	
	// 4. Place Footer at the VERY END of the physical block.
	footerOffset := physicalSize - int64(SegmentFooterSize)
	binary.LittleEndian.PutUint64(buf[footerOffset:footerOffset+8], uint64(recordDataSize))
	binary.LittleEndian.PutUint32(buf[footerOffset+8:footerOffset+12], checksum)
	binary.LittleEndian.PutUint64(buf[footerOffset+12:footerOffset+20], uint64(segmentMagic))
	
	return buf
}

// DecodeSegmentRecordWithChecksum decodes and validates a segment record
func DecodeSegmentRecordWithChecksum(buf []byte, expectedChecksum uint32) (SegmentRecord, error) {
	computedChecksum := crc32.ChecksumIEEE(buf)
	if computedChecksum != expectedChecksum {
		return SegmentRecord{}, fmt.Errorf("checksum mismatch: %x != %x", computedChecksum, expectedChecksum)
	}
	
	return DecodeSegmentRecord(buf)
}

// ReadSegmentFooterFromFile reads and validates segment footer and record from file
func ReadSegmentFooterFromFile(
		file interface {
	ReadAt([]byte, int64) (int, error)
}, fileSize int64, segmentID int64,
) (SegmentRecord, int64, error) {
	if fileSize < int64(SegmentFooterSize) {
		return SegmentRecord{}, 0, fmt.Errorf("file too small for footer")
	}
	
	// 1. Read footer from the end
	footerBuf := make([]byte, SegmentFooterSize)
	footerPos := fileSize - int64(SegmentFooterSize)
	if _, err := file.ReadAt(footerBuf, footerPos); err != nil {
		return SegmentRecord{}, 0, fmt.Errorf("failed to read footer: %w", err)
	}
	
	footer, err := DecodeSegmentFooter(footerBuf)
	if err != nil {
		return SegmentRecord{}, 0, fmt.Errorf("invalid footer: %w", err)
	}
	
	// 2. Calculate the start of the entire Aligned Metadata Block
	physicalSize := roundToPage(footer.Len + int64(SegmentFooterSize))
	metadataBlockStart := fileSize - physicalSize
	
	if metadataBlockStart < 0 {
		return SegmentRecord{}, 0, fmt.Errorf("invalid metadata block geometry")
	}
	
	// 3. Read segment record from the START of the physical block
	segmentRecordBuf := make([]byte, footer.Len)
	if _, err := file.ReadAt(segmentRecordBuf, metadataBlockStart); err != nil {
		return SegmentRecord{}, 0, fmt.Errorf("failed to read segment record: %w", err)
	}
	
	// 4. Validate and Decode
	segment, err := DecodeSegmentRecordWithChecksum(segmentRecordBuf, footer.Checksum)
	if err != nil {
		return SegmentRecord{}, 0, fmt.Errorf("segment record validation failed: %w", err)
	}
	
	if segmentID != -1 && segment.SegmentID != segmentID {
		return SegmentRecord{}, 0, fmt.Errorf("segment SegmentID mismatch: exp=%d, got=%d", segmentID, segment.SegmentID)
	}
	
	return segment, metadataBlockStart, nil
}

func roundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) & ^(pageSize - 1)
}

// PhysicalMetadataSize returns the exact physical bytes (4KB aligned)
func PhysicalMetadataSize(numRecords int) int64 {
	logicalSize := int64(SegmentRecordHeaderSize +
			(numRecords * EncodedBlobRecordSize) +
			SegmentFooterSize)
	return roundToPage(logicalSize)
}
