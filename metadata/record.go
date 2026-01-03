package metadata

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	// segmentMagic appears at the end of the footer for validation
	segmentMagic = 0xB10BCA4EB10BCA4E // "BlobCache" in hex-ish

	EncodedBlobRecordSize   = 32 // Hash(8) + Pos(8) + Size(8) + Checksum(8)
	SegmentRecordHeaderSize = 16 // SegmentID(8) + CTime(8)
	SegmentFooterSize       = 20 // Len(8) + Checksum(4) + Magic(8)

	// Checksum field flags (high 32 bits)
	InvalidChecksumFlag = uint64(1) << 32 // Bit 32: no checksum available
	DeletedFlag         = uint64(1) << 33 // Bit 33: blob deleted (hole punched)

	// Helper for backward compatibility
	InvalidChecksum = InvalidChecksumFlag
)

// BlobRecord is one entry in the SegmentRecord
type BlobRecord struct {
	Hash  uint64 // xxhash of key (for bloom rebuild)
	Pos   int64  // Offset within segment
	Size  int64  // Blob size
	Flags uint64 // Low 32 bits: CRC32 checksum, High 32 bits: flags (InvalidChecksumFlag, DeletedFlag)
}

// IsDeleted returns true if the blob has been evicted (hole punched)
func (b *BlobRecord) IsDeleted() bool {
	return (b.Flags & DeletedFlag) != 0
}

// SetDeleted marks the blob as deleted
func (b *BlobRecord) SetDeleted() {
	b.Flags |= DeletedFlag
}

// Checksum returns the CRC32 checksum value (low 32 bits, without flags)
func (b *BlobRecord) Checksum() uint32 {
	return uint32(b.Flags & 0xFFFFFFFF)
}

// HasChecksum returns true if a checksum is available
func (b *BlobRecord) HasChecksum() bool {
	return (b.Flags & InvalidChecksumFlag) == 0
}

// SegmentRecord contains all metadata for a segment
type SegmentRecord struct {
	Records   []BlobRecord // Blob records in this segment
	SegmentID int64        // Unique segment identifier
	CTime     time.Time    // Creation time of the segment

	// Segment record key; not serialized to disk but set when iterating.
	IndexKey []byte
}

// SegmentFooter is the fixed-size footer at the end of a segment
type SegmentFooter struct {
	Len      int64  // Length of the SegmentRecord data
	Checksum uint32 // CRC32 checksum of SegmentRecord data
}

// AppendBlobRecord appends a blob record to buffer (32 bytes)
func AppendBlobRecord(buf []byte, rec BlobRecord) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, rec.Hash)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Pos))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Size))
	buf = binary.LittleEndian.AppendUint64(buf, rec.Flags)
	return buf
}

// DecodeBlobRecord decodes a blob record from buffer (32 bytes)
func DecodeBlobRecord(buf []byte) (BlobRecord, error) {
	if len(buf) < EncodedBlobRecordSize {
		return BlobRecord{}, fmt.Errorf("buffer too small for blob record")
	}
	return BlobRecord{
		Hash:  binary.LittleEndian.Uint64(buf[0:8]),
		Pos:   int64(binary.LittleEndian.Uint64(buf[8:16])),
		Size:  int64(binary.LittleEndian.Uint64(buf[16:24])),
		Flags: binary.LittleEndian.Uint64(buf[24:32]),
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
		// Change this line in DecodeSegmentRecord
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
	// We ensure it is exactly 'physicalSize' and fully zeroed.
	if int64(cap(buf)) < physicalSize {
		buf = make([]byte, physicalSize)
	} else {
		buf = buf[:physicalSize]
		// Optimization: Use a built-in idiom for zeroing instead of a manual loop
		for i := range buf {
			buf[i] = 0
		}
	}

	// 2. Serialize Record at the VERY START of the physical block.
	// We pass buf[:0] to AppendSegmentRecord so it uses 'buf' as its underlying array.
	_ = AppendSegmentRecord(buf[:0], sr)

	// 3. Compute Checksum of the record data ONLY.
	// This makes the checksum independent of the page-alignment padding.
	checksum := crc32.ChecksumIEEE(buf[:recordDataSize])

	// 4. Place Footer at the VERY END of the physical block.
	// This allows the reader to find it by just looking at the last 20 bytes of the file.
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
	// The block size is based on the logical Len stored in the footer
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
