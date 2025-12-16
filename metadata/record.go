package metadata

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

const (
	// segmentMagic appears at the end of the footer for validation
	segmentMagic = 0xB10BCA4EB10BCA4E // "BlobCache" in hex-ish

	EncodedBlobRecordSize   = 32 // Hash(8) + Pos(8) + Size(8) + Checksum(8)
	SegmentRecordHeaderSize = 16 // SegmentID(8) + CTime(8)
	SegmentFooterSize       = 20 // Len(8) + Checksum(4) + Magic(8)

	InvalidChecksum = uint64(math.MaxUint32) + 1
)

// BlobRecord is one entry in the SegmentRecord
type BlobRecord struct {
	Hash     uint64 // xxhash of key (for bloom rebuild)
	Pos      int64  // Offset within segment
	Size     int64  // Blob size
	Checksum uint64 // CRC32 checksum; InvalidChecksum if not set.
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

// AppendBlobRecord appends a blob record to buffer (28 bytes)
// Follows stdlib pattern like binary.LittleEndian.AppendUint64
func AppendBlobRecord(buf []byte, rec BlobRecord) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, rec.Hash)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Pos))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Size))
	buf = binary.LittleEndian.AppendUint64(buf, rec.Checksum)
	return buf
}

// DecodeBlobRecord decodes a blob record from buffer (28 bytes)
func DecodeBlobRecord(buf []byte) (BlobRecord, error) {
	if len(buf) < EncodedBlobRecordSize {
		return BlobRecord{}, fmt.Errorf("buffer too small for blob record (need %d bytes, got %d)",
			EncodedBlobRecordSize, len(buf))
	}
	return BlobRecord{
		Hash:     binary.LittleEndian.Uint64(buf[0:8]),
		Pos:      int64(binary.LittleEndian.Uint64(buf[8:16])),
		Size:     int64(binary.LittleEndian.Uint64(buf[16:24])),
		Checksum: binary.LittleEndian.Uint64(buf[24:32]),
	}, nil
}

// AppendSegmentRecord appends a segment record to buffer
// Format: SegmentID(8) + CTime(8) + [BlobRecords...]
func AppendSegmentRecord(buf []byte, sr SegmentRecord) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sr.SegmentID))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sr.CTime.Unix()))

	for i := range sr.Records {
		buf = AppendBlobRecord(buf, sr.Records[i])
	}

	return buf
}

// DecodeSegmentRecord decodes a segment record from buffer
// Format: SegmentID(8) + CTime(8) + [BlobRecords...]
func DecodeSegmentRecord(buf []byte) (SegmentRecord, error) {
	if len(buf) < SegmentRecordHeaderSize {
		return SegmentRecord{}, fmt.Errorf("buffer too small for segment record header")
	}

	segmentID := int64(binary.LittleEndian.Uint64(buf[0:8]))
	ctime := int64(binary.LittleEndian.Uint64(buf[8:16]))

	recordsSize := len(buf) - SegmentRecordHeaderSize
	if recordsSize%EncodedBlobRecordSize != 0 {
		return SegmentRecord{}, fmt.Errorf("invalid buffer size: %d bytes after header is not a multiple of %d",
			recordsSize, EncodedBlobRecordSize)
	}
	numRecords := recordsSize / EncodedBlobRecordSize

	records := make([]BlobRecord, numRecords)
	offset := SegmentRecordHeaderSize
	for i := 0; i < numRecords; i++ {
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
// Format: Len(8) + Checksum(4) + Magic(8)
func DecodeSegmentFooter(buf []byte) (SegmentFooter, error) {
	if len(buf) < SegmentFooterSize {
		return SegmentFooter{}, fmt.Errorf("buffer too small for footer (need %d bytes, got %d)",
			SegmentFooterSize, len(buf))
	}

	// Parse footer: Len(8) + Checksum(4) + Magic(8)
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
// Structure: SegmentRecord + Len(8) + Checksum(4) + Magic(8)
func AppendSegmentRecordWithFooter(buf []byte, sr SegmentRecord) []byte {
	segmentRecordStart := len(buf)
	buf = AppendSegmentRecord(buf, sr)
	segmentRecordLen := len(buf) - segmentRecordStart

	checksum := crc32.ChecksumIEEE(buf[segmentRecordStart:])

	// Append footer: Len(8) + Checksum(4) + Magic(8)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(segmentRecordLen))
	buf = binary.LittleEndian.AppendUint32(buf, checksum)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(segmentMagic))

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
