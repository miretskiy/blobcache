package blobcache

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	// segmentMagic is repeated at p0, p2, and footer end for validation
	segmentMagic = 0xB10BCA4EB10BCA4E // "BlobCache" in hex-ish

	// Sizes
	magicSize  = 8  // Size of magic number
	recordSize = 28 // Hash(8) + Pos(8) + Size(8) + Checksum(4)
	footerSize = 32 // Flags(8) + p0(8) + NumRecords(4) + Checksum(4) + Magic(8)

	// Minimum valid footer section: p0 magic + (0 records) + p2 magic + fixed footer
	minFooterSection = magicSize + magicSize + footerSize // 48 bytes

	// Footer flags
	flagHasChecksums = 1 << 0 // All blobs in segment have checksums
)

type ChecksumHandling bool

const (
	OmitChecksums    = ChecksumHandling(false)
	IncludeChecksums = ChecksumHandling(true)
)

// SegmentRecord is one entry in the footer
type SegmentRecord struct {
	Hash     uint64 // xxhash of key (for bloom rebuild)
	Pos      int64  // Offset within segment
	Size     int64  // Blob size
	Checksum uint32 // CRC32 checksum (valid if footer HasChecksums=true)
}

// AppendRecord appends a segment record to buffer (28 bytes)
// Follows stdlib pattern like binary.LittleEndian.AppendUint64
func AppendRecord(buf []byte, rec SegmentRecord) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, rec.Hash)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Pos))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Size))
	buf = binary.LittleEndian.AppendUint32(buf, rec.Checksum)
	return buf
}

// DecodeRecord decodes a segment record from buffer (28 bytes)
func DecodeRecord(buf []byte) SegmentRecord {
	return SegmentRecord{
		Hash:     binary.LittleEndian.Uint64(buf[0:8]),
		Pos:      int64(binary.LittleEndian.Uint64(buf[8:16])),
		Size:     int64(binary.LittleEndian.Uint64(buf[16:24])),
		Checksum: binary.LittleEndian.Uint32(buf[24:28]),
	}
}

// EncodeFooter encodes footer section (without I/O)
// Returns footer bytes ready to append to segment
// p0 is the offset where this footer will start
func EncodeFooter(p0 int64, records []SegmentRecord, checksums ChecksumHandling) []byte {
	// Build complete footer in one pass
	// Structure: Magic(8) + Records + Magic(8) + Footer(32)
	totalSize := magicSize + recordSize*len(records) + magicSize + footerSize
	buf := make([]byte, 0, totalSize)

	// p0: Magic
	buf = binary.LittleEndian.AppendUint64(buf, uint64(segmentMagic))

	// Records section
	recordsStart := len(buf)
	for i := range records {
		buf = AppendRecord(buf, records[i])
	}

	// Compute checksum of records section
	recordsChecksum := crc32.ChecksumIEEE(buf[recordsStart:])

	// p2: Magic
	buf = binary.LittleEndian.AppendUint64(buf, uint64(segmentMagic))

	// Fixed footer (32 bytes): Flags(8) + p0(8) + NumRecords(4) + Checksum(4) + Magic(8)
	var flags uint64
	if checksums == IncludeChecksums {
		flags |= flagHasChecksums
	}
	buf = binary.LittleEndian.AppendUint64(buf, flags)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(p0))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(records)))
	buf = binary.LittleEndian.AppendUint32(buf, recordsChecksum)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(segmentMagic))

	return buf
}

// DecodeFooter decodes and validates footer from segment data
// segmentData contains the entire footer section (from p0 to end)
// Returns (records, hasChecksums, error)
func DecodeFooter(segmentData []byte) ([]SegmentRecord, ChecksumHandling, error) {
	if len(segmentData) < minFooterSection {
		return nil, OmitChecksums, fmt.Errorf("data too small for footer (need %d bytes, got %d)", minFooterSection, len(segmentData))
	}

	// Read fixed footer from end
	footerStart := len(segmentData) - footerSize
	footerBuf := segmentData[footerStart:]

	// Parse footer: Flags(8) + p0(8) + NumRecords(4) + Checksum(4) + Magic(8)
	flags := binary.LittleEndian.Uint64(footerBuf[0:8])
	p0 := int64(binary.LittleEndian.Uint64(footerBuf[8:16]))
	numRecords := binary.LittleEndian.Uint32(footerBuf[16:20])
	checksum := binary.LittleEndian.Uint32(footerBuf[20:24])
	magic := binary.LittleEndian.Uint64(footerBuf[24:32])

	if magic != segmentMagic {
		return nil, OmitChecksums, fmt.Errorf("invalid footer magic: %x", magic)
	}

	hasChecksums := ChecksumHandling((flags & flagHasChecksums) != 0)

	// Validate p0 magic
	if p0 < 0 || p0 >= int64(len(segmentData))-8 {
		return nil, OmitChecksums, fmt.Errorf("invalid p0 offset: %d", p0)
	}
	p0Magic := binary.LittleEndian.Uint64(segmentData[p0 : p0+8])
	if p0Magic != segmentMagic {
		return nil, OmitChecksums, fmt.Errorf("invalid p0 magic: %x", p0Magic)
	}

	// Extract records section [p1→p2]
	p1 := p0 + 8
	recordsSize := int64(recordSize * numRecords)
	p2 := p1 + recordsSize

	if p2+8 > int64(len(segmentData)) {
		return nil, OmitChecksums, fmt.Errorf("records section exceeds data bounds")
	}

	recordsData := segmentData[p1:p2]

	// Validate p2 magic
	p2Magic := binary.LittleEndian.Uint64(segmentData[p2 : p2+8])
	if p2Magic != segmentMagic {
		return nil, OmitChecksums, fmt.Errorf("invalid p2 magic: %x", p2Magic)
	}

	// Verify checksum
	computedChecksum := crc32.ChecksumIEEE(recordsData)
	if computedChecksum != checksum {
		return nil, OmitChecksums, fmt.Errorf("checksum mismatch: %x != %x", computedChecksum, checksum)
	}

	// Parse records
	records := make([]SegmentRecord, numRecords)
	for i := uint32(0); i < numRecords; i++ {
		offset := i * recordSize
		records[i] = DecodeRecord(recordsData[offset : offset+recordSize])
	}

	return records, hasChecksums, nil
}
