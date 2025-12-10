package blobcache

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFooter_EncodeAndDecode(t *testing.T) {
	// Simulate segment with some blob data
	blobData := []byte("test blob data here")
	p0 := int64(len(blobData))

	// Create test records
	records := []SegmentRecord{
		{Hash: 0x1234567890ABCDEF, Pos: 0, Size: 1024, Checksum: 0xAABBCCDD},
		{Hash: 0xFEDCBA0987654321, Pos: 1024, Size: 2048, Checksum: 0x11223344},
		{Hash: 0xABCDEF0123456789, Pos: 3072, Size: 512, Checksum: 0x99887766},
	}

	// Encode footer
	footerBytes := EncodeFooter(p0, records, IncludeChecksums)

	// Build segment data (blob + footer)
	segmentData := append(blobData, footerBytes...)

	// Decode footer (pass entire segment data, not sliced)
	decodedRecords, hasChecksums, err := DecodeFooter(segmentData)
	require.NoError(t, err)
	require.Equal(t, IncludeChecksums, hasChecksums)
	require.Equal(t, 3, len(decodedRecords))

	// Validate records
	for i, rec := range records {
		require.Equal(t, rec, decodedRecords[i])
	}
}

func TestFooter_EmptyRecords(t *testing.T) {
	data := []byte("some data")
	p0 := int64(len(data))

	footerBytes := EncodeFooter(p0, []SegmentRecord{}, OmitChecksums)
	segmentData := append(data, footerBytes...)

	records, hasChecksums, err := DecodeFooter(segmentData)
	require.NoError(t, err)
	require.Equal(t, OmitChecksums, hasChecksums)
	require.Equal(t, 0, len(records))
}

func TestFooter_ManyRecords(t *testing.T) {
	// Create 1000 records
	records := make([]SegmentRecord, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = SegmentRecord{
			Hash:     uint64(i) << 32,
			Pos:      int64(i * 1000),
			Size:     int64(i * 100),
			Checksum: uint32(i),
		}
	}

	p0 := int64(0)
	footerBytes := EncodeFooter(p0, records, IncludeChecksums)

	decodedRecords, hasChecksums, err := DecodeFooter(footerBytes)
	require.NoError(t, err)
	require.Equal(t, IncludeChecksums, hasChecksums)
	require.Equal(t, 1000, len(decodedRecords))

	// Spot check
	require.Equal(t, records[0], decodedRecords[0])
	require.Equal(t, records[500], decodedRecords[500])
	require.Equal(t, records[999], decodedRecords[999])
}

func TestFooter_InvalidMagic_Footer(t *testing.T) {
	records := []SegmentRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}}
	footerBytes := EncodeFooter(0, records, OmitChecksums)

	// Corrupt footer magic
	// Footer: Flags(8) + p0(8) + NumRecords(4) + Checksum(4) + Magic(8)
	// Magic is at offset 24-32 from footer start
	footerStart := len(footerBytes) - footerSize
	binary.LittleEndian.PutUint64(footerBytes[footerStart+24:], 0xDEADBEEFDEADBEEF)

	_, _, err := DecodeFooter(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid footer magic")
}

func TestFooter_InvalidMagic_P0(t *testing.T) {
	data := make([]byte, 100) // Padding
	p0 := int64(len(data))

	records := []SegmentRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}}
	footerBytes := EncodeFooter(p0, records, OmitChecksums)
	segmentData := append(data, footerBytes...)

	// Corrupt p0 magic (at offset 100)
	binary.LittleEndian.PutUint64(segmentData[100:108], 0xBADBADBADBADBAD)

	_, _, err := DecodeFooter(segmentData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid p0 magic")
}

func TestFooter_InvalidMagic_P2(t *testing.T) {
	records := []SegmentRecord{
		{Hash: 111, Pos: 0, Size: 100, Checksum: 111},
		{Hash: 222, Pos: 100, Size: 200, Checksum: 222},
	}

	footerBytes := EncodeFooter(0, records, OmitChecksums)

	// Corrupt p2 magic (after records, before footer)
	// p2 = p0 + 8 + (28 * 2) = 0 + 8 + 56 = 64
	binary.LittleEndian.PutUint64(footerBytes[64:72], 0xBADBADBADBADBAD)

	_, _, err := DecodeFooter(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid p2 magic")
}

func TestFooter_ChecksumMismatch(t *testing.T) {
	records := []SegmentRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}}
	footerBytes := EncodeFooter(0, records, OmitChecksums)

	// Corrupt checksum in footer
	// Footer: Flags(8) + p0(8) + NumRecords(4) + Checksum(4)...
	// Checksum is at offset 20-24 from footer start
	footerStart := len(footerBytes) - footerSize
	binary.LittleEndian.PutUint32(footerBytes[footerStart+20:], 0xDEADBEEF)

	_, _, err := DecodeFooter(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestFooter_CorruptRecordData(t *testing.T) {
	records := []SegmentRecord{
		{Hash: 0x1111111111111111, Pos: 0, Size: 100, Checksum: 0xAAAAAAAA},
		{Hash: 0x2222222222222222, Pos: 100, Size: 200, Checksum: 0xBBBBBBBB},
	}

	footerBytes := EncodeFooter(0, records, IncludeChecksums)

	// Corrupt a record in the middle (first record starts at p1 = 8)
	footerBytes[8] = 0xFF
	footerBytes[9] = 0xFF

	_, _, err := DecodeFooter(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestFooter_TooSmall(t *testing.T) {
	_, _, err := DecodeFooter([]byte("tiny"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too small")
}

func TestFooter_RoundTrip_LargeValues(t *testing.T) {
	// Test with large hash/size values
	records := []SegmentRecord{
		{Hash: 0xFFFFFFFFFFFFFFFF, Pos: 0, Size: 0x7FFFFFFFFFFFFFFF, Checksum: 0xFFFFFFFF},
		{Hash: 0x0000000000000000, Pos: 1000, Size: 0x0000000000000000, Checksum: 0x00000000},
		{Hash: 0x7FFFFFFFFFFFFFFF, Pos: 2000, Size: 0x7FFFFFFFFFFFFFFF, Checksum: 0x80000000},
	}

	footerBytes := EncodeFooter(100, records, IncludeChecksums)

	// Prepend some data to simulate p0 offset
	data := make([]byte, 100)
	segmentData := append(data, footerBytes...)

	decodedRecords, hasChecksums, err := DecodeFooter(segmentData)
	require.NoError(t, err)
	require.Equal(t, IncludeChecksums, hasChecksums)
	require.Equal(t, 3, len(decodedRecords))

	for i := range records {
		require.Equal(t, records[i], decodedRecords[i])
	}
}

func TestSegmentRecord_Encode(t *testing.T) {
	rec := SegmentRecord{
		Hash:     0x1122334455667788,
		Pos:      1024,
		Size:     2048,
		Checksum: 0xAABBCCDD,
	}

	buf := AppendRecord(nil, rec)
	require.Equal(t, recordSize, len(buf))

	// Decode and verify
	decoded := DecodeRecord(buf)
	require.Equal(t, rec, decoded)
}

func TestSegmentRecord_RoundTrip(t *testing.T) {
	records := []SegmentRecord{
		{Hash: 1, Pos: 0, Size: 100, Checksum: 0x11111111},
		{Hash: 2, Pos: 100, Size: 200, Checksum: 0x22222222},
		{Hash: 3, Pos: 300, Size: 300, Checksum: 0x33333333},
	}

	// Encode all
	var buf []byte
	for i := range records {
		buf = AppendRecord(buf, records[i])
	}

	require.Equal(t, recordSize*3, len(buf))

	// Decode all
	for i := 0; i < 3; i++ {
		offset := i * recordSize
		decoded := DecodeRecord(buf[offset : offset+recordSize])
		require.Equal(t, records[i], decoded)
	}
}
