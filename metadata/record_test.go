package metadata

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper for tests: two-phase decode of footer section
func decodeFooterSection(segmentData []byte) (SegmentRecord, error) {
	if len(segmentData) < SegmentFooterSize {
		return SegmentRecord{}, fmt.Errorf("data too small")
	}

	footer, err := DecodeSegmentFooter(segmentData[len(segmentData)-SegmentFooterSize:])
	if err != nil {
		return SegmentRecord{}, err
	}

	if footer.Len < 0 || footer.Len > int64(len(segmentData)-SegmentFooterSize) {
		return SegmentRecord{}, fmt.Errorf("invalid segment record length: %d", footer.Len)
	}

	segmentRecordStart := len(segmentData) - SegmentFooterSize - int(footer.Len)
	segmentRecordData := segmentData[segmentRecordStart : len(segmentData)-SegmentFooterSize]

	return DecodeSegmentRecordWithChecksum(segmentRecordData, footer.Checksum)
}

func TestFooter_EncodeAndDecode(t *testing.T) {
	// Simulate segment with some blob data
	blobData := []byte("test blob data here")

	// Create test segment record
	ctime := time.Unix(1234567890, 0)
	sr := SegmentRecord{
		Records: []BlobRecord{
			{Hash: 0x1234567890ABCDEF, Pos: 0, Size: 1024, Checksum: 0xAABBCCDD},
			{Hash: 0xFEDCBA0987654321, Pos: 1024, Size: 2048, Checksum: 0x11223344},
			{Hash: 0xABCDEF0123456789, Pos: 3072, Size: 512, Checksum: 0x99887766},
		},
		SegmentID: 1,
		CTime:     ctime,
	}

	// Encode footer
	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Build segment data (blob + footer)
	segmentData := append(blobData, footerBytes...)

	decodedSR, err := decodeFooterSection(segmentData)
	require.NoError(t, err)
	require.Equal(t, sr.SegmentID, decodedSR.SegmentID)
	require.Equal(t, 3, len(decodedSR.Records))
	require.Equal(t, ctime.Unix(), decodedSR.CTime.Unix())

	// Validate records
	for i, rec := range sr.Records {
		require.Equal(t, rec, decodedSR.Records[i])
	}
}

func TestFooter_EmptyRecords(t *testing.T) {
	data := []byte("some data")

	sr := SegmentRecord{
		Records:   []BlobRecord{},
		SegmentID: 0,
		CTime:     time.Unix(1000000000, 0),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)
	segmentData := append(data, footerBytes...)

	decodedSR, err := decodeFooterSection(segmentData)
	require.NoError(t, err)
	require.Equal(t, int64(0), decodedSR.SegmentID)
	require.Equal(t, 0, len(decodedSR.Records))
	require.Equal(t, sr.CTime.Unix(), decodedSR.CTime.Unix())
}

func TestFooter_ManyRecords(t *testing.T) {
	// Create 1000 records
	records := make([]BlobRecord, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = BlobRecord{
			Hash:     uint64(i) << 32,
			Pos:      int64(i * 1000),
			Size:     int64(i * 100),
			Checksum: uint64(i),
		}
	}

	sr := SegmentRecord{
		Records:   records,
		SegmentID: 1,
		CTime:     time.Unix(1500000000, 0),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	decodedSR, err := decodeFooterSection(footerBytes)
	require.NoError(t, err)
	require.Equal(t, sr.SegmentID, decodedSR.SegmentID)
	require.Equal(t, 1000, len(decodedSR.Records))
	require.Equal(t, sr.CTime.Unix(), decodedSR.CTime.Unix())

	// Spot check
	require.Equal(t, records[0], decodedSR.Records[0])
	require.Equal(t, records[500], decodedSR.Records[500])
	require.Equal(t, records[999], decodedSR.Records[999])
}

func TestFooter_InvalidMagic_Footer(t *testing.T) {
	sr := SegmentRecord{
		Records:   []BlobRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}},
		SegmentID: 0,
		CTime:     time.Now(),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Corrupt footer magic: Len(8) + Checksum(4) + Magic(8)
	footerStart := len(footerBytes) - SegmentFooterSize
	binary.LittleEndian.PutUint64(footerBytes[footerStart+12:], 0xDEADBEEFDEADBEEF)

	_, err := decodeFooterSection(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid footer magic")
}

func TestFooter_InvalidLength(t *testing.T) {
	sr := SegmentRecord{
		Records:   []BlobRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}},
		SegmentID: 0,
		CTime:     time.Now(),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Corrupt Len to point beyond valid range
	// Footer: Len(8) + Checksum(4) + Magic(8)
	footerStart := len(footerBytes) - SegmentFooterSize
	binary.LittleEndian.PutUint64(footerBytes[footerStart:], uint64(len(footerBytes)))

	_, err := decodeFooterSection(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid segment record length")
}

func TestFooter_ChecksumMismatch(t *testing.T) {
	sr := SegmentRecord{
		Records:   []BlobRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}},
		SegmentID: 0,
		CTime:     time.Now(),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Corrupt checksum in footer: Len(8) + Checksum(4) + Magic(8)
	footerStart := len(footerBytes) - SegmentFooterSize
	binary.LittleEndian.PutUint32(footerBytes[footerStart+8:], 0xDEADBEEF)

	_, err := decodeFooterSection(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestFooter_CorruptRecordData(t *testing.T) {
	sr := SegmentRecord{
		Records: []BlobRecord{
			{Hash: 0x1111111111111111, Pos: 0, Size: 100, Checksum: 0xAAAAAAAA},
			{Hash: 0x2222222222222222, Pos: 100, Size: 200, Checksum: 0xBBBBBBBB},
		},
		SegmentID: 1,
		CTime:     time.Now(),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Corrupt first blob record in segment record
	firstRecordPos := SegmentRecordHeaderSize
	footerBytes[firstRecordPos] = 0xFF
	footerBytes[firstRecordPos+1] = 0xFF

	_, err := decodeFooterSection(footerBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestFooter_TooSmall(t *testing.T) {
	_, err := decodeFooterSection([]byte("tiny"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too small")
}

func TestFooter_RoundTrip_LargeValues(t *testing.T) {
	// Test with large hash/size values
	sr := SegmentRecord{
		Records: []BlobRecord{
			{Hash: 0xFFFFFFFFFFFFFFFF, Pos: 0, Size: 0x7FFFFFFFFFFFFFFF, Checksum: 0xFFFFFFFF},
			{Hash: 0x0000000000000000, Pos: 1000, Size: 0x0000000000000000, Checksum: 0x00000000},
			{Hash: 0x7FFFFFFFFFFFFFFF, Pos: 2000, Size: 0x7FFFFFFFFFFFFFFF, Checksum: 0x80000000},
		},
		SegmentID: 1,
		CTime:     time.Unix(2147483647, 0), // Max 32-bit timestamp
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Prepend some data
	data := make([]byte, 100)
	segmentData := append(data, footerBytes...)

	decodedSR, err := decodeFooterSection(segmentData)
	require.NoError(t, err)
	require.Equal(t, sr.SegmentID, decodedSR.SegmentID)
	require.Equal(t, 3, len(decodedSR.Records))
	require.Equal(t, sr.CTime.Unix(), decodedSR.CTime.Unix())

	for i := range sr.Records {
		require.Equal(t, sr.Records[i], decodedSR.Records[i])
	}
}

func TestBlobRecord_Encode(t *testing.T) {
	rec := BlobRecord{
		Hash:     0x1122334455667788,
		Pos:      1024,
		Size:     2048,
		Checksum: 0xAABBCCDD,
	}

	buf := AppendBlobRecord(nil, rec)
	require.Equal(t, EncodedBlobRecordSize, len(buf))

	// Decode and verify
	decoded, err := DecodeBlobRecord(buf)
	require.NoError(t, err)
	require.Equal(t, rec, decoded)
}

func TestBlobRecord_TooSmall(t *testing.T) {
	_, err := DecodeBlobRecord([]byte("short"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too small")
}

func TestBlobRecord_RoundTrip(t *testing.T) {
	records := []BlobRecord{
		{Hash: 1, Pos: 0, Size: 100, Checksum: 0x11111111},
		{Hash: 2, Pos: 100, Size: 200, Checksum: 0x22222222},
		{Hash: 3, Pos: 300, Size: 300, Checksum: 0x33333333},
	}

	// Encode all
	var buf []byte
	for i := range records {
		buf = AppendBlobRecord(buf, records[i])
	}

	require.Equal(t, EncodedBlobRecordSize*3, len(buf))

	// Decode all
	for i := 0; i < 3; i++ {
		offset := i * EncodedBlobRecordSize
		decoded, err := DecodeBlobRecord(buf[offset : offset+EncodedBlobRecordSize])
		require.NoError(t, err)
		require.Equal(t, records[i], decoded)
	}
}

func TestSegmentRecord_RoundTrip(t *testing.T) {
	sr := SegmentRecord{
		Records: []BlobRecord{
			{Hash: 0x1111111111111111, Pos: 0, Size: 1000, Checksum: 0xAAAAAAAA},
			{Hash: 0x2222222222222222, Pos: 1000, Size: 2000, Checksum: 0xBBBBBBBB},
			{Hash: 0x3333333333333333, Pos: 3000, Size: 3000, Checksum: 0xCCCCCCCC},
		},
		SegmentID: 1,
		CTime:     time.Unix(1234567890, 0),
	}

	// Encode
	buf := AppendSegmentRecord(nil, sr)

	// Expected size: header + 3 records
	expectedSize := SegmentRecordHeaderSize + EncodedBlobRecordSize*3
	require.Equal(t, expectedSize, len(buf))

	// Decode
	decodedSR, err := DecodeSegmentRecord(buf)
	require.NoError(t, err)
	require.Equal(t, 3, len(decodedSR.Records))
	require.Equal(t, sr.SegmentID, decodedSR.SegmentID)
	require.Equal(t, sr.CTime.Unix(), decodedSR.CTime.Unix())

	for i := range sr.Records {
		require.Equal(t, sr.Records[i], decodedSR.Records[i])
	}
}

func TestSegmentRecord_EmptyRecords(t *testing.T) {
	sr := SegmentRecord{
		Records:   []BlobRecord{},
		SegmentID: 0,
		CTime:     time.Unix(1000000000, 0),
	}

	buf := AppendSegmentRecord(nil, sr)
	require.Equal(t, SegmentRecordHeaderSize, len(buf)) // Just the header

	decodedSR, err := DecodeSegmentRecord(buf)
	require.NoError(t, err)
	require.Equal(t, 0, len(decodedSR.Records))
	require.Equal(t, sr.SegmentID, decodedSR.SegmentID)
	require.Equal(t, sr.CTime.Unix(), decodedSR.CTime.Unix())
}

func TestSegmentRecord_TooSmall(t *testing.T) {
	_, err := DecodeSegmentRecord([]byte("short"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too small")
}

func TestSegmentRecord_InvalidRecordCount(t *testing.T) {
	// Create buffer with header + partial record (not a multiple of EncodedBlobRecordSize)
	buf := make([]byte, SegmentRecordHeaderSize+10)   // Header + 10 bytes (not a full record)
	binary.LittleEndian.PutUint64(buf[0:8], 0)        // SegmentID = false
	binary.LittleEndian.PutUint64(buf[8:16], 1000000) // CTime

	_, err := DecodeSegmentRecord(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a multiple")
}

func TestSegmentFooter_DecodeValid(t *testing.T) {
	sr := SegmentRecord{
		Records:   []BlobRecord{{Hash: 123, Pos: 0, Size: 456, Checksum: 789}},
		SegmentID: 1,
		CTime:     time.Unix(1234567890, 0),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// Extract and decode just the footer
	footerStart := len(footerBytes) - SegmentFooterSize
	footer, err := DecodeSegmentFooter(footerBytes[footerStart:])
	require.NoError(t, err)
	require.Greater(t, footer.Len, int64(0))
	require.NotEqual(t, uint32(0), footer.Checksum)
}

func TestSegmentFooter_TooSmall(t *testing.T) {
	_, err := DecodeSegmentFooter([]byte("short"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too small")
}

func TestSegmentFooter_InvalidMagic(t *testing.T) {
	buf := make([]byte, SegmentFooterSize)
	binary.LittleEndian.PutUint64(buf[0:8], 100)         // Len
	binary.LittleEndian.PutUint32(buf[8:12], 0x12345678) // Checksum
	binary.LittleEndian.PutUint64(buf[12:20], 0xBADBAD)  // Bad magic

	_, err := DecodeSegmentFooter(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid footer magic")
}

func TestFooter_CTimePreservation(t *testing.T) {
	// Test various timestamps
	testCases := []time.Time{
		time.Unix(0, 0),                  // Epoch
		time.Unix(1234567890, 0),         // Arbitrary past
		time.Unix(2147483647, 0),         // Max 32-bit timestamp
		time.Now().Truncate(time.Second), // Current time (truncated to second)
	}

	for _, ctime := range testCases {
		sr := SegmentRecord{
			Records: []BlobRecord{
				{Hash: 123, Pos: 0, Size: 100, Checksum: 456},
			},
			SegmentID: 1,
			CTime:     ctime,
		}

		footerBytes := AppendSegmentRecordWithFooter(nil, sr)
		decodedSR, err := decodeFooterSection(footerBytes)
		require.NoError(t, err)
		require.Equal(t, ctime.Unix(), decodedSR.CTime.Unix(),
			"CTime not preserved for timestamp %v", ctime)
	}
}
