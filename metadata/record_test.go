package metadata

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/compression"
	"github.com/stretchr/testify/require"
)

// Helper for tests: two-phase decode of footer section
func decodeFooterSection(segmentData []byte) (SegmentRecord, error) {
	if len(segmentData) < SegmentFooterSize {
		return SegmentRecord{}, fmt.Errorf("data too small")
	}

	// 1. Footer is always at the absolute end
	footerBuf := segmentData[len(segmentData)-SegmentFooterSize:]
	footer, err := DecodeSegmentFooter(footerBuf)
	if err != nil {
		return SegmentRecord{}, err
	}

	// 2. Calculate where the Aligned Metadata Block starts
	physicalSize := roundToPage(footer.Len + int64(SegmentFooterSize))

	metadataBlockStart := len(segmentData) - int(physicalSize)
	if metadataBlockStart < 0 {
		return SegmentRecord{}, fmt.Errorf("invalid segment record length: metadata block out of bounds")
	}

	// 3. The Record Data is at the START of that physical block
	segmentRecordData := segmentData[metadataBlockStart : metadataBlockStart+int(footer.Len)]

	return DecodeSegmentRecordWithChecksum(segmentRecordData, footer.Checksum)
}

func TestBlobRecord_CompressionAndSizes(t *testing.T) {
	// Test case: Compressed blob with dual-size tracking
	rec := BlobRecord{
		Hash:         0x1234567890ABCDEF,
		Pos:          5000,
		LogicalSize:  10240, // 10KB original
		PhysicalSize: 4096,  // 4KB compressed
		Flags:        0,
	}
	rec.SetCompression(compression.CodexZstd) // Set compression bit

	require.True(t, rec.IsCompressed())
	require.Equal(t, compression.CodexZstd, rec.Compression())
	require.InDelta(t, 0.4, rec.CompressionRatio(), 0.001) // 4096 / 10240

	// Round-trip serialization
	buf := AppendBlobRecord(nil, rec)
	require.Equal(t, EncodedBlobRecordSize, len(buf))

	decoded, err := DecodeBlobRecord(buf)
	require.NoError(t, err)
	require.Equal(t, rec, decoded)
	require.Equal(t, int64(10240), decoded.LogicalSize)
	require.Equal(t, int64(4096), decoded.PhysicalSize)
}

func TestFooter_EncodeAndDecode_WithCompression(t *testing.T) {
	ctime := time.Unix(1234567890, 0)

	// Create records with mixed compression and sizes
	rec1 := BlobRecord{Hash: 1, Pos: 0, LogicalSize: 1000, PhysicalSize: 1000, Flags: 0}
	rec1.SetCompression(compression.CodexNone)

	rec2 := BlobRecord{Hash: 2, Pos: 1000, LogicalSize: 5000, PhysicalSize: 1200, Flags: 0}
	rec2.SetCompression(compression.CodexLZ4) // Corrected Codex name

	sr := SegmentRecord{
		Records:   []BlobRecord{rec1, rec2},
		SegmentID: 99,
		CTime:     ctime,
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	decodedSR, err := decodeFooterSection(footerBytes)
	require.NoError(t, err)
	require.Equal(t, 2, len(decodedSR.Records))

	// Validate logical vs physical size preservation
	require.Equal(t, int64(5000), decodedSR.Records[1].LogicalSize)
	require.Equal(t, int64(1200), decodedSR.Records[1].PhysicalSize)
	require.Equal(t, compression.CodexLZ4, decodedSR.Records[1].Compression())
}

func TestFooter_HolePunchingAlignment(t *testing.T) {
	// Verify that PhysicalMetadataSize accounts for the larger 40-byte records
	numRecords := 100

	pSize := PhysicalMetadataSize(numRecords)
	// (16 + 4000 + 20) = 4036. This fits in ONE 4KB page (4096)
	require.Equal(t, int64(4096), pSize)
	require.True(t, pSize%4096 == 0)
}

func TestCompressionHeuristic_FlagSafety(t *testing.T) {
	// Test that setting compression doesn't clobber other flags like Deleted
	rec := BlobRecord{Flags: 0}
	rec.SetDeleted()
	rec.SetCompression(compression.CodexZstd)

	require.True(t, rec.IsDeleted())
	require.True(t, rec.IsCompressed())
	require.Equal(t, compression.CodexZstd, rec.Compression())

	// Unset compression
	rec.SetCompression(compression.CodexNone)
	require.False(t, rec.IsCompressed())
	require.True(t, rec.IsDeleted()) // Deleted bit must remain
}

func TestSegmentRecord_InvalidRecordCount_Updated(t *testing.T) {
	// With 40-byte records, a 32-byte addition should fail
	buf := make([]byte, SegmentRecordHeaderSize+32)
	binary.LittleEndian.PutUint64(buf[0:8], 1)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(time.Now().Unix()))

	_, err := DecodeSegmentRecord(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a multiple of 40")
}

func TestFooter_ManyRecords_LargeSlab(t *testing.T) {
	// Create 5000 records to test metadata block crossing multiple pages
	records := make([]BlobRecord, 5000)
	for i := 0; i < 5000; i++ {
		records[i] = BlobRecord{
			Hash:         uint64(i),
			Pos:          int64(i * 4096),
			LogicalSize:  8192,
			PhysicalSize: 2048,
			Flags:        0,
		}
		records[i].SetCompression(compression.CodexZstd)
	}

	sr := SegmentRecord{
		Records:   records,
		SegmentID: 500,
		CTime:     time.Now().Truncate(time.Second),
	}

	footerBytes := AppendSegmentRecordWithFooter(nil, sr)

	// The metadata block should be (16 + (5000 * 40) + 20) = 200,036 bytes.
	// Rounded to 4KB: 200,704 bytes.
	require.Equal(t, int(PhysicalMetadataSize(5000)), len(footerBytes))

	decodedSR, err := decodeFooterSection(footerBytes)
	require.NoError(t, err)
	require.Equal(t, 5000, len(decodedSR.Records))
	require.Equal(t, compression.CodexZstd, decodedSR.Records[4999].Compression())
}

func TestFooter_RoundTrip_LargeValues(t *testing.T) {
	// Test with large hash/size values
	sr := SegmentRecord{
		Records: []BlobRecord{
			{Hash: 0xFFFFFFFFFFFFFFFF, Pos: 0, LogicalSize: 0x7FFFFFFFFFFFFFFF, PhysicalSize: 0x7FFFFFFFFFFFFFFF, Flags: 0xFFFFFFFF},
			{Hash: 0x0000000000000000, Pos: 1000, LogicalSize: 0, PhysicalSize: 0, Flags: 0},
			{Hash: 0x7FFFFFFFFFFFFFFF, Pos: 2000, LogicalSize: 0x7FFFFFFFFFFFFFFF, PhysicalSize: 0x7FFFFFFFFFFFFFFF, Flags: 0x80000000},
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
