package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/compression"
	"github.com/stretchr/testify/require"
)

// Helper for tests: two-phase decode of footer section
func decodeFooterSection(segmentData []byte) (SegmentFooter, error) {
	if len(segmentData) < LegacyFooterSize {
		return SegmentFooter{}, fmt.Errorf("data too small")
	}

	// 1. Footer is always at the absolute end
	footerBuf := segmentData[len(segmentData)-LegacyFooterSize:]
	tail, err := DecodeLegacySegmentTail(footerBuf)
	if err != nil {
		return SegmentFooter{}, err
	}

	// 2. Calculate where the Aligned Metadata Block starts
	physicalSize := roundToPage(tail.DataLen + int64(LegacyFooterSize))

	metadataBlockStart := len(segmentData) - int(physicalSize)
	if metadataBlockStart < 0 {
		return SegmentFooter{}, fmt.Errorf("invalid segment record length: metadata block out of bounds")
	}

	// 3. The Record Data is at the START of that physical block
	segmentRecordData := segmentData[metadataBlockStart : metadataBlockStart+int(tail.DataLen)]

	// 4. Validate checksum
	computedChecksum := crc32.ChecksumIEEE(segmentRecordData)
	if computedChecksum != tail.Checksum {
		return SegmentFooter{}, fmt.Errorf("checksum mismatch: expected %x, got %x", tail.Checksum, computedChecksum)
	}

	return DecodeSegmentFooter(segmentRecordData)
}

func TestFooterEntry_CompressionAndSizes(t *testing.T) {
	// Test case: Compressed blob with dual-size tracking
	entry := FooterEntry{
		Hash:         0x1234567890ABCDEF,
		Pos:          5000,
		LogicalSize:  10240, // 10KB original
		PhysicalSize: 4096,  // 4KB compressed
		Flags:        0,
	}
	entry.SetCompression(compression.CodexZstd) // Set compression bit

	require.True(t, entry.IsCompressed())
	require.Equal(t, compression.CodexZstd, entry.Compression())
	require.InDelta(t, 0.4, entry.CompressionRatio(), 0.001) // 4096 / 10240

	// Round-trip serialization
	buf := AppendFooterEntry(nil, entry)
	require.Equal(t, FooterEntrySize, len(buf))

	decoded, err := DecodeFooterEntry(buf)
	require.NoError(t, err)
	require.Equal(t, entry, decoded)
	require.Equal(t, int64(10240), decoded.LogicalSize)
	require.Equal(t, int64(4096), decoded.PhysicalSize)
}

func TestSegmentFooter_EncodeAndDecode_WithCompression(t *testing.T) {
	ctime := time.Unix(1234567890, 0)

	// Create entries with mixed compression and sizes
	entry1 := FooterEntry{Hash: 1, Pos: 0, LogicalSize: 1000, PhysicalSize: 1000, Flags: 0}
	entry1.SetCompression(compression.CodexNone)

	entry2 := FooterEntry{Hash: 2, Pos: 1000, LogicalSize: 5000, PhysicalSize: 1200, Flags: 0}
	entry2.SetCompression(compression.CodexLZ4)

	sf := SegmentFooter{
		Entries:   []FooterEntry{entry1, entry2},
		SegmentID: 99,
		CTime:     ctime.Unix(),
	}

	footerBytes := AppendSegmentFooterWithTail(nil, sf)

	decodedSF, err := decodeFooterSection(footerBytes)
	require.NoError(t, err)
	require.Equal(t, 2, len(decodedSF.Entries))

	// Validate logical vs physical size preservation
	require.Equal(t, int64(5000), decodedSF.Entries[1].LogicalSize)
	require.Equal(t, int64(1200), decodedSF.Entries[1].PhysicalSize)
	require.Equal(t, compression.CodexLZ4, decodedSF.Entries[1].Compression())
}

func TestSegmentFooter_HolePunchingAlignment(t *testing.T) {
	// Verify that SegmentFooterPhysicalSize accounts for 48-byte entries
	numEntries := 100

	pSize := SegmentFooterPhysicalSize(numEntries)
	// (16 + 100*48 + 20) = 4836. This rounds to 8KB (8192)
	require.Equal(t, int64(8192), pSize)
	require.True(t, pSize%4096 == 0)
}

func TestFooterEntry_FlagSafety(t *testing.T) {
	// Test that setting compression doesn't clobber other flags like Deleted
	entry := FooterEntry{Flags: 0}
	entry.SetDeleted()
	entry.SetCompression(compression.CodexZstd)

	require.True(t, entry.IsDeleted())
	require.True(t, entry.IsCompressed())
	require.Equal(t, compression.CodexZstd, entry.Compression())

	// Unset compression
	entry.SetCompression(compression.CodexNone)
	require.False(t, entry.IsCompressed())
	require.True(t, entry.IsDeleted()) // Deleted bit must remain
}

func TestSegmentFooter_InvalidRecordCount(t *testing.T) {
	// With 48-byte entries, a 32-byte addition should fail
	buf := make([]byte, SegmentFooterHeaderSize+32)
	binary.LittleEndian.PutUint64(buf[0:8], 1)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(time.Now().Unix()))

	_, err := DecodeSegmentFooter(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid segment footer size")
}

func TestSegmentFooter_ManyEntries_LargeSlab(t *testing.T) {
	// Create 5000 entries to test metadata block crossing multiple pages
	entries := make([]FooterEntry, 5000)
	for i := 0; i < 5000; i++ {
		entries[i] = FooterEntry{
			Hash:         uint64(i),
			Pos:          int64(i * 4096),
			LogicalSize:  8192,
			PhysicalSize: 2048,
			Flags:        0,
		}
		entries[i].SetCompression(compression.CodexZstd)
	}

	sf := SegmentFooter{
		Entries:   entries,
		SegmentID: 500,
		CTime:     time.Now().Unix(),
	}

	footerBytes := AppendSegmentFooterWithTail(nil, sf)

	// The metadata block should be (16 + (5000 * 48) + 20) = 240,036 bytes.
	// Rounded to 4KB: 240,640 bytes.
	require.Equal(t, int(SegmentFooterPhysicalSize(5000)), len(footerBytes))

	decodedSF, err := decodeFooterSection(footerBytes)
	require.NoError(t, err)
	require.Equal(t, 5000, len(decodedSF.Entries))
	require.Equal(t, compression.CodexZstd, decodedSF.Entries[4999].Compression())
}

func TestSegmentFooter_RoundTrip_LargeValues(t *testing.T) {
	// Test with large hash/size values
	sf := SegmentFooter{
		Entries: []FooterEntry{
			{Hash: 0xFFFFFFFFFFFFFFFF, Pos: 0, LogicalSize: 0x7FFFFFFFFFFFFFFF, PhysicalSize: 0x7FFFFFFFFFFFFFFF, Flags: 0xFFFFFFFF},
			{Hash: 0x0000000000000000, Pos: 1000, LogicalSize: 0, PhysicalSize: 0, Flags: 0},
			{Hash: 0x7FFFFFFFFFFFFFFF, Pos: 2000, LogicalSize: 0x7FFFFFFFFFFFFFFF, PhysicalSize: 0x7FFFFFFFFFFFFFFF, Flags: 0x80000000},
		},
		SegmentID: 1,
		CTime:     2147483647, // Max 32-bit timestamp
	}

	footerBytes := AppendSegmentFooterWithTail(nil, sf)

	// Prepend some data
	data := make([]byte, 100)
	segmentData := append(data, footerBytes...)

	decodedSF, err := decodeFooterSection(segmentData)
	require.NoError(t, err)
	require.Equal(t, sf.SegmentID, decodedSF.SegmentID)
	require.Equal(t, 3, len(decodedSF.Entries))
	require.Equal(t, sf.CTime, decodedSF.CTime)

	for i := range sf.Entries {
		require.Equal(t, sf.Entries[i], decodedSF.Entries[i])
	}
}

func TestReadSegmentFooterFromFile(t *testing.T) {
	// Create a temporary file with a valid segment footer
	tmpFile, err := os.CreateTemp("", "segment-footer-test-*.seg")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create test footer
	sf := SegmentFooter{
		Entries: []FooterEntry{
			{Hash: 0x123, Pos: 0, LogicalSize: 1000, PhysicalSize: 1000},
			{Hash: 0x456, Pos: 1000, LogicalSize: 2000, PhysicalSize: 1500},
		},
		SegmentID: 42,
		CTime:     time.Now().Unix(),
	}

	// Write some data before the footer (simulating segment data)
	dataPrefixSize := int64(8192)
	dataPrefix := make([]byte, dataPrefixSize)
	_, err = tmpFile.Write(dataPrefix)
	require.NoError(t, err)

	// Write the footer
	footerBytes := AppendSegmentFooterWithTail(nil, sf)
	_, err = tmpFile.Write(footerBytes)
	require.NoError(t, err)

	// Sync and get file size
	require.NoError(t, tmpFile.Sync())
	stat, err := tmpFile.Stat()
	require.NoError(t, err)

	// Read it back
	// ReadSegmentFooterFromFile returns (footer, metadataBlockStart, error)
	// metadataBlockStart is the offset where the metadata block begins
	decodedSF, metadataBlockStart, err := ReadSegmentFooterFromFile(tmpFile, stat.Size(), 42)
	require.NoError(t, err)
	// The metadata block starts right after the data prefix
	require.Equal(t, dataPrefixSize, metadataBlockStart)
	require.Equal(t, sf.SegmentID, decodedSF.SegmentID)
	require.Equal(t, len(sf.Entries), len(decodedSF.Entries))

	for i := range sf.Entries {
		require.Equal(t, sf.Entries[i].Hash, decodedSF.Entries[i].Hash)
		require.Equal(t, sf.Entries[i].LogicalSize, decodedSF.Entries[i].LogicalSize)
	}
}

func TestFooterEntry_Checksum(t *testing.T) {
	// Test default state - invalid CRC flag is set by default (FlagInvalidCRC)
	entry := FooterEntry{Hash: 123, Pos: 0, LogicalSize: 1000, Flags: FlagInvalidCRC}
	require.False(t, entry.HasChecksum())
	require.Equal(t, uint32(0), entry.Checksum())

	// Test with checksum set via Flags field directly
	// Clear FlagInvalidCRC and set CRC value in lower 32 bits
	entry.Flags = uint64(0xDEADBEEF) // CRC in lower 32 bits, FlagInvalidCRC cleared
	require.True(t, entry.HasChecksum())
	require.Equal(t, uint32(0xDEADBEEF), entry.Checksum())
}

func TestFooterEntry_SeqID(t *testing.T) {
	entry := FooterEntry{Hash: 123, SeqID: 999}
	require.Equal(t, uint64(999), entry.SeqID)

	// Round-trip
	buf := AppendFooterEntry(nil, entry)
	decoded, err := DecodeFooterEntry(buf)
	require.NoError(t, err)
	require.Equal(t, uint64(999), decoded.SeqID)
}
