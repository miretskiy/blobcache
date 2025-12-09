package blobcache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentWriter_Buffered(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create segments directory
	segDir := filepath.Join(tmpDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// Create segment writer with 10KB segments
	writer := NewSegmentWriter(tmpDir, 10*1024, false, false, 0)
	defer writer.Close()

	// Write multiple small values to same segment
	key1 := []byte("key1")
	value1 := make([]byte, 1000)
	err = writer.Write(key1, value1)
	require.NoError(t, err)
	pos1 := writer.Pos()
	require.NotEqual(t, 0, pos1.SegmentID) // Timestamp-based segment ID
	require.Equal(t, int64(0), pos1.Pos)

	key2 := []byte("key2")
	value2 := make([]byte, 2000)
	err = writer.Write(key2, value2)
	require.NoError(t, err)
	pos2 := writer.Pos()
	require.Equal(t, pos1.SegmentID, pos2.SegmentID) // Same segment
	require.Equal(t, int64(1000), pos2.Pos)          // After first value

	// Write large value to trigger rotation
	key3 := []byte("key3")
	value3 := make([]byte, 15000) // Exceeds 10KB segment size
	err = writer.Write(key3, value3)
	require.NoError(t, err)
	pos3 := writer.Pos()
	require.NotEqual(t, pos1.SegmentID, pos3.SegmentID) // New segment
	require.Equal(t, int64(0), pos3.Pos)                // Start of new segment
}

func TestSegmentWriter_DirectIO(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-directio-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create segments directory
	segDir := filepath.Join(tmpDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// Create segment writer with DirectIO
	writer := NewSegmentWriter(tmpDir, 1024*1024, true, false, 0)
	defer writer.Close()

	// Write value (tests leftover handling)
	key := []byte("test-key")
	value := make([]byte, 5000) // Unaligned size
	for i := range value {
		value[i] = byte(i % 256)
	}

	err = writer.Write(key, value)
	require.NoError(t, err)

	pos := writer.Pos()
	require.Equal(t, int64(0), pos.Pos)

	// Close should flush leftover and truncate
	err = writer.Close()
	require.NoError(t, err)

	// TODO: Verify file size is exactly 5000 bytes (not padded)
}

func TestSegmentWriter_MultipleSegments(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-multi-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create segments directory
	segDir := filepath.Join(tmpDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// Small segment size to trigger multiple segments
	writer := NewSegmentWriter(tmpDir, 5000, false, false, 0)
	defer writer.Close()

	segments := make(map[int64]bool)

	// Write 5 values, should span multiple segments
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := make([]byte, 2000)
		err := writer.Write(key, value)
		require.NoError(t, err)

		pos := writer.Pos()
		segments[pos.SegmentID] = true
	}

	// Should have created multiple segments
	require.Greater(t, len(segments), 1, "Should have created multiple segments")
}
