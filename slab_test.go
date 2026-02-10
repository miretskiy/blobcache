package blobcache

import (
	"testing"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
)

// TestActiveSlab_AllocDensePacking verifies that Alloc packs records densely
// without padding. Records are placed sequentially from wPos.
func TestActiveSlab_AllocDensePacking(t *testing.T) {
	buf := NewMmapBuffer(1 << 20) // 1MB
	defer buf.Unpin()

	as := &ActiveSlab{
		SharedSlab: SharedSlab{
			buf:   buf,
			index: xmap.New[SlabEntry, xmap.Pad32](),
		},
		wPos:       record.FileHeaderSize, // 8 — matches real initialization
		writesDone: newSignal(),
	}

	expectedOffset := int64(record.FileHeaderSize)
	sizes := []int{1, 42, 100, 4096, 4097, 8000, 16384, 59}
	for _, size := range sizes {
		data, offset := as.Alloc(size)
		require.NotNil(t, data, "Alloc(%d) returned nil", size)
		require.Equal(t, expectedOffset, offset,
			"Alloc(%d) should pack densely at offset %d, got %d", size, expectedOffset, offset)
		require.Equal(t, size, len(data),
			"Alloc(%d) returned wrong buffer length", size)
		expectedOffset += int64(size)
	}
}

// TestActiveSlab_AllocFirstRecordAtFileHeader verifies the first record starts
// immediately after the file header (dense packing, no padding).
func TestActiveSlab_AllocFirstRecordAtFileHeader(t *testing.T) {
	buf := NewMmapBuffer(1 << 20)
	defer buf.Unpin()

	as := &ActiveSlab{
		SharedSlab: SharedSlab{
			buf:   buf,
			index: xmap.New[SlabEntry, xmap.Pad32](),
		},
		wPos:       record.FileHeaderSize,
		writesDone: newSignal(),
	}

	_, offset := as.Alloc(100)
	require.Equal(t, int64(record.FileHeaderSize), offset,
		"first record should start at FileHeaderSize (%d), got %d", record.FileHeaderSize, offset)
}
