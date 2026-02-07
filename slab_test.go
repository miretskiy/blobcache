package blobcache

import (
	"testing"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
)

// TestActiveSlab_AllocBlockAlignment verifies that Alloc always returns
// block-aligned offsets, enabling XFS reflinks during compaction.
func TestActiveSlab_AllocBlockAlignment(t *testing.T) {
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

	sizes := []int{1, 42, 100, 4096, 4097, 8000, 16384, 59}
	for _, size := range sizes {
		data, offset := as.Alloc(size)
		require.NotNil(t, data, "Alloc(%d) returned nil", size)
		require.Zero(t, offset%sys.BlockSize,
			"Alloc(%d) returned unaligned offset %d", size, offset)
		require.Equal(t, size, len(data),
			"Alloc(%d) returned wrong buffer length", size)
	}
}

// TestActiveSlab_AllocFirstRecordAt4096 verifies the first record starts
// at offset 4096 (past the padded file header), not at FileHeaderSize (8).
func TestActiveSlab_AllocFirstRecordAt4096(t *testing.T) {
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
	require.Equal(t, int64(sys.BlockSize), offset,
		"first record should start at BlockSize (4096), got %d", offset)
}
