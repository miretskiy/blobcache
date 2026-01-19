package sys

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsAligned(t *testing.T) {
	// AllocAligned always returns aligned memory
	buf := AllocAligned(4096)
	defer FreeAligned(buf)
	require.True(t, IsAligned(buf), "AllocAligned should return aligned buffer")

	// Subslice starting at offset 0 is still aligned
	require.True(t, IsAligned(buf[:100]), "buf[:100] should be aligned")

	// Empty slice is considered aligned
	require.True(t, IsAligned(nil), "nil should be considered aligned")
	require.True(t, IsAligned([]byte{}), "empty slice should be considered aligned")
}

func TestIsAligned_Unaligned(t *testing.T) {
	if !RequiresAlignment {
		t.Skip("platform does not enforce alignment")
	}

	buf := AllocAligned(4096)
	defer FreeAligned(buf)

	// Subslice starting at offset 1 is NOT aligned
	require.False(t, IsAligned(buf[1:]), "buf[1:] should NOT be aligned")
}

func TestRoundToBlock(t *testing.T) {
	tests := []struct {
		input    int64
		expected int64
	}{
		{0, 0},
		{1, 4096},
		{4095, 4096},
		{4096, 4096},
		{4097, 8192},
		{8192, 8192},
		{12345, 16384},
	}

	for _, tt := range tests {
		result := PageAlign(tt.input)
		require.Equal(t, tt.expected, result, "PageAlign(%d)", tt.input)
	}
}

func TestAllocAligned_Alignment(t *testing.T) {
	// AllocAligned returns a page-aligned buffer of at least the requested size,
	// rounded up to the nearest page boundary. This is optimal for O_DIRECT I/O
	// which requires page-aligned memory, size, and offset.
	tests := []struct {
		request  int
		expected int // page-aligned size
	}{
		{1, 4096},
		{100, 4096},
		{4096, 4096},
		{4097, 8192},
		{8192, 8192},
		{16384, 16384},
	}

	for _, tt := range tests {
		buf := AllocAligned(tt.request)
		require.True(t, IsAligned(buf), "AllocAligned(%d) should be aligned", tt.request)
		require.Equal(t, tt.expected, len(buf),
			"AllocAligned(%d) should return page-aligned length", tt.request)
		FreeAligned(buf)
	}
}
