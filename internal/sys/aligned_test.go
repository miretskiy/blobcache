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
		result := RoundToBlock(tt.input)
		require.Equal(t, tt.expected, result, "RoundToBlock(%d)", tt.input)
	}
}

func TestAllocAligned_Alignment(t *testing.T) {
	sizes := []int{1, 100, 4096, 8192, 16384}
	for _, size := range sizes {
		buf := AllocAligned(size)
		require.True(t, IsAligned(buf), "AllocAligned(%d) should be aligned", size)
		require.Equal(t, size, len(buf), "AllocAligned(%d) should have correct length", size)
		FreeAligned(buf)
	}
}
