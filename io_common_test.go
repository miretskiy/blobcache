package blobcache

import (
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

func TestAlignForHolePunch(t *testing.T) {
	const bs = int64(directio.BlockSize) // 4096

	tests := []struct {
		name           string
		offset, length int64
		expectOffset   int64
		expectLength   int64
		expectCanPunch bool
	}{
		{
			name:           "Perfect alignment",
			offset:         bs,
			length:         bs,
			expectOffset:   bs,
			expectLength:   bs,
			expectCanPunch: true,
		},
		{
			name:           "Sub-block length",
			offset:         0,
			length:         bs - 1,
			expectOffset:   0,
			expectLength:   0,
			expectCanPunch: false,
		},
		{
			name:           "Offset=1, Length=4096 (rounds UP, becomes 0)",
			offset:         1,
			length:         bs,
			expectOffset:   bs, // Rounded UP
			expectLength:   0,  // Nothing left after rounding
			expectCanPunch: false,
		},
		{
			name:           "Offset just past page (4097)",
			offset:         bs + 1,
			length:         bs,
			expectOffset:   2 * bs, // Round UP to 8192
			expectLength:   0,      // Nothing left
			expectCanPunch: false,
		},
		{
			name:           "Large blob with small misalignment",
			offset:         100,
			length:         3*bs + 200, // 12,488 bytes
			expectOffset:   bs,         // Round UP from 100 to 4096
			expectLength:   2 * bs,     // 8192 (loses ~4KB to alignment)
			expectCanPunch: true,
		},
		{
			name:           "Exactly 2 blocks, offset=1",
			offset:         1,
			length:         2 * bs,
			expectOffset:   bs,
			expectLength:   bs, // Only 1 block fits after rounding
			expectCanPunch: true,
		},
		{
			name:           "Large aligned punch",
			offset:         10 * bs,
			length:         100 * bs,
			expectOffset:   10 * bs,
			expectLength:   100 * bs,
			expectCanPunch: true,
		},
		{
			name:           "End-of-file scenario (offset near end)",
			offset:         100*bs - 10, // 10 bytes before page boundary
			length:         bs + 100,    // Extends past boundary
			expectOffset:   100 * bs,    // Round UP
			expectLength:   bs,          // Exactly 1 block
			expectCanPunch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOffset, gotLength, gotCanPunch := alignForHolePunch(tt.offset, tt.length)

			require.Equal(t, tt.expectCanPunch, gotCanPunch, "canPunch mismatch")
			if tt.expectCanPunch {
				require.Equal(t, tt.expectOffset, gotOffset,
					"offset: want %d, got %d", tt.expectOffset, gotOffset)
				require.Equal(t, tt.expectLength, gotLength,
					"length: want %d, got %d", tt.expectLength, gotLength)

				// Verify alignment invariants
				require.Equal(t, int64(0), gotOffset%bs, "offset must be block-aligned")
				require.Equal(t, int64(0), gotLength%bs, "length must be block-aligned")
				require.GreaterOrEqual(t, gotLength, bs, "length must be at least 1 block")
			}
		})
	}
}
