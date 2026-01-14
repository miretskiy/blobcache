package sys

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

func TestFallocate_FileSize(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "fallocate_test.bin")

	// Create file using OpenDirect with DirectIO enabled
	f, err := OpenDirect(path, true)
	require.NoError(t, err)
	defer f.Close()

	// Check initial size
	info, err := f.Stat()
	require.NoError(t, err)
	t.Logf("Initial file size: %d", info.Size())
	require.Equal(t, int64(0), info.Size(), "newly created file should be 0 bytes")

	// Pre-allocate 16MB
	allocSize := int64(16 * 1024 * 1024)
	err = Fallocate(f, allocSize)
	if err != nil {
		t.Logf("fallocate returned error (may be expected on some filesystems): %v", err)
	}

	// Check size after fallocate
	info, err = f.Stat()
	require.NoError(t, err)
	t.Logf("File size after fallocate(%d): %d", allocSize, info.Size())

	// Write some data at position 0
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	n, err := f.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)

	// Check size after write
	info, err = f.Stat()
	require.NoError(t, err)
	t.Logf("File size after WriteAt(4096 bytes at offset 0): %d", info.Size())

	// Write at offset 8192
	n, err = f.WriteAt(data, 8192)
	require.NoError(t, err)
	require.Equal(t, 4096, n)

	info, err = f.Stat()
	require.NoError(t, err)
	t.Logf("File size after WriteAt(4096 bytes at offset 8192): %d", info.Size())

	// Close and re-check with os.Stat
	require.NoError(t, f.Close())

	info, err = os.Stat(path)
	require.NoError(t, err)
	t.Logf("Final file size (after close): %d", info.Size())
}

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
			gotOffset, gotLength, gotCanPunch := AlignForHolePunch(tt.offset, tt.length)

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
