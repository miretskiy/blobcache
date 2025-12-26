package blobcache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

// TestPunchHole_Alignment tests that PunchHole correctly aligns ranges
func TestPunchHole_Alignment(t *testing.T) {
	const blockSize = directio.BlockSize // Typically 4096

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")

	// Create test file with some data
	f, err := os.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	// Write 10 blocks of data
	data := make([]byte, blockSize*10)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, err = f.Write(data)
	require.NoError(t, err)

	tests := []struct {
		name   string
		offset int64
		size   int64
	}{
		{
			name:   "exact block aligned",
			offset: 0,
			size:   blockSize,
		},
		{
			name:   "multiple blocks aligned",
			offset: blockSize * 2,
			size:   blockSize * 3,
		},
		{
			name:   "misaligned but large enough",
			offset: 100,
			size:   blockSize * 2,
		},
		{
			name:   "small blob (should skip)",
			offset: 100,
			size:   1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// PunchHole should not return error (either punches or skips if too small)
			err := PunchHole(f, tt.offset, tt.size)
			require.NoError(t, err)
		})
	}
}

// TestAlignForHolePunch tests the alignment helper logic
func TestAlignForHolePunch(t *testing.T) {
	const blockSize = directio.BlockSize // Typically 4096

	tests := []struct {
		name         string
		offset       int64
		size         int64
		wantOff      int64
		wantLen      int64
		wantCanPunch bool
	}{
		{
			name:         "exact block aligned",
			offset:       0,
			size:         blockSize,
			wantOff:      0,
			wantLen:      blockSize,
			wantCanPunch: true,
		},
		{
			name:         "multiple blocks aligned",
			offset:       blockSize * 2,
			size:         blockSize * 3,
			wantOff:      blockSize * 2,
			wantLen:      blockSize * 3,
			wantCanPunch: true,
		},
		{
			name:         "misaligned offset, large size",
			offset:       100,
			size:         blockSize * 2,
			wantOff:      blockSize,
			wantLen:      blockSize,
			wantCanPunch: true,
		},
		{
			name:         "small blob (< blockSize)",
			offset:       100,
			size:         1024,
			wantOff:      0,
			wantLen:      0,
			wantCanPunch: false,
		},
		{
			name:         "crosses boundary but no full block",
			offset:       blockSize - 100,
			size:         200,
			wantOff:      0,
			wantLen:      0,
			wantCanPunch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alignedOff, alignedLen, canPunch := alignForHolePunch(tt.offset, tt.size)

			require.Equal(t, tt.wantCanPunch, canPunch, "canPunch mismatch")
			require.Equal(t, tt.wantOff, alignedOff, "aligned offset mismatch")
			require.Equal(t, tt.wantLen, alignedLen, "aligned length mismatch")

			if canPunch {
				// Verify alignment
				require.Equal(t, int64(0), alignedOff%blockSize, "offset not block-aligned")
				require.Equal(t, int64(0), alignedLen%blockSize, "length not block-aligned")

				// Verify safety: punched range entirely within original range
				require.GreaterOrEqual(t, alignedOff, tt.offset, "punching before blob start (UNSAFE)")
				require.LessOrEqual(t, alignedOff+alignedLen, tt.offset+tt.size, "punching beyond blob end (UNSAFE)")
			}
		})
	}
}
