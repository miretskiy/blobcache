package blobcache

import (
	"os"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

func TestDirectIO_TruncateAfterAlignedWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "directio-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	filePath := tmpDir + "/test.dat"

	// Open file with DirectIO
	f, err := directio.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()

	// Write aligned block (4096 bytes)
	alignedData := directio.AlignedBlock(directio.BlockSize)
	for i := range alignedData {
		alignedData[i] = byte(i % 256)
	}
	n, err := f.Write(alignedData)
	require.NoError(t, err)
	require.Equal(t, directio.BlockSize, n)

	// Close the file
	err = f.Close()
	require.NoError(t, err)

	// Reopen and truncate to 1 byte
	err = os.Truncate(filePath, 1)
	require.NoError(t, err)

	// Verify truncation worked
	info, err := os.Stat(filePath)
	require.NoError(t, err)
	require.Equal(t, int64(1), info.Size())

	// Verify data is correct
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, []byte{0}, data)
}
