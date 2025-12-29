//go:build linux

package blobcache

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

// alignmentEnforced checks if the filesystem at path enforces DirectIO alignment
func alignmentEnforced(path string) bool {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_DIRECT|unix.O_TRUNC, 0666)
	if err != nil {
		return false // FS doesn't support O_DIRECT
	}
	defer unix.Close(fd)
	defer os.Remove(path)

	// Try unaligned write
	buf := []byte("bad") // 3 bytes, unaligned
	_, err = unix.Write(fd, buf)

	// EINVAL means alignment is mandatory
	return err == unix.EINVAL
}

// TestSegmentFile_DirectIOReadUnaligned exposes DirectIO read alignment bug
// BlobRecord.Size stores unpadded length, but DirectIO requires aligned reads
func TestSegmentFile_DirectIOReadUnaligned(t *testing.T) {
	t.Logf("directio.BlockSize=%d directio.AlignSize=%d", directio.BlockSize, directio.AlignSize)
	if directio.BlockSize == 0 || directio.AlignSize == 0 {
		t.Skip("DirectIO not supported on this platform")
	}

	// Check if filesystem enforces alignment
	// Try /instance_storage first (real NVMe), fall back to current directory
	testDir := "/instance_storage"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		testDir = "."
	}

	testPath := filepath.Join(testDir, "alignment_test.tmp")
	enforced := alignmentEnforced(testPath)
	t.Logf("Filesystem at %s enforces alignment: %v", testDir, enforced)
	if !enforced {
		t.Skipf("Filesystem at %s does not enforce DirectIO alignment", testDir)
	}

	segPath := filepath.Join(testDir, "test_directio_unaligned.seg")
	defer os.Remove(segPath)

	// Open with O_DIRECT using unix package directly
	fd, err := unix.Open(segPath, unix.O_RDWR|unix.O_CREAT|unix.O_TRUNC|unix.O_DIRECT, 0644)
	require.NoError(t, err, "failed to open with O_DIRECT")
	defer unix.Close(fd)
	t.Logf("Opened with O_DIRECT")

	// Write aligned data
	alignedBuf := directio.AlignedBlock(directio.BlockSize)
	value := []byte("value1")
	copy(alignedBuf, value)
	_, err = unix.Write(fd, alignedBuf)
	require.NoError(t, err, "aligned write should succeed")

	// Test 1: Verify raw unix.Pread fails with unaligned size
	smallBuf := make([]byte, 6) // 6 bytes - unaligned size
	_, err = unix.Pread(fd, smallBuf, 0)
	t.Logf("unix.Pread with size=6: %v", err)
	require.ErrorIs(t, err, unix.EINVAL, "raw Pread should fail")

	// Test 2: Now test SegmentFile.ReadAt (what our code actually uses)
	// Wrap fd in os.File
	file := os.NewFile(uintptr(fd), segPath)
	sf := newSegmentFile(file, 0, metadata.SegmentRecord{SegmentID: 1})

	buf2 := make([]byte, 6)
	_, err = sf.ReadAt(buf2, 0)
	t.Logf("SegmentFile.ReadAt with size=6: %v", err)

	// This is the actual bug - SegmentFile.ReadAt doesn't handle alignment!
	require.Error(t, err, "BUG: SegmentFile.ReadAt should fail on unaligned DirectIO read")
}
