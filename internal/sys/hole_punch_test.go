package sys

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

func TestPunchHole_DataIntegrity(t *testing.T) {
	const blockSize = int64(directio.BlockSize)
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "integrity.dat")

	// 1. Create a file with 3 blocks of distinct data: [AAAA...][BBBB...][CCCC...]
	f, err := os.OpenFile(testFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer f.Close()

	blockA := bytes.Repeat([]byte{0xA1}, int(blockSize))
	blockB := bytes.Repeat([]byte{0xB2}, int(blockSize))
	blockC := bytes.Repeat([]byte{0xC3}, int(blockSize))

	_, err = f.Write(append(append(blockA, blockB...), blockC...))
	require.NoError(t, err)
	f.Sync()

	// 2. Measure physical blocks BEFORE hole punch
	fiBefore, err := f.Stat()
	require.NoError(t, err)
	statBefore := fiBefore.Sys().(*syscall.Stat_t)
	blocksBefore := statBefore.Blocks
	t.Logf("BEFORE punch: %d blocks (%.2f KB)", blocksBefore, float64(blocksBefore*512)/1024)

	// 3. Punch only the middle block (Block B)
	// We use exactly aligned offsets to ensure the OS *must* punch it if supported.
	reclaimed, err := PunchHole(f, blockSize, blockSize)
	require.NoError(t, err)
	t.Logf("PunchHole returned: %d bytes reclaimed (requested %d)", reclaimed, blockSize)
	require.Equal(t, blockSize, reclaimed, "Aligned punch should reclaim full requested amount")
	f.Sync()

	// 4. Measure physical blocks AFTER hole punch
	// Compare f.Stat() (via handle) vs os.Stat() (direct filesystem)
	fiHandle, err := f.Stat()
	require.NoError(t, err)
	blocksHandle := fiHandle.Sys().(*syscall.Stat_t).Blocks

	fiPath, err := os.Stat(testFile)
	require.NoError(t, err)
	blocksPath := fiPath.Sys().(*syscall.Stat_t).Blocks

	t.Logf("AFTER (f.Stat):  %d blocks", blocksHandle)
	t.Logf("AFTER (os.Stat): %d blocks", blocksPath)
	t.Logf("Reclaimed: %d blocks (%.2f KB)", blocksBefore-blocksPath,
		float64((blocksBefore-blocksPath)*512)/1024)

	if blocksHandle != blocksPath {
		t.Errorf("DISCREPANCY: f.Stat=%d, os.Stat=%d - metadata cached!",
			blocksHandle, blocksPath)
	}

	expectedBlocks := (blockSize * 2) / 512
	require.Equal(t, expectedBlocks, blocksPath,
		"Hole punch failed: expected %d blocks, got %d", expectedBlocks, blocksPath)
	// 4. Verify Data: Neighbors must be intact, punched area must be zeroes.
	buf := make([]byte, blockSize*3)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)

	require.Equal(t, blockA, buf[0:blockSize], "Block A (neighbor) was corrupted!")
	require.Equal(t, make([]byte, blockSize), buf[blockSize:blockSize*2], "Punched area (Block B) is not zeroed!")
	require.Equal(t, blockC, buf[blockSize*2:], "Block C (neighbor) was corrupted!")
}

func TestPunchHole_PartialBlockSafety(t *testing.T) {
	const blockSize = int64(directio.BlockSize)
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "partial.dat")

	// Create a file with 2 blocks of data
	f, err := os.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	data := bytes.Repeat([]byte{0xFF}, int(blockSize*2))
	_, err = f.Write(data)
	require.NoError(t, err)

	// Attempt to punch a range that spans the boundary but isn't a full block
	// Offset: half of block 0. Length: half a block.
	// This range: [   [XXXX]   ]
	// Result: No full block is contained, so alignForHolePunch should return 0
	midPoint := blockSize / 2
	reclaimed, err := PunchHole(f, midPoint, blockSize/2)
	require.NoError(t, err)
	require.Equal(t, int64(0), reclaimed, "Partial block punch should reclaim 0 bytes")
	t.Logf("Partial punch correctly returned 0 bytes reclaimed")

	// Verify file is still 100% 0xFF (nothing was punched)
	buf := make([]byte, blockSize*2)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf, "Partial block punch accidentally cleared data!")
}

func TestPunchHole_NonAlignedReclamation(t *testing.T) {
	const blockSize = int64(directio.BlockSize)
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "non-aligned.dat")

	f, err := os.OpenFile(testFile, os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Write 10 blocks of data
	data := bytes.Repeat([]byte{0xAA}, int(blockSize*10))
	_, err = f.Write(data)
	require.NoError(t, err)
	f.Sync()

	// Test Case 1: Punch non-aligned blob that spans multiple blocks
	// Blob: offset=100, size=3*blockSize+200 = 12,488
	// Aligned start: (100 + 4095) &^ 4095 = 4096
	// Remaining: 12488 - (4096-100) = 8492
	// Aligned length: 8492 &^ 4095 = 8192 = 2 blocks
	offset1 := int64(100)
	size1 := 3*blockSize + 200
	reclaimed1, err := PunchHole(f, offset1, size1)
	require.NoError(t, err)
	expectedReclaim1 := 2 * blockSize // Alignment loses ~4KB
	require.Equal(t, expectedReclaim1, reclaimed1,
		"Should reclaim 2 full blocks (alignment rounds away the edges)")

	// Test Case 2: Punch blob smaller than one block
	// Alignment should round it to 0
	offset2 := int64(5*blockSize) + 100
	size2 := blockSize - 500
	reclaimed2, err := PunchHole(f, offset2, size2)
	require.NoError(t, err)
	require.Equal(t, int64(0), reclaimed2,
		"Sub-block punch should reclaim 0 bytes")

	// Test Case 3: Perfectly aligned punch
	offset3 := 7 * blockSize
	size3 := 2 * blockSize
	reclaimed3, err := PunchHole(f, offset3, size3)
	require.NoError(t, err)
	require.Equal(t, size3, reclaimed3,
		"Perfectly aligned punch should reclaim full amount")

	t.Logf("Test Case 1: Requested %d, Reclaimed %d", size1, reclaimed1)
	t.Logf("Test Case 2: Requested %d, Reclaimed %d", size2, reclaimed2)
	t.Logf("Test Case 3: Requested %d, Reclaimed %d", size3, reclaimed3)
}
