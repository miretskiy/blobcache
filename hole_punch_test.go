package blobcache

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
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
	
	// 2. Punch only the middle block (Block B)
	// We use exactly aligned offsets to ensure the OS *must* punch it if supported.
	err = PunchHole(f, blockSize, blockSize)
	require.NoError(t, err)
	
	// 3. Verify physical reclamation (Linux only)
	if runtime.GOOS == "linux" {
		fi, err := f.Stat()
		require.NoError(t, err)
		stat := fi.Sys().(*syscall.Stat_t)
		// Blocks are reported in 512-byte units.
		// Original size was 3 blocks. After punch, it should be ~2 blocks.
		expectedBlocks := (blockSize * 2) / 512
		require.LessOrEqual(t, stat.Blocks, expectedBlocks, "Physical blocks were not reclaimed")
	}
	
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
	// Offset: half of block 0. LogicalSize: half a block.
	// This range: [   [XXXX]   ]
	// Result: No full block is contained, so alignForHolePunch should say "canPunch: false"
	midPoint := blockSize / 2
	err = PunchHole(f, midPoint, blockSize/2)
	require.NoError(t, err)
	
	// Verify file is still 100% 0xFF (nothing was punched)
	buf := make([]byte, blockSize*2)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf, "Partial block punch accidentally cleared data!")
}
