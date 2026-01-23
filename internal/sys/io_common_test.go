package sys

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFallocate_FileSize(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "fallocate_test.bin")

	f, err := CreateFile(path, FlDirectIO)
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

func TestFdatasync(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "fdatasync_test.bin")

	// Test 1: Normal file (buffered I/O)
	t.Run("BufferedIO", func(t *testing.T) {
		f, err := os.Create(path)
		require.NoError(t, err)
		defer f.Close()

		// Write some data
		data := []byte("hello fdatasync test")
		_, err = f.Write(data)
		require.NoError(t, err)

		// Call Fdatasync - should succeed
		err = Fdatasync(f)
		require.NoError(t, err, "Fdatasync should succeed on buffered file")
	})

	t.Run("DirectIO", func(t *testing.T) {
		directPath := filepath.Join(tmpDir, "fdatasync_direct.bin")
		f, err := CreateFile(directPath, FlDirectIO)
		require.NoError(t, err)
		defer f.Close()

		// Write aligned data (O_DIRECT requires alignment)
		data := make([]byte, 4096)
		for i := range data {
			data[i] = byte(i % 256)
		}
		_, err = f.WriteAt(data, 0)
		require.NoError(t, err)

		// Call Fdatasync
		err = Fdatasync(f)
		require.NoError(t, err, "Fdatasync should succeed on O_DIRECT file")
	})

	// Test 3: Multiple writes then sync
	t.Run("MultipleWritesThenSync", func(t *testing.T) {
		multiPath := filepath.Join(tmpDir, "fdatasync_multi.bin")
		f, err := os.Create(multiPath)
		require.NoError(t, err)
		defer f.Close()

		// Multiple writes
		for i := 0; i < 100; i++ {
			_, err = f.Write([]byte("line of data\n"))
			require.NoError(t, err)
		}

		// Single Fdatasync at the end
		err = Fdatasync(f)
		require.NoError(t, err, "Fdatasync should succeed after multiple writes")

		// Verify file size
		info, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(100*13), info.Size()) // 13 bytes per line
	})

	// Test 4: Read mode (similar to WAL)
	t.Run("AppendMode", func(t *testing.T) {
		appendPath := filepath.Join(tmpDir, "fdatasync_append.bin")

		// Create initial file
		f, err := os.Create(appendPath)
		require.NoError(t, err)
		f.Write([]byte("initial data\n"))
		f.Close()

		// Reopen in append mode
		f, err = os.OpenFile(appendPath, os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		defer f.Close()

		// Read data
		_, err = f.Write([]byte("appended data\n"))
		require.NoError(t, err)

		// Fdatasync
		err = Fdatasync(f)
		require.NoError(t, err, "Fdatasync should succeed on append-mode file")
	})
}

func TestWritev_AppendMode(t *testing.T) {
	// Test net.Buffers.WriteTo with O_APPEND (WAL pattern)
	// This mimics what the WAL does for gathered I/O
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "writev_append.bin")

	// Open in append mode (like WAL)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Create multiple buffers (like WAL batch)
	var buffers net.Buffers
	for i := 0; i < 100; i++ {
		buf := make([]byte, 1024) // 1KB per record
		for j := range buf {
			buf[j] = byte((i + j) % 256)
		}
		buffers = append(buffers, buf)
	}

	// Write all buffers at once (writev)
	n, err := buffers.WriteTo(f)
	require.NoError(t, err, "WriteTo should succeed")
	require.Equal(t, int64(100*1024), n, "should write all buffers")

	// Fdatasync
	err = Fdatasync(f)
	require.NoError(t, err)

	// Verify file size
	info, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(100*1024), info.Size())
}

func TestWritev_LargeBatch(t *testing.T) {
	// Test with large number of buffers (exceeding IOV_MAX)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "writev_large.bin")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	defer f.Close()

	// Create 2000 buffers (exceeds IOV_MAX of 1024 on Linux)
	var buffers net.Buffers
	for i := 0; i < 2000; i++ {
		buf := make([]byte, 512)
		for j := range buf {
			buf[j] = byte((i + j) % 256)
		}
		buffers = append(buffers, buf)
	}

	// Go's net.Buffers should handle IOV_MAX chunking
	n, err := buffers.WriteTo(f)
	require.NoError(t, err, "WriteTo should handle large batch")
	require.Equal(t, int64(2000*512), n)

	err = Fdatasync(f)
	require.NoError(t, err)
}

func TestAlignForHolePunch(t *testing.T) {
	tests := []struct {
		name           string
		offset, length int64
		expectOffset   int64
		expectLength   int64
		expectCanPunch bool
	}{
		{
			name:           "Perfect alignment",
			offset:         BlockSize,
			length:         BlockSize,
			expectOffset:   BlockSize,
			expectLength:   BlockSize,
			expectCanPunch: true,
		},
		{
			name:           "Sub-block length",
			offset:         0,
			length:         BlockSize - 1,
			expectOffset:   0,
			expectLength:   0,
			expectCanPunch: false,
		},
		{
			name:           "Offset=1, Length=4096 (rounds UP, becomes 0)",
			offset:         1,
			length:         BlockSize,
			expectOffset:   BlockSize, // Rounded UP
			expectLength:   0,         // Nothing left after rounding
			expectCanPunch: false,
		},
		{
			name:           "Offset just past page (4097)",
			offset:         BlockSize + 1,
			length:         BlockSize,
			expectOffset:   2 * BlockSize, // Round UP to 8192
			expectLength:   0,             // Nothing left
			expectCanPunch: false,
		},
		{
			name:           "Large blob with small misalignment",
			offset:         100,
			length:         3*BlockSize + 200, // 12,488 bytes
			expectOffset:   BlockSize,         // Round UP from 100 to 4096
			expectLength:   2 * BlockSize,     // 8192 (loses ~4KB to alignment)
			expectCanPunch: true,
		},
		{
			name:           "Exactly 2 blocks, offset=1",
			offset:         1,
			length:         2 * BlockSize,
			expectOffset:   BlockSize,
			expectLength:   BlockSize, // Only 1 block fits after rounding
			expectCanPunch: true,
		},
		{
			name:           "Large aligned punch",
			offset:         10 * BlockSize,
			length:         100 * BlockSize,
			expectOffset:   10 * BlockSize,
			expectLength:   100 * BlockSize,
			expectCanPunch: true,
		},
		{
			name:           "End-of-file scenario (offset near end)",
			offset:         100*BlockSize - 10, // 10 bytes before page boundary
			length:         BlockSize + 100,    // Extends past boundary
			expectOffset:   100 * BlockSize,    // Round UP
			expectLength:   BlockSize,          // Exactly 1 block
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
				require.EqualValues(t, 0, gotOffset%BlockSize, "offset must be block-aligned")
				require.EqualValues(t, 0, gotLength%BlockSize, "length must be block-aligned")
				require.GreaterOrEqual(t, gotLength, int64(BlockSize), "length must be at least 1 block")
			}
		})
	}
}

func TestWriteBulkAligned(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("aligned data succeeds", func(t *testing.T) {
		path := filepath.Join(tmpDir, "aligned.bin")
		data := AllocAligned(8192)
		for i := range data {
			data[i] = byte(i % 256)
		}

		err := WriteFile(path, data, FlDirectIO|SyncData)
		require.NoError(t, err)

		// Verify file contents
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, data, got)
	})

	t.Run("unaligned data with DirectIO fails", func(t *testing.T) {
		path := filepath.Join(tmpDir, "unaligned.bin")
		// Create definitely unaligned buffer by taking a sub-slice starting at offset 1
		buf := AllocAligned(8192)
		data := buf[1:4097]

		err := WriteFile(path, data, FlDirectIO)
		require.ErrorIs(t, err, ErrAlignment)
	})

	t.Run("unaligned data without DirectIO succeeds", func(t *testing.T) {
		path := filepath.Join(tmpDir, "unaligned_nodirect.bin")
		data := []byte("hello world")

		err := WriteFile(path, data, SyncNone)
		require.NoError(t, err)

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, data, got)
	})
}
