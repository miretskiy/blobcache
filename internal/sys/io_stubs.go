//go:build !linux && !darwin

package sys

import "os"

// UseFadvise indicates whether fadvise is effective on this platform.
const UseFadvise = false

// RequiresAlignment indicates whether O_DIRECT requires 4KB-aligned buffers.
// On unsupported platforms, alignment is not enforced.
const RequiresAlignment = false

// Fdatasync is a no-op on unsupported platforms
func Fdatasync(f *os.File) error {
	return f.Sync() // Fall back to full sync
}

// Fallocate is a no-op on unsupported platforms
func Fallocate(f *os.File, size int64) error {
	return nil // No pre-allocation support
}

// PunchHole is a no-op on unsupported platforms
// Space will not be reclaimed until segment compaction occurs
// Returns 0 bytes reclaimed
func PunchHole(f *os.File, offset, length int64) (int64, error) {
	return 0, nil // No hole punching support
}

// Fadvise is a no-op on unsupported platforms
func Fadvise(fd uintptr, offset Offset_t, length int64, hint FadviseHint) error {
	return nil
}

// RequiresExplicitSync indicates whether explicit sync calls are needed for durable writes.
// On unsupported platforms, we use standard sync.
const RequiresExplicitSync = true

// CreateFile creates a file for writing (no DirectIO support on this platform).
// FlDirectIO and FlDSync/FlSync are ignored; caller must use Fdatasync after writes.
func CreateFile(path string, _ OpenFlag) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

// OpenFileForRead opens an existing file for reading.
// FlDirectIO is ignored on unsupported platforms.
func OpenFileForRead(path string, _ OpenFlag) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY, 0)
}

// PreadAligned reads from a file at the specified offset.
// No alignment enforcement on unsupported platforms.
func PreadAligned(f *os.File, buf []byte, offset int64, _ OpenFlag) (int, error) {
	return f.ReadAt(buf, offset)
}

// CopyFileRange emulates copy_file_range(2) using read/write.
func CopyFileRange(srcFile, dstFile *os.File, srcOff, dstOff *int64, length int) (int, error) {
	return copyFileRangeEmulated(srcFile, dstFile, srcOff, dstOff, length)
}
