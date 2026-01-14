//go:build !linux && !darwin

package sys

import (
	"os"
	"unsafe"

	"github.com/ncw/directio"
)

// UseFadvise indicates whether fadvise is effective on this platform.
const UseFadvise = false

// Fdatasync is a no-op on unsupported platforms
func Fdatasync(f *os.File) error {
	return f.Sync() // Fall back to full sync
}

// IsAligned checks if block is aligned in memory for DirectIO
func IsAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	alignment := int(uintptr(unsafe.Pointer(&block[0])) & uintptr(directio.AlignSize-1))
	return alignment == 0
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

// OpenDirect opens specified file for writing (no DirectIO support on this platform)
func OpenDirect(path string, _ bool) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
}
