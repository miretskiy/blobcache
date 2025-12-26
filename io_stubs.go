//go:build !linux && !darwin

package blobcache

import (
	"os"
	"unsafe"

	"github.com/ncw/directio"
)

// fdatasync is a no-op on unsupported platforms
func fdatasync(f *os.File) error {
	return f.Sync() // Fall back to full sync
}

// isAligned checks if block is aligned in memory for DirectIO
func isAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	alignment := int(uintptr(unsafe.Pointer(&block[0])) & uintptr(directio.AlignSize-1))
	return alignment == 0
}

// fallocate is a no-op on unsupported platforms
func fallocate(f *os.File, size int64) error {
	return nil // No pre-allocation support
}

// PunchHole is a no-op on unsupported platforms
// Space will not be reclaimed until segment compaction occurs
func PunchHole(f *os.File, offset, length int64) error {
	return nil // No hole punching support
}
