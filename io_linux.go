//go:build linux

package blobcache

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/ncw/directio"
)

// fdatasync syncs file data to disk without syncing metadata
// Uses fdatasync(2) on Linux for better performance than fsync
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// isAligned checks if block is aligned in memory for DirectIO
func isAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	alignment := int(uintptr(unsafe.Pointer(&block[0])) & uintptr(directio.AlignSize-1))
	return alignment == 0
}

// fallocate pre-allocates disk space for a file
// Reduces fragmentation and improves write performance
func fallocate(f *os.File, size int64) error {
	return syscall.Fallocate(int(f.Fd()), 0, 0, size)
}
