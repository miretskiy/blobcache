//go:build linux

package blobcache

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

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

// PunchHole deallocates a range within a file (creates sparse file)
// Uses FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE to reclaim space
// Aligns to filesystem block boundaries to avoid punching adjacent blobs
func PunchHole(f *os.File, offset, length int64) error {
	alignedOffset, alignedLength, canPunch := alignForHolePunch(offset, length)
	if !canPunch {
		return nil
	}
	// Mode must be the bitwise OR of PUNCH_HOLE and KEEP_SIZE
	mode := uint32(unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE)

	// Fallocate(fd, mode, offset, length)
	return unix.Fallocate(int(f.Fd()), mode, alignedOffset, alignedLength)
}
