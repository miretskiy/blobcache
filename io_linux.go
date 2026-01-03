//go:build linux

package blobcache

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

var defaultIOConfig = IOConfig{
	FDataSync: false,
	Fadvise:   true,
}

// fdatasync syncs file data to disk without syncing metadata
// Uses fdatasync(2) on Linux for better performance than fsync
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// isAligned checks if the memory address of the slice is on a 4KB boundary.
func isAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	// 4095 is the mask for 4096-byte alignment.
	return uintptr(unsafe.Pointer(&block[0]))&4095 == 0
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

// Fadvise maps the internal FadviseHint to Linux-specific posix_fadvise constants.
func Fadvise(fd uintptr, offset Offset_t, length int64, hint FadviseHint) error {
	var linuxHint int
	switch hint {
	case FadvSequential:
		linuxHint = syscall.POSIX_FADV_SEQUENTIAL
	case FadvDontNeed:
		// On Linux, we use DONTNEED.
		// If you want to be extra aggressive, you can also call NOREUSE,
		// but DONTNEED is the standard for releasing Page Cache.
		linuxHint = syscall.POSIX_FADV_DONTNEED
	default:
		return nil
	}

	// Signature: fd, offset, length, advice
	return syscall.PosixFadvise(int(fd), offset, length, linuxHint)
}

// OpenWriter opens specified file for writing with O_DIRECT.
func OpenWriter(path string) (*os.File, error) {
	// We use unix.O_DIRECT for hardware-aligned zero-copy.
	// We also use O_WRONLY because the writer handle is append-only.
	// We do NOT use O_APPEND because we use WriteAt for precise positioning.
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|unix.O_DIRECT, 0644)
}
