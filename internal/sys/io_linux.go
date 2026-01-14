//go:build linux

package sys

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// UseFadvise indicates whether fadvise is effective on this platform.
const UseFadvise = true

// Fdatasync syncs file data to disk without syncing metadata
// Uses fdatasync(2) on Linux for better performance than fsync
func Fdatasync(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

// IsAligned checks if the memory address of the slice is on a 4KB boundary.
func IsAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	// 4095 is the mask for 4096-byte alignment.
	return uintptr(unsafe.Pointer(&block[0]))&4095 == 0
}

// Fallocate pre-allocates disk space for a file
// Reduces fragmentation and improves write performance
func Fallocate(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), 0, 0, size)
}

// PunchHole deallocates a range within a file (creates sparse file)
// Uses FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE to reclaim space
// Aligns to filesystem block boundaries to avoid punching adjacent blobs
// Returns the actual number of bytes reclaimed (after alignment)
func PunchHole(f *os.File, offset, length int64) (int64, error) {
	alignedOffset, alignedLength, canPunch := AlignForHolePunch(offset, length)
	if !canPunch {
		return 0, nil
	}
	// Mode must be the bitwise OR of PUNCH_HOLE and KEEP_SIZE
	mode := uint32(unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE)

	// Fallocate(fd, mode, offset, length)
	err := unix.Fallocate(int(f.Fd()), mode, alignedOffset, alignedLength)
	if err != nil {
		return 0, err
	}
	return alignedLength, nil
}

// Fadvise maps the internal FadviseHint to Linux-specific posix_fadvise constants.
func Fadvise(fd uintptr, offset Offset_t, length int64, hint FadviseHint) error {
	var linuxHint int
	switch hint {
	case FadvSequential:
		linuxHint = unix.FADV_SEQUENTIAL
	case FadvDontNeed:
		// On Linux, we use DONTNEED.
		// If you want to be extra aggressive, you can also call NOREUSE,
		// but DONTNEED is the standard for releasing Page Cache.
		linuxHint = unix.FADV_DONTNEED
	default:
		return nil
	}

	// Signature: fd, offset, length, advice
	return unix.Fadvise(int(fd), int64(offset), length, linuxHint)
}

// OpenDirect opens specified file for writing.
// If directIO is true, uses O_DIRECT to bypass the page cache.
func OpenDirect(path string, directIO bool) (*os.File, error) {
	// We use O_WRONLY because the writer handle is append-only.
	// We do NOT use O_APPEND because we use WriteAt for precise positioning.
	flags := os.O_CREATE | os.O_WRONLY
	if directIO {
		flags |= unix.O_DIRECT
	}
	return os.OpenFile(path, flags, 0644)
}
