//go:build darwin

package blobcache

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// fdatasync syncs file data to disk
// Darwin doesn't have fdatasync, so we use F_FULLFSYNC which ensures
// data reaches physical disk (not just drive cache)
func fdatasync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_FULLFSYNC), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// isAligned always returns true on macOS since AlignSize is 0
func isAligned(block []byte) bool {
	return true
}

// fallocate pre-allocates disk space for a file
// Darwin uses F_PREALLOCATE via fcntl
func fallocate(f *os.File, size int64) error {
	// fstore_t structure for F_PREALLOCATE
	fstore := syscall.Fstore_t{
		Posmode: syscall.F_PEOFPOSMODE, // Allocate from current EOF
		Offset:  0,
		Length:  size,
	}

	// Try contiguous allocation first
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		f.Fd(),
		uintptr(syscall.F_PREALLOCATE),
		uintptr(unsafe.Pointer(&fstore)),
	)
	if errno == 0 {
		return nil
	}

	// Fall back to non-contiguous allocation
	fstore.Flags = syscall.F_ALLOCATEALL
	_, _, errno = syscall.Syscall(
		syscall.SYS_FCNTL,
		f.Fd(),
		uintptr(syscall.F_PREALLOCATE),
		uintptr(unsafe.Pointer(&fstore)),
	)
	if errno != 0 {
		return errno
	}

	return nil
}

// fpunchhole_t matches the C struct used by fcntl(F_PUNCHHOLE)
type fpunchhole_t struct {
	FP_flags  uint32
	FP_offset int64
	FP_length int64
}

// PunchHole deallocates a range within a file (creates sparse file)
// Uses F_PUNCHHOLE to reclaim space on macOS (requires APFS)
// Aligns to filesystem block boundaries to avoid punching adjacent blobs.
func PunchHole(f *os.File, offset, length int64) error {
	alignedOffset, alignedLength, canPunch := alignForHolePunch(offset, length)
	if !canPunch {
		return nil
	}

	ph := fpunchhole_t{
		FP_flags:  0, // Must be 0
		FP_offset: alignedOffset,
		FP_length: alignedLength,
	}

	_, _, errno := syscall.Syscall(
		unix.SYS_FCNTL,
		f.Fd(),
		uintptr(unix.F_PUNCHHOLE),
		uintptr(unsafe.Pointer(&ph)),
	)

	if errno != 0 {
		return errno
	}
	return nil
}
