//go:build darwin

package blobcache

import (
	"os"
	"syscall"
	"unsafe"
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
		Flags:   syscall.F_ALLOCATECONTIG, // Try contiguous first
		Posmode: syscall.F_PEOFPOSMODE,    // Allocate from current EOF
		Offset:  0,
		Length:  size,
	}

	// Try contiguous allocation first
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_PREALLOCATE), uintptr(unsafe.Pointer(&fstore)))
	if errno == 0 {
		return nil
	}

	// Fall back to non-contiguous allocation
	fstore.Flags = syscall.F_ALLOCATEALL
	_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_PREALLOCATE), uintptr(unsafe.Pointer(&fstore)))
	if errno != 0 {
		return errno
	}

	return nil
}
