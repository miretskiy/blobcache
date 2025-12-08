//go:build darwin

package blobcache

import (
	"os"
	"syscall"
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
