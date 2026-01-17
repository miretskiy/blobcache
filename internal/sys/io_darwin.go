//go:build darwin

package sys

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// UseFadvise indicates whether fadvise is effective on this platform.
// Darwin's fadvise equivalent (F_RDAHEAD) is less capable than Linux.
const UseFadvise = false

// RequiresAlignment indicates whether O_DIRECT requires 4KB-aligned buffers.
// Darwin's F_NOCACHE does not enforce strict alignment like Linux O_DIRECT.
const RequiresAlignment = false

// Fdatasync syncs file data to disk
// Darwin doesn't have fdatasync, so we use F_FULLFSYNC which ensures
// data reaches physical disk (not just drive cache)
func Fdatasync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_FULLFSYNC), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// IsAligned always returns true on Darwin as F_NOCACHE does not
// enforce the same strict memory-alignment rules as Linux O_DIRECT.
func IsAligned(_ []byte) bool {
	return true
}

// Fallocate pre-allocates disk space for a file
// Darwin uses F_PREALLOCATE via fcntl
func Fallocate(f *os.File, size int64) error {
	// fstore_t structure for F_PREALLOCATE
	fstore := syscall.Fstore_t{
		Flags:   syscall.F_ALLOCATECONTIG, // Try contiguous first
		Posmode: syscall.F_PEOFPOSMODE,    // Allocate from current EOF
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

	// Fall back to non-contiguous allocation if contiguous failed
	if errno != 0 {
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
	}

	// CRITICAL: Explicitly set the file size (Logical EOF)
	// F_PREALLOCATE only reserves disk blocks but reports size 0.
	return f.Truncate(size)
}

// fpunchhole_t matches the C struct used by fcntl(F_PUNCHHOLE)
type fpunchhole_t struct {
	FP_flags    uint32
	FP_reserved uint32 // Padding for 8-byte alignment
	FP_offset   int64
	FP_length   int64
}

// PunchHole deallocates a range within a file (creates sparse file)
// Uses F_PUNCHHOLE to reclaim space on macOS (requires APFS)
// Aligns to filesystem block boundaries to avoid punching adjacent blobs.
// Returns the actual number of bytes reclaimed (after alignment)
func PunchHole(f *os.File, offset, length int64) (int64, error) {
	alignedOffset, alignedLength, canPunch := AlignForHolePunch(offset, length)
	if !canPunch {
		return 0, nil
	}

	ph := fpunchhole_t{
		FP_flags:    0, // Must be 0
		FP_reserved: 0, // Must be 0
		FP_offset:   alignedOffset,
		FP_length:   alignedLength,
	}

	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		f.Fd(),
		uintptr(unix.F_PUNCHHOLE),
		uintptr(unsafe.Pointer(&ph)),
	)

	if errno != 0 {
		return 0, errno
	}
	return alignedLength, nil
}

// Fadvise on darwin is less flexible than linux in that it's a global, file descriptor
// based operation.  But we keep the same signature as linux (ignoring offset and the length).
func Fadvise(fd uintptr, _ Offset_t, _ int64, hint FadviseHint) error {
	switch hint {
	case FadvDontNeed:
		// F_NOCACHE: 1 turns off, 0 turns on
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_NOCACHE), 1)
		if errno != 0 {
			return errno
		}
		return nil
	case FadvSequential:
		// F_RDAHEAD turns on/off the read-ahead engine.
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_RDAHEAD), 1)
		if errno != 0 {
			return errno
		}
		return nil
	default:
		return nil // Unsupported hints are ignored on Darwin
	}
}

// OpenDirect opens specified file for writing.
// If directIO is true, uses F_NOCACHE to bypass the page cache.
func OpenDirect(path string, directIO bool) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	if directIO {
		// Darwin's equivalent to O_DIRECT (bypassing the Page Cache)
		if _, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return f, nil
}
