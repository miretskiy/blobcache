package sys

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Block alignment constants for O_DIRECT I/O.
// All O_DIRECT operations require memory address, write size, and file offset
// to be aligned to BlockSize (4KB on most systems).
const (
	BlockSize = 4096
	BlockMask = BlockSize - 1
)

// PageAlign rounds size up to the nearest BlockSize (4KB) boundary.
func PageAlign(size int64) int64 {
	return (size + BlockMask) &^ BlockMask
}

// AllocAligned allocates a byte slice with 4KB-aligned memory address.
// Uses mmap with MAP_ANONYMOUS for guaranteed alignment (mmap always returns
// page-aligned memory). The returned slice is pre-warmed to force physical
// RAM commitment.
//
// The returned buffer size is rounded UP to the nearest page boundary (4KB).
// This is optimal for O_DIRECT I/O which requires page-aligned memory address,
// write size, and file offset. Callers needing exactly N bytes can reslice.
//
// IMPORTANT: The buffer is NOT managed by Go's GC. Callers MUST call
// FreeAligned to release the memory, otherwise it will leak.
func AllocAligned(size int) []byte {
	alignedSize := int(PageAlign(int64(size)))
	data, err := unix.Mmap(-1, 0, alignedSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Sprintf("sys: failed to allocate %d aligned bytes: %v", size, err))
	}

	// Pre-warm: force physical RAM commitment
	for i := 0; i < len(data); i += BlockSize {
		data[i] = 0
	}

	// Return full page-aligned slice so callers can use the entire allocation
	return data
}

// FreeAligned releases memory allocated by AllocAligned.
// After calling this, the slice must not be used.
func FreeAligned(buf []byte) {
	if len(buf) == 0 {
		return
	}
	// Round up to get original allocation size
	alignedSize := int(PageAlign(int64(cap(buf))))
	// Reslice to original capacity for munmap
	_ = unix.Munmap(buf[:alignedSize])
}

// GrowAligned grows an aligned buffer to at least newSize bytes.
// If the current buffer has sufficient capacity, returns it unchanged.
// Otherwise, allocates a new aligned buffer and frees the old one.
// The new buffer's contents are undefined (not zeroed beyond pre-warming).
func GrowAligned(buf []byte, newSize int) []byte {
	if cap(buf) >= newSize {
		return buf[:newSize]
	}
	// Free old buffer if it was allocated
	if len(buf) > 0 {
		FreeAligned(buf)
	}
	return AllocAligned(newSize)
}
