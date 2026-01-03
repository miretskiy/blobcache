package blobcache

import (
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/ncw/directio"
)

const mask = directio.BlockSize - 1

type FadviseHint int

const (
	FadvSequential FadviseHint = iota
	FadvDontNeed
)

type Offset_t int64

// alignForHolePunch aligns offset and length to filesystem block boundaries
// Returns (alignedOffset, alignedLength, canPunch)
// canPunch is false if there are no complete blocks to punch
func alignForHolePunch(offset, length int64) (int64, int64, bool) {
	// Round offset UP to next block boundary (don't punch into previous blob)
	alignedOffset := (offset + mask) &^ mask
	length -= alignedOffset - offset

	// Skip if blob smaller than one block after adjustment
	if length < directio.BlockSize {
		return 0, 0, false
	}

	// Round length DOWN to block multiple (don't punch into next blob)
	length &^= mask

	return alignedOffset, length, true
}

// IsTransientIOError returns true if the error is likely temporary and
// the operation might succeed if retried. This is used to distinguish
// between "data is gone" and "the system is busy."
func IsTransientIOError(err error) bool {
	if err == nil {
		return false
	}

	// 1. Check for specific transient syscall errors
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EINTR, // Interrupted system call
			syscall.EAGAIN, // Try again
			syscall.EBUSY,  // Device or resource busy
			syscall.EMFILE, // Too many open files (process limit)
			syscall.ENFILE, // Too many open files (system limit)
			syscall.ENOMEM: // Out of memory
			return true
		}
	}

	// 2. Check for network timeouts (if using network-attached storage)
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}

	// 3. Context cancellation or deadline exceeded
	// These are technically transient because the next request might have a fresh context.
	if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.ErrUnexpectedEOF) {
		// Note: ErrUnexpectedEOF is tricky; usually it means the file is corrupted/truncated.
		// We usually treat it as PERMANENT for a specific blob, but transient for the system.
		return false
	}

	return false
}
