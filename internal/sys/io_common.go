package sys

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"unsafe"
)

// IsAligned checks if block is aligned in memory for DirectIO.
func IsAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	return uintptr(unsafe.Pointer(&block[0]))&uintptr(BlockMask) == 0
}

// OpenFlag is a bitBlockMask controlling file opening behavior.
type OpenFlag int

const (
	// FlDirectIO bypasses the page cache (O_DIRECT on Linux, F_NOCACHE on Darwin).
	FlDirectIO OpenFlag = 1 << iota

	// FlDSync ensures data is synced before write returns (O_DSYNC on Linux).
	// On Darwin, this flag triggers explicit Fdatasync after writes.
	FlDSync

	// FlSync ensures data AND metadata are synced (O_SYNC on Linux).
	// On Darwin, this flag triggers explicit Sync after writes.
	FlSync
)

// Convenience flag combinations for common use cases.
const (
	// SyncNone opens without any sync flags (testing only).
	SyncNone OpenFlag = 0

	// SyncData opens with FlDSync for data durability.
	SyncData OpenFlag = FlDSync

	// SyncFull opens with FlSync for full durability (data + metadata).
	SyncFull OpenFlag = FlSync
)

// ErrAlignment is returned when data is not properly aligned for O_DIRECT.
var ErrAlignment = errors.New("data buffer not aligned for O_DIRECT")

// SyncFile syncs the file based on flags.
// On Linux, O_DSYNC/O_SYNC at open time handles sync; on Darwin, explicit calls are needed.
func SyncFile(f *os.File, flags OpenFlag) error {
	if !RequiresExplicitSync {
		return nil
	}
	if flags&FlSync != 0 {
		return f.Sync()
	}
	if flags&FlDSync != 0 {
		return Fdatasync(f)
	}
	return nil
}

// WriteBulkAligned atomically writes aligned data to a new file.
// data must be 4KB-aligned if FlDirectIO is set.
func WriteBulkAligned(path string, data []byte, flags OpenFlag) (retErr error) {
	if flags&FlDirectIO != 0 && !IsAligned(data) {
		return ErrAlignment
	}

	f, err := CreateFile(path, flags)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()

	if err := Fallocate(f, int64(len(data))); err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		return err
	}

	return SyncFile(f, flags)
}

type FadviseHint int

const (
	FadvSequential FadviseHint = iota
	FadvDontNeed
)

type Offset_t int64

// AlignForHolePunch aligns offset and length to filesystem block boundaries
// Returns (alignedOffset, alignedLength, canPunch)
// canPunch is false if there are no complete blocks to punch
func AlignForHolePunch(offset, length int64) (int64, int64, bool) {
	// Round offset UP to next block boundary (don't punch into previous blob)
	alignedOffset := (offset + BlockMask) &^ BlockMask
	length -= alignedOffset - offset

	// Skip if blob smaller than one block after adjustment
	if length < BlockSize {
		return 0, 0, false
	}

	// Round length DOWN to block multiple (don't punch into next blob)
	length &^= BlockMask

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
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
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
