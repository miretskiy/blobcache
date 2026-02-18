// Package iosched provides pluggable I/O scheduling for positioned reads.
//
// Two implementations are provided:
//   - PreadScheduler: synchronous pread(2), zero overhead (default)
//   - URingScheduler: asynchronous io_uring with channel-based coordination
//     and optional SQPOLL (Linux only, experimental)
//
// The io_uring path is opt-in. Use [Available] to check runtime support
// before constructing a [URingScheduler].
package iosched

import "io"

// IOScheduler abstracts positioned reads for pluggable I/O backends.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The buffer passed to ReadAt must remain valid and unmodified until
// ReadAt returns.
type IOScheduler interface {
	// ReadAt performs a positioned read of len(buf) bytes from file
	// descriptor fd at the given byte offset.
	ReadAt(fd int, buf []byte, offset int64) (int, error)

	io.Closer
}
