// Package iosched provides pluggable I/O scheduling for positioned reads.
//
// Two implementations are provided:
//   - PreadScheduler: synchronous pread(2), zero overhead (default)
//   - URingScheduler: asynchronous io_uring with batched submission
//     and optional SQPOLL (Linux only)
//
// The io_uring path is opt-in. Use [IOUringAvailable] to check runtime
// support before constructing a [URingScheduler].
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

// URingConfig configures the io_uring scheduler.
type URingConfig struct {
	// RingDepth is the number of SQ/CQ entries. Must be a power of two.
	// Default: 256.
	RingDepth uint32

	// SQPOLL enables IORING_SETUP_SQPOLL. The kernel spawns a polling
	// thread that continuously checks for new submissions, eliminating
	// the io_uring_enter syscall on the submission path. Burns one CPU
	// core; the kernel thread sleeps after a default idle period (~1s).
	SQPOLL bool
}

// Stats reports batching effectiveness counters for URingScheduler.
type Stats struct {
	Batches  int64   // number of SubmitAndWait calls
	Requests int64   // total requests submitted
	MaxBatch int64   // largest single batch observed
	AvgBatch float64 // average batch size
}
