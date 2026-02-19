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

import (
	"io"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// IOScheduler abstracts positioned reads for pluggable I/O backends.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The buffer passed to ReadAt must remain valid and unmodified until
// ReadAt returns.
type IOScheduler interface {
	// ReadAt performs a positioned read of len(buf) bytes from file
	// descriptor fd at the given byte offset.
	ReadAt(fd int, buf []byte, offset int64) (int, error)

	// Stats returns a snapshot of scheduler statistics.
	Stats() Stats

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

// Stats reports I/O scheduler statistics.
type Stats struct {
	// ReadLatency is a snapshot of disk read latency (may be nil if
	// the scheduler was not constructed via its proper constructor).
	ReadLatency *hdrhistogram.Histogram

	// Batching stats (populated by URingScheduler, zero for pread).
	Batches  int64   // number of SubmitAndWait calls
	Requests int64   // total requests submitted
	MaxBatch int64   // largest single batch observed
	AvgBatch float64 // average batch size
}

// readLatency tracks per-read I/O latency using a mutex-protected HDR
// histogram. Embedded by both PreadScheduler and URingScheduler;
// initialized in their constructors. The mutex cost (~50ns) is
// negligible relative to disk I/O.
type readLatency struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

func (r *readLatency) initLatency() {
	r.hist = hdrhistogram.New(1_000, 10_000_000_000, 3)
}

// latencySnapshot returns a copy of the histogram, or nil if not initialized.
func (r *readLatency) latencySnapshot() *hdrhistogram.Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hist == nil {
		return nil
	}
	return hdrhistogram.Import(r.hist.Export())
}

func (r *readLatency) recordRead(start time.Time) {
	if r.hist == nil {
		return
	}
	ns := time.Since(start).Nanoseconds()
	r.mu.Lock()
	r.hist.RecordValue(ns)
	r.mu.Unlock()
}
