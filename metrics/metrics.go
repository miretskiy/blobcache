// Package metrics holds process-wide blobcache measurements. No configuration
// is needed. Counters and gauges use Load; histograms use Snapshot for inspection.
package metrics

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// PutDuration covers Cache.Put: inline submission or external I/O completion,
// cache publication, and errors.
// All durations are in nanoseconds.
var PutDuration = newHistogram()

// Read I/O durations come from completed read tickets. Slat covers Submit
// through ring placement; Clat covers placement through completion. Memory hits
// produce no samples. These exclude caller wakeup delay.
var (
	ReadSlat = newHistogram()
	ReadClat = newHistogram()
)

// Open-reader ownership gauges. Byte counts include allocation padding. Readers
// can share buffers with the cache and other readers. Pending I/O may retain a
// buffer after Close. These counts must not be added to estimate unique memory.
var (
	ResidentReadBytes atomic.Int64
	ResidentReaders   atomic.Int64
	DiskReadBytes     atomic.Int64
	DiskReaders       atomic.Int64
)

// Memory reclamation: calls and the bytes they asked for and freed, and queue
// entries visited, evicted, spared for a recent read (second chance), skipped
// while their write is pending, and evicted while a reader still pins them,
// which frees nothing until the reader closes.
var (
	ReclaimCalls          atomic.Int64
	ReclaimRequestedBytes atomic.Int64
	ReclaimReleasedBytes  atomic.Int64
	ReclaimVisited        atomic.Int64
	ReclaimEvicted        atomic.Int64
	ReclaimSpared         atomic.Int64
	ReclaimPending        atomic.Int64
	ReclaimPinned         atomic.Int64
)

// Error counters count failed attempts, including attempts retried by callers.
var (
	WriterErrors atomic.Int64
	PutErrors    atomic.Int64
)

// Histogram serializes access to HDR, which is not itself concurrency-safe.
type Histogram struct {
	mu     sync.Mutex
	values *hdrhistogram.Histogram
}

func newHistogram() *Histogram {
	return &Histogram{values: hdrhistogram.New(1, math.MaxInt64, 3)}
}

// RecordValue records a nonnegative value with three significant digits.
func (h *Histogram) RecordValue(value int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.values.RecordValue(value)
}

// Snapshot returns an independent histogram that the caller may inspect freely.
func (h *Histogram) Snapshot() *hdrhistogram.Histogram {
	h.mu.Lock()
	defer h.mu.Unlock()
	return hdrhistogram.Import(h.values.Export())
}

// Reset clears accumulated durations and errors, preserving live ownership
// gauges. Call once between workloads, after their workers have finished; the
// reset is not an atomic boundary across all measurements during concurrent use.
func Reset() {
	for _, h := range []*Histogram{PutDuration, ReadSlat, ReadClat} {
		h.mu.Lock()
		h.values.Reset()
		h.mu.Unlock()
	}
	WriterErrors.Store(0)
	PutErrors.Store(0)
}
