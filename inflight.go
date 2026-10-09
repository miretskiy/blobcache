package blobcache

import (
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/blobstore"
)

// inflight hands writes from Put to the completer, in submission order. The
// queue is a buffered channel; Put reserves room in it before submitting, so
// that once the store has accepted a write, queueing it never blocks. When
// there is no room, Put returns ErrBusy without taking the caller's buffer.
// Each accepted write also keeps its memory pinned until completion.
//
// Drain queues a marker behind the writes before it and waits for the
// completer to reach it. Drains are serialized, and the queue has one place
// beyond what Put may reserve, so the marker never waits for room either.
type inflight struct {
	queue    chan pendingWrite
	reserved atomic.Int64 // room claimed by Put, queued writes included
	drainMu  sync.Mutex
}

// pendingWrite is a write between submission and completion, or a Drain
// marker.
type pendingWrite struct {
	hash    Key
	mem     memoryValue // the value's memory, pinned until the write completes
	ticket  blobstore.Ticket
	drained chan struct{} // a Drain marker: closed when reached
}

func (q *inflight) init(n int) {
	q.queue = make(chan pendingWrite, n+1) // one place for a Drain marker
}

// reserve claims room for one write, or reports false if the queue is full.
func (q *inflight) reserve() bool {
	if q.reserved.Add(1) > int64(cap(q.queue)-1) {
		q.reserved.Add(-1)
		return false
	}
	return true
}

// unreserve gives back room for a write the store did not accept, or one the
// completer finished.
func (q *inflight) unreserve() { q.reserved.Add(-1) }

// push queues an accepted write into room claimed by reserve.
func (q *inflight) push(w pendingWrite) { q.queue <- w }

// drain waits until every write queued before the call has completed.
func (q *inflight) drain() {
	q.drainMu.Lock()
	defer q.drainMu.Unlock()
	drained := make(chan struct{})
	q.queue <- pendingWrite{drained: drained}
	<-drained
}

// inFlight returns how many writes are reserved and not yet completed.
func (q *inflight) inFlight() int { return int(q.reserved.Load()) }
