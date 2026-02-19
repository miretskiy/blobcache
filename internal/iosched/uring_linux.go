//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/pawelgaczynski/giouring"
)

// IOUringAvailable reports whether io_uring is supported on the running kernel.
// Probed once at package init time.
var IOUringAvailable = probeIOUring()

func probeIOUring() bool {
	ring := giouring.NewRing()
	if err := ring.QueueInit(1, 0); err != nil {
		return false
	}
	ring.QueueExit()
	return true
}

const defaultRingDepth uint32 = 256

// uringReq is a single in-flight read request. Pooled to avoid allocation.
type uringReq struct {
	fd     int
	buf    []byte
	offset int64
	n      int
	err    error
	done   chan struct{}
}

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// URingScheduler is an asynchronous IOScheduler backed by io_uring.
//
// Architecture: a single coordinator goroutine owns the ring. Callers
// append requests to a mutex-protected queue and send a notification
// on a buffered(1) signal channel. The coordinator uses a sliding window:
// it continuously fills free ring slots from the pending queue, calls
// SubmitAndWait(1) to wait for at least one completion, reaps all ready
// CQEs, and loops. The ring stays full, maximizing NVMe utilization.
type URingScheduler struct {
	mu      sync.Mutex
	pending []*uringReq

	signal  chan struct{} // buffered(1): wakeup for coordinator
	reqPool sync.Pool

	// stopCh is closed to signal shutdown (by Close or fatal ring error).
	stopCh   chan struct{}
	stopOnce sync.Once

	// fatalErr stores the first non-recoverable ring error.
	// Once set, all subsequent ReadAt calls return this error.
	fatalErr atomic.Pointer[error]

	wg sync.WaitGroup

	// Stats — updated by the coordinator goroutine (single writer).
	batches  atomic.Int64
	requests atomic.Int64
	maxBatch atomic.Int64
}

// Stats returns batching effectiveness counters.
func (s *URingScheduler) Stats() Stats {
	batches := s.batches.Load()
	reqs := s.requests.Load()
	var avg float64
	if batches > 0 {
		avg = float64(reqs) / float64(batches)
	}
	return Stats{
		Batches:  batches,
		Requests: reqs,
		MaxBatch: s.maxBatch.Load(),
		AvgBatch: avg,
	}
}

// NewURingScheduler creates an io_uring-backed scheduler.
// The ring is created inside the coordinator goroutine; if ring setup
// fails, the error is returned synchronously via a ready channel.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	s := &URingScheduler{
		signal: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	s.reqPool.New = func() any {
		return &uringReq{done: make(chan struct{}, 1)}
	}

	depth := max(cfg.RingDepth, defaultRingDepth)
	readyCh := make(chan error, 1)
	s.wg.Add(1)
	go s.loop(depth, cfg.SQPOLL, readyCh)

	if err := <-readyCh; err != nil {
		return nil, err
	}
	return s, nil
}

// ReadAt submits a positioned read to the io_uring ring and blocks
// until the kernel completes it. The buffer must remain valid until
// ReadAt returns.
func (s *URingScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	if err := s.err(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}

	req := s.getReq(fd, buf, offset)

	// Append to pending queue under lock.
	s.mu.Lock()
	s.pending = append(s.pending, req)
	s.mu.Unlock()

	// Wake the coordinator (non-blocking: if signal already pending, no-op).
	select {
	case s.signal <- struct{}{}:
	default:
	}

	// Block until the coordinator completes the request.
	select {
	case <-req.done:
	case <-s.stopCh:
		// Coordinator stopped while we were waiting. Its deferred drain
		// may have already failed this request — check once.
		select {
		case <-req.done:
		default:
			runtime.KeepAlive(buf)
			return 0, errSchedulerClosed
		}
	}

	n, err := req.n, req.err
	s.putReq(req)

	// Prevent GC from collecting buf before we've read the result.
	runtime.KeepAlive(buf)

	return n, err
}

// Close shuts down the coordinator goroutine and releases the ring.
func (s *URingScheduler) Close() error {
	s.stop()
	s.wg.Wait()
	return nil
}

func (s *URingScheduler) getReq(fd int, buf []byte, offset int64) *uringReq {
	req := s.reqPool.Get().(*uringReq)
	req.fd = fd
	req.buf = buf
	req.offset = offset
	req.n = 0
	req.err = nil
	return req
}

func (s *URingScheduler) putReq(req *uringReq) {
	*req = uringReq{done: req.done}
	s.reqPool.Put(req)
}

// stop signals the coordinator to shut down. Safe to call multiple times
// (from Close and from the coordinator on fatal ring error).
func (s *URingScheduler) stop() {
	s.stopOnce.Do(func() {
		s.setFatalErr(errSchedulerClosed)
		close(s.stopCh)
	})
}

// err returns the fatal error if one has been set, or nil.
func (s *URingScheduler) err() error {
	if p := s.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}

// setFatalErr stores a fatal error. Only the first call takes effect.
func (s *URingScheduler) setFatalErr(err error) {
	s.fatalErr.CompareAndSwap(nil, &err)
}

// ─── Coordinator ────────────────────────────────────────────────

// coordinator encapsulates the io_uring sliding-window event loop.
// All fields are owned exclusively by a single goroutine.
type coordinator struct {
	sched *URingScheduler
	ring  *giouring.Ring

	// inflight maps ring slot index → in-flight request.
	// Nil entries are free slots.
	inflight []*uringReq

	// freeSlots is a LIFO stack of available slot indices.
	freeSlots []int

	// nInflight is the number of SQEs currently in-flight with the kernel.
	nInflight int

	// batch holds requests collected from the pending queue (ping-pong swap).
	batch []*uringReq

	// nSubmitted counts SQEs prepared since the last SubmitAndWait.
	nSubmitted int
}

// loop is the coordinator goroutine. It exclusively owns the ring.
//
// Sliding-window protocol: the coordinator continuously fills free ring
// slots from the pending queue, calls SubmitAndWait(1) to wait for at
// least one completion, reaps all ready CQEs, and loops. The ring stays
// as full as possible, keeping the NVMe pipeline saturated.
func (s *URingScheduler) loop(depth uint32, sqpoll bool, readyCh chan<- error) {
	defer s.wg.Done()

	var flags uint32
	if sqpoll {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		readyCh <- fmt.Errorf("iosched: io_uring_setup: %w", err)
		return
	}
	defer ring.QueueExit()

	c := coordinator{
		sched:    s,
		ring:     ring,
		inflight: make([]*uringReq, depth),
		batch:    make([]*uringReq, 0, depth),
	}

	// Initialize free slots stack (all slots free).
	c.freeSlots = make([]int, depth)
	for i := range depth {
		c.freeSlots[i] = int(i)
	}

	readyCh <- nil
	defer c.drainPending()

	for {
		// 1. Collect: grab new requests from the pending queue.
		if !c.collect() {
			return
		}

		// 2. Submit: fill free ring slots from the batch.
		c.fillSlots()

		if c.nSubmitted == 0 && c.nInflight == 0 {
			// Nothing submitted and nothing in-flight. This can happen
			// if collect returned an empty batch (spurious wakeup).
			continue
		}

		// 3. Submit to kernel and wait for at least 1 completion.
		if err := c.submitAndReap(); err != nil {
			s.setFatalErr(fmt.Errorf("iosched: ring error: %w", err))
			s.stop()
			return
		}
	}
}

// collect grabs new requests from the pending queue. When no requests
// are in-flight, it blocks on the signal channel. When in-flight
// requests exist, it does a non-blocking check (the ring is already
// busy, we'll get more work next iteration). Returns false on shutdown.
func (c *coordinator) collect() bool {
	if c.nInflight == 0 && len(c.batch) == 0 {
		// Idle: block until new work arrives or shutdown.
		select {
		case <-c.sched.signal:
		case <-c.sched.stopCh:
			return false
		}
	} else {
		// Busy: non-blocking check for new work.
		select {
		case <-c.sched.signal:
		case <-c.sched.stopCh:
			return false
		default:
			return true
		}
	}

	// Swap out the pending queue.
	c.sched.mu.Lock()
	collected := len(c.sched.pending)
	c.batch = append(c.batch, c.sched.pending...)
	c.sched.pending = c.sched.pending[:0]
	c.sched.mu.Unlock()

	// Record stats for newly collected items only.
	if n := int64(collected); n > 0 {
		c.sched.batches.Add(1)
		c.sched.requests.Add(n)
		for {
			cur := c.sched.maxBatch.Load()
			if n <= cur || c.sched.maxBatch.CompareAndSwap(cur, n) {
				break
			}
		}
	}

	return true
}

// fillSlots fills as many free ring slots as possible from the batch.
func (c *coordinator) fillSlots() {
	for len(c.batch) > 0 && len(c.freeSlots) > 0 {
		req := c.batch[0]
		c.batch = c.batch[1:]

		slot := c.freeSlots[len(c.freeSlots)-1]
		c.freeSlots = c.freeSlots[:len(c.freeSlots)-1]

		sqe := c.ring.GetSQE()
		if sqe == nil {
			// SQ ring unexpectedly full — fail the request, return slot.
			req.err = errors.New("iosched: SQ ring full")
			req.done <- struct{}{}
			c.freeSlots = append(c.freeSlots, slot)
			continue
		}

		sqe.PrepareRead(
			req.fd,
			uintptr(unsafe.Pointer(&req.buf[0])),
			uint32(len(req.buf)),
			uint64(req.offset),
		)
		sqe.SetData64(uint64(slot))

		c.inflight[slot] = req
		c.nInflight++
		c.nSubmitted++
	}
}

// submitAndReap submits pending SQEs to the kernel, waits for at least
// one completion, and reaps all ready CQEs. Freed slots are returned
// to freeSlots for immediate reuse. Returns a non-nil error only for
// fatal ring failures.
func (c *coordinator) submitAndReap() error {
	for {
		// Wait for at least 1 completion (or just submit if nInflight==0,
		// but we only get here with nInflight > 0 or nSubmitted > 0).
		waitNr := uint32(1)
		if c.nInflight == 0 {
			waitNr = 0
		}

		_, err := c.ring.SubmitAndWait(waitNr)
		c.nSubmitted = 0 // SQEs consumed by kernel regardless of error.

		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			// Fatal: fail all in-flight requests.
			for i, req := range c.inflight {
				if req != nil {
					req.err = fmt.Errorf("iosched: ring error: %w", err)
					req.done <- struct{}{}
					c.inflight[i] = nil
					c.freeSlots = append(c.freeSlots, i)
					c.nInflight--
				}
			}
			return err
		}

		// Reap all ready CQEs.
		var count uint32
		c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
			count++
			slot := int(cqe.GetData64())
			req := c.inflight[slot]
			c.inflight[slot] = nil
			c.freeSlots = append(c.freeSlots, slot)
			c.nInflight--

			if cqe.Res < 0 {
				req.err = syscall.Errno(-cqe.Res)
			} else {
				req.n = int(cqe.Res)
			}
			req.done <- struct{}{}
		})
		if count > 0 {
			c.ring.CQAdvance(count)
		}
		return nil
	}
}

// drainPending fails all in-flight and pending requests.
// Called via defer when the coordinator exits.
func (c *coordinator) drainPending() {
	// Fail in-flight requests (ring is being torn down).
	for i, req := range c.inflight {
		if req != nil {
			req.err = errSchedulerClosed
			req.done <- struct{}{}
			c.inflight[i] = nil
		}
	}

	// Fail remaining batch items.
	for _, req := range c.batch {
		req.err = errSchedulerClosed
		req.done <- struct{}{}
	}
	c.batch = c.batch[:0]

	// Fail anything still in the pending queue.
	c.sched.mu.Lock()
	for _, req := range c.sched.pending {
		req.err = errSchedulerClosed
		req.done <- struct{}{}
	}
	c.sched.pending = c.sched.pending[:0]
	c.sched.mu.Unlock()
}
