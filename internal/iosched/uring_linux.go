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

const (
	defaultRingDepth  = 256
	defaultChanBuffer = 512
	defaultBatchSize  = 64
)

// URingConfig configures the io_uring scheduler.
type URingConfig struct {
	// RingDepth is the number of SQ entries. Must be a power of two.
	// Default: 256.
	RingDepth uint32

	// ChanBuffer is the capacity of the submission channel.
	// Default: 512.
	ChanBuffer int

	// BatchSize is the maximum number of SQEs filled per loop iteration.
	// Default: 64.
	BatchSize int

	// SQPOLL enables IORING_SETUP_SQPOLL. The kernel spawns a polling
	// thread that continuously checks for new submissions, eliminating
	// the io_uring_enter syscall on the submission path. Burns one CPU
	// core; the kernel thread sleeps after a default idle period (~1s).
	SQPOLL bool
}

func (c *URingConfig) ringDepth() uint32 {
	if c.RingDepth > 0 {
		return c.RingDepth
	}
	return defaultRingDepth
}

func (c *URingConfig) chanBuffer() int {
	if c.ChanBuffer > 0 {
		return c.ChanBuffer
	}
	return defaultChanBuffer
}

func (c *URingConfig) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return defaultBatchSize
}

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
// Architecture: a single coordinator goroutine owns the ring and
// processes submissions from a buffered channel. Implicit batching:
// under concurrency, multiple ReadAt calls naturally coalesce into
// a single io_uring_enter.
type URingScheduler struct {
	submitCh chan *uringReq
	reqPool  sync.Pool

	// stopCh is closed to signal shutdown (by Close or fatal ring error).
	// submitCh is never closed — ReadAt selects on both channels.
	stopCh   chan struct{}
	stopOnce sync.Once

	// fatalErr stores the first non-recoverable ring error.
	// Once set, all subsequent ReadAt calls return this error.
	fatalErr atomic.Pointer[error]

	wg sync.WaitGroup
}

// NewURingScheduler creates an io_uring-backed scheduler.
// The ring is created inside the coordinator goroutine; if ring setup
// fails, the error is returned synchronously via a ready channel.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	s := &URingScheduler{
		submitCh: make(chan *uringReq, cfg.chanBuffer()),
		stopCh:   make(chan struct{}),
	}
	s.reqPool.New = func() any {
		return &uringReq{done: make(chan struct{}, 1)}
	}

	readyCh := make(chan error, 1)
	s.wg.Add(1)
	go s.loop(cfg, readyCh)

	if err := <-readyCh; err != nil {
		return nil, err
	}
	return s, nil
}

// ReadAt submits a positioned read to the io_uring ring and blocks
// until the kernel completes it. The buffer must remain valid and
// must not be moved by GC (heap-allocated or mmap'd).
func (s *URingScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	if err := s.err(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}

	req := s.getReq()
	req.fd = fd
	req.buf = buf
	req.offset = offset
	req.n = 0
	req.err = nil

	select {
	case s.submitCh <- req:
	case <-s.stopCh:
		s.putReq(req)
		if err := s.err(); err != nil {
			return 0, err
		}
		return 0, errSchedulerClosed
	}

	// Block until the coordinator signals completion.
	select {
	case <-req.done:
	case <-s.stopCh:
		// Coordinator stopped while we were waiting. Its deferred drain
		// may have already failed this request — check once.
		select {
		case <-req.done:
		default:
			// Orphaned: request is in submitCh but the coordinator exited
			// before processing it. Abandon the req to avoid data races.
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

func (s *URingScheduler) getReq() *uringReq {
	return s.reqPool.Get().(*uringReq)
}

func (s *URingScheduler) putReq(req *uringReq) {
	req.buf = nil // release reference to caller's buffer
	// Drain stale signal so the next Get returns a clean req.
	select {
	case <-req.done:
	default:
	}
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

// coordinator encapsulates the io_uring event loop state.
// All fields are owned exclusively by a single goroutine.
type coordinator struct {
	sched     *URingScheduler
	ring      *giouring.Ring
	inflight  []*uringReq // slot index → pending request (nil = free)
	free      []uint64    // LIFO stack of available slot indices
	batch     []*uringReq
	batchSize int
}

// hasInflight reports whether any requests are pending in the kernel.
// Derived from the free stack: cap(free) == ring depth, so
// len(free) < cap(free) means slots are in use.
func (c *coordinator) hasInflight() bool {
	return len(c.free) < cap(c.free)
}

// loop is the coordinator goroutine. It exclusively owns the ring.
//
// Three-phase cycle: collect requests from the channel, fill SQEs and
// push them to the kernel, then reap CQEs and signal callers. Under
// low concurrency a single read goes through immediately; under high
// concurrency requests naturally coalesce into one io_uring_enter.
func (s *URingScheduler) loop(cfg URingConfig, readyCh chan<- error) {
	defer s.wg.Done()

	depth := cfg.ringDepth()
	var flags uint32
	if cfg.SQPOLL {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		readyCh <- fmt.Errorf("iosched: io_uring_setup: %w", err)
		return
	}
	defer ring.QueueExit()

	batchSize := min(cfg.batchSize(), int(depth))
	free := make([]uint64, depth)
	for i := range free {
		free[i] = uint64(i)
	}

	c := coordinator{
		sched:     s,
		ring:      ring,
		inflight:  make([]*uringReq, depth),
		free:      free,
		batch:     make([]*uringReq, 0, batchSize),
		batchSize: batchSize,
	}
	readyCh <- nil
	defer c.drainPending()

	for {
		if !c.collect() {
			return
		}
		c.submit()
		if c.hasInflight() {
			if !c.reap() {
				return
			}
		}
	}
}

// collect gathers pending requests from the submission channel.
// Blocks for the first request when nothing is in-flight (otherwise
// the loop would spin). Returns false on shutdown.
func (c *coordinator) collect() bool {
	c.batch = c.batch[:0]
	limit := min(c.batchSize, len(c.free))
	if limit == 0 {
		return true // all slots in use; proceed to reap
	}

	if !c.hasInflight() {
		select {
		case req := <-c.sched.submitCh:
			c.batch = append(c.batch, req)
		case <-c.sched.stopCh:
			return false
		}
	}

	for len(c.batch) < limit {
		select {
		case req := <-c.sched.submitCh:
			c.batch = append(c.batch, req)
		case <-c.sched.stopCh:
			return false
		default:
			return true
		}
	}
	return true
}

// submit fills SQEs for each request in the batch and registers them
// in the inflight table. Requests that fail SQE allocation are
// completed immediately with an error.
func (c *coordinator) submit() {
	for _, req := range c.batch {
		slot := c.allocSlot()
		c.inflight[slot] = req

		sqe := c.ring.GetSQE()
		if sqe == nil {
			// SQ full: flush what we have and retry once.
			if _, err := c.ring.Submit(); err != nil && !errors.Is(err, syscall.EINTR) {
				c.failReq(slot, req, err)
				continue
			}
			sqe = c.ring.GetSQE()
			if sqe == nil {
				c.failReq(slot, req, errors.New("iosched: SQ ring full"))
				continue
			}
		}

		sqe.PrepareRead(
			req.fd,
			uintptr(unsafe.Pointer(&req.buf[0])),
			uint32(len(req.buf)),
			uint64(req.offset),
		)
		sqe.SetData64(slot)
	}
}

// reap pushes pending SQEs to the kernel, waits for at least one
// completion, then signals all completed callers. Returns false on
// fatal ring error.
func (c *coordinator) reap() bool {
	_, err := c.ring.SubmitAndWait(1)
	if err != nil && !errors.Is(err, syscall.EINTR) {
		c.sched.setFatalErr(fmt.Errorf("iosched: ring error: %w", err))
		c.failAll(err)
		c.sched.stop()
		return false
	}

	var count uint32
	c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := cqe.GetData64()
		req := c.inflight[slot]
		c.inflight[slot] = nil
		if req == nil {
			return
		}
		if cqe.Res < 0 {
			req.err = syscall.Errno(-cqe.Res)
		} else {
			req.n = int(cqe.Res)
		}
		req.done <- struct{}{}
		c.freeSlot(slot)
	})
	if count > 0 {
		c.ring.CQAdvance(count)
	}
	return true
}

func (c *coordinator) allocSlot() uint64 {
	n := len(c.free) - 1
	slot := c.free[n]
	c.free = c.free[:n]
	return slot
}

func (c *coordinator) freeSlot(slot uint64) {
	c.free = append(c.free, slot)
}

// failReq completes a request as failed and returns its slot.
func (c *coordinator) failReq(slot uint64, req *uringReq, err error) {
	req.err = err
	req.done <- struct{}{}
	c.inflight[slot] = nil
	c.freeSlot(slot)
}

// failAll completes all in-flight requests with the given error.
func (c *coordinator) failAll(err error) {
	for i, req := range c.inflight {
		if req != nil {
			req.err = fmt.Errorf("iosched: ring error: %w", err)
			req.done <- struct{}{}
			c.inflight[i] = nil
			c.freeSlot(uint64(i))
		}
	}
}

// drainPending fails all pending requests on coordinator exit:
// batch items not yet submitted, in-flight kernel requests, and
// requests still queued in the submission channel.
func (c *coordinator) drainPending() {
	for _, req := range c.batch {
		req.err = errSchedulerClosed
		req.done <- struct{}{}
	}
	c.batch = c.batch[:0]

	for i, req := range c.inflight {
		if req != nil {
			req.err = errSchedulerClosed
			req.done <- struct{}{}
			c.inflight[i] = nil
		}
	}

	for {
		select {
		case req := <-c.sched.submitCh:
			req.err = errSchedulerClosed
			req.done <- struct{}{}
		default:
			return
		}
	}
}
