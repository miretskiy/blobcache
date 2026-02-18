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
	<-req.done

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
	s.stopOnce.Do(func() { close(s.stopCh) })
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

// loop is the coordinator goroutine. It exclusively owns the ring.
//
// The loop implements a three-phase cycle:
//  1. Collect: block for the first request, then opportunistically drain
//     up to batchSize requests from the channel (non-blocking).
//  2. Submit: fill SQEs for the batch and call SubmitAndWait(1) to push
//     them to the kernel and block until at least one CQE is ready.
//  3. Reap: walk all available CQEs, signal waiters, return slots.
//
// Under low concurrency, a single read goes through immediately (no
// batching delay). Under high concurrency, requests naturally coalesce
// into one io_uring_enter call.
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

	inflight := make([]atomic.Pointer[uringReq], depth)
	freeSlot := make(chan uint64, depth)
	for i := uint64(0); i < uint64(depth); i++ {
		freeSlot <- i
	}

	readyCh <- nil // signal constructor: ring is ready

	batch := make([]*uringReq, 0, batchSize)
	inflightCount := 0

	// drainPending fails all buffered and inflight requests on exit.
	defer func() {
		// Fail inflight requests that the kernel hasn't completed.
		for i := range inflight {
			if req := inflight[i].Swap(nil); req != nil {
				req.n = 0
				req.err = errSchedulerClosed
				req.done <- struct{}{}
			}
		}
		// Fail requests still queued in submitCh.
		for {
			select {
			case req := <-s.submitCh:
				req.n = 0
				req.err = errSchedulerClosed
				req.done <- struct{}{}
			default:
				return
			}
		}
	}()

	for {
		batch = batch[:0]

		// ── Phase 1: Collect ─────────────────────────────────
		if inflightCount == 0 {
			// Nothing in-flight: safe to block for work or stop.
			select {
			case req := <-s.submitCh:
				batch = append(batch, req)
			case <-s.stopCh:
				return
			}
		}

		// Opportunistic non-blocking drain.
	drain:
		for len(batch) < batchSize {
			select {
			case req := <-s.submitCh:
				batch = append(batch, req)
			case <-s.stopCh:
				// Fail the batch we've collected so far.
				for _, req := range batch {
					req.n = 0
					req.err = errSchedulerClosed
					req.done <- struct{}{}
				}
				return
			default:
				break drain
			}
		}

		// ── Phase 2: Submit ──────────────────────────────────
		for _, req := range batch {
			slot := <-freeSlot
			inflight[slot].Store(req)

			sqe := ring.GetSQE()
			if sqe == nil {
				// SQ full: flush what we have and retry once.
				if _, err := ring.Submit(); err != nil && !errors.Is(err, syscall.EINTR) {
					completeReq(inflight[:], freeSlot, slot, req, 0, err)
					continue
				}
				sqe = ring.GetSQE()
				if sqe == nil {
					completeReq(inflight[:], freeSlot, slot, req, 0, errors.New("iosched: SQ ring full"))
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
			inflightCount++
		}

		// ── Phase 3: Submit to kernel + reap completions ─────
		if inflightCount > 0 {
			_, err := ring.SubmitAndWait(1)
			if err != nil && !errors.Is(err, syscall.EINTR) {
				s.setFatalErr(fmt.Errorf("iosched: ring error: %w", err))
				failAll(inflight[:], freeSlot, err)
				s.stop()
				return
			}

			reaped := reapCompletions(ring, inflight[:], freeSlot)
			inflightCount -= reaped
		}
	}
}

// reapCompletions processes all available CQEs and signals the waiters.
func reapCompletions(ring *giouring.Ring, inflight []atomic.Pointer[uringReq], freeSlot chan uint64) int {
	var count uint32
	ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := cqe.GetData64()
		req := inflight[slot].Swap(nil)
		if req == nil {
			return
		}

		if cqe.Res < 0 {
			req.n = 0
			req.err = syscall.Errno(-cqe.Res)
		} else {
			req.n = int(cqe.Res)
		}

		req.done <- struct{}{}
		freeSlot <- slot
	})

	if count > 0 {
		ring.CQAdvance(count)
	}
	return int(count)
}

// completeReq signals a request as failed without going through the ring.
func completeReq(inflight []atomic.Pointer[uringReq], freeSlot chan uint64, slot uint64, req *uringReq, n int, err error) {
	req.n = n
	req.err = err
	req.done <- struct{}{}
	inflight[slot].Store(nil)
	freeSlot <- slot
}

// failAll completes all in-flight requests with the given error.
func failAll(inflight []atomic.Pointer[uringReq], freeSlot chan uint64, err error) {
	for i := range inflight {
		if req := inflight[i].Swap(nil); req != nil {
			req.n = 0
			req.err = fmt.Errorf("iosched: ring error: %w", err)
			req.done <- struct{}{}
			freeSlot <- uint64(i)
		}
	}
}
