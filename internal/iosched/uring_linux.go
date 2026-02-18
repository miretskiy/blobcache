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

// available caches the result of the runtime io_uring probe.
var available atomic.Int32 // 0=unknown, 1=yes, -1=no

// Available reports whether io_uring is supported on the running kernel.
// The result is cached after the first call.
func Available() bool {
	if v := available.Load(); v != 0 {
		return v > 0
	}
	ring := giouring.NewRing()
	err := ring.QueueInit(1, 0)
	if err != nil {
		available.Store(-1)
		return false
	}
	ring.QueueExit()
	available.Store(1)
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

// URingScheduler is an asynchronous IOScheduler backed by io_uring.
//
// Architecture: a single coordinator goroutine owns the ring and
// processes submissions from a buffered channel. Implicit batching:
// under concurrency, multiple ReadAt calls naturally coalesce into
// a single io_uring_enter.
type URingScheduler struct {
	ring     *giouring.Ring
	submitCh chan *uringReq
	reqPool  sync.Pool

	// inflight maps slot index -> request. Fixed-size array indexed by
	// SQE UserData. Slot allocation via a freelist channel.
	inflight []atomic.Pointer[uringReq]
	freeSlot chan uint64 // stack of available slot indices

	batchSize int
	closed    atomic.Bool
	wg        sync.WaitGroup
}

// NewURingScheduler creates an io_uring-backed scheduler.
// Returns an error if io_uring is not available on this kernel.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	if !Available() {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	depth := cfg.ringDepth()

	var flags uint32
	if cfg.SQPOLL {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		return nil, fmt.Errorf("iosched: io_uring_setup: %w", err)
	}

	batchSize := cfg.batchSize()
	if batchSize > int(depth) {
		batchSize = int(depth)
	}

	s := &URingScheduler{
		ring:      ring,
		submitCh:  make(chan *uringReq, cfg.chanBuffer()),
		inflight:  make([]atomic.Pointer[uringReq], depth),
		freeSlot:  make(chan uint64, depth),
		batchSize: batchSize,
	}
	s.reqPool.New = func() any {
		return &uringReq{done: make(chan struct{}, 1)}
	}

	// Populate free slot stack.
	for i := uint64(0); i < uint64(depth); i++ {
		s.freeSlot <- i
	}

	s.wg.Add(1)
	go s.loop()

	return s, nil
}

// ReadAt submits a positioned read to the io_uring ring and blocks
// until the kernel completes it. The buffer must remain valid and
// must not be moved by GC (heap-allocated or mmap'd).
func (s *URingScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	if s.closed.Load() {
		return 0, errors.New("iosched: scheduler closed")
	}
	if len(buf) == 0 {
		return 0, nil
	}

	req := s.reqPool.Get().(*uringReq)
	req.fd = fd
	req.buf = buf
	req.offset = offset
	req.n = 0
	req.err = nil

	// Drain stale signal from a previous cycle.
	select {
	case <-req.done:
	default:
	}

	s.submitCh <- req

	// Block until the coordinator signals completion.
	<-req.done

	n, err := req.n, req.err
	s.reqPool.Put(req)

	// Prevent GC from collecting buf before we've read the result.
	runtime.KeepAlive(buf)

	return n, err
}

// Close shuts down the coordinator goroutine and releases the ring.
// Any pending ReadAt calls will return after their in-flight operations
// complete or are failed.
func (s *URingScheduler) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.submitCh)
	s.wg.Wait()
	s.ring.QueueExit()
	return nil
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
func (s *URingScheduler) loop() {
	defer s.wg.Done()

	batch := make([]*uringReq, 0, s.batchSize)
	inflightCount := 0

	for {
		batch = batch[:0]

		// ── Phase 1: Collect ─────────────────────────────────
		if inflightCount == 0 {
			// Nothing in-flight: safe to block on channel.
			req, ok := <-s.submitCh
			if !ok {
				return // channel closed, shutdown
			}
			batch = append(batch, req)
		}

		// Opportunistic non-blocking drain.
	drain:
		for len(batch) < s.batchSize {
			select {
			case req, ok := <-s.submitCh:
				if !ok {
					break drain // channel closed
				}
				batch = append(batch, req)
			default:
				break drain
			}
		}

		// ── Phase 2: Submit ──────────────────────────────────
		for _, req := range batch {
			slot := <-s.freeSlot
			s.inflight[slot].Store(req)

			sqe := s.ring.GetSQE()
			if sqe == nil {
				// SQ full: flush what we have and retry once.
				if _, err := s.ring.Submit(); err != nil && !errors.Is(err, syscall.EINTR) {
					s.completeReq(slot, req, 0, err)
					continue
				}
				sqe = s.ring.GetSQE()
				if sqe == nil {
					s.completeReq(slot, req, 0, errors.New("iosched: SQ ring full"))
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
			_, err := s.ring.SubmitAndWait(1)
			if err != nil && !errors.Is(err, syscall.EINTR) {
				s.failAll(err)
				inflightCount = 0

				// If channel is closed and nothing left, exit.
				if s.closed.Load() {
					return
				}
				continue
			}

			reaped := s.reapCompletions()
			inflightCount -= reaped
		}

		// If channel is closed and nothing in-flight, we're done.
		if s.closed.Load() && inflightCount == 0 {
			// Drain any remaining requests that arrived before close.
			for req := range s.submitCh {
				req.err = errors.New("iosched: scheduler closed")
				req.done <- struct{}{}
			}
			return
		}
	}
}

// reapCompletions processes all available CQEs and signals the waiters.
// Returns the number of completions reaped.
func (s *URingScheduler) reapCompletions() int {
	var count uint32
	s.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := cqe.GetData64()
		req := s.inflight[slot].Swap(nil)
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
		s.freeSlot <- slot
	})

	if count > 0 {
		s.ring.CQAdvance(count)
	}
	return int(count)
}

// completeReq signals a request as failed without going through the ring.
func (s *URingScheduler) completeReq(slot uint64, req *uringReq, n int, err error) {
	req.n = n
	req.err = err
	req.done <- struct{}{}
	s.inflight[slot].Store(nil)
	s.freeSlot <- slot
}

// failAll completes all in-flight requests with the given error.
func (s *URingScheduler) failAll(err error) {
	for i := range s.inflight {
		if req := s.inflight[i].Swap(nil); req != nil {
			req.err = fmt.Errorf("iosched: ring error: %w", err)
			req.done <- struct{}{}
			s.freeSlot <- uint64(i)
		}
	}
}
