package blobcache

import (
	"sync"
	"sync/atomic"
)

// handle is the internal pooled buffer structure.
// Only accessed through BufferHandle to prevent aliasing bugs.
type handle struct {
	buf   []byte
	inUse int32 // Atomic poison pill: 0=in pool, 1=in use
}

// bufferPool stores *handle to prevent sub-slice aliasing bugs.
// Public API returns BufferHandle (by value) which holds *handle pointer.
var bufferPool = sync.Pool{
	New: func() any {
		return &handle{
			buf: make([]byte, 0, 64*1024),
		}
	},
}

// BufferHandle is a handle to a pooled buffer that must be released after use.
// Returned by value with embedded *handle pointer for idempotent Release().
type BufferHandle struct {
	h *handle // Internal handle (nil after first Release for idempotency)
	_ noCopy
}

// Bytes returns the current buffer slice.
func (bh *BufferHandle) Bytes() []byte {
	if bh.h == nil {
		return nil
	}
	return bh.h.buf
}

// SetBytes updates the buffer view (e.g., to a sub-slice after compression).
func (bh *BufferHandle) SetBytes(b []byte) {
	if bh.h != nil {
		bh.h.buf = b
	}
}

// IsZero returns true if the handle is uninitialized (no compression applied).
func (bh *BufferHandle) IsZero() bool {
	return bh.h == nil
}

// Release returns the buffer to the pool with full capacity reset.
// Idempotent: first call returns buffer to pool, subsequent calls are no-ops.
// Safe to call on nil or zero-value BufferHandle.
func (bh *BufferHandle) Release() {
	if bh.h == nil {
		return // Already released or never acquired
	}

	// Poison pill: detect double-release
	if !atomic.CompareAndSwapInt32(&bh.h.inUse, 1, 0) {
		panic("bufferpool: DOUBLE RELEASE! handle.inUse was not 1 (either already released or never acquired)")
	}

	// Reset to zero length with full capacity preserved
	bh.h.buf = bh.h.buf[:0:cap(bh.h.buf)]
	bufferPool.Put(bh.h)

	// Clear pointer - makes subsequent Release() calls no-ops
	bh.h = nil
}

// AcquireBuffer gets a buffer from the pool with specified length and capacity.
// Returns BufferHandle by value (contains *handle). MUST call Release() after use.
func AcquireBuffer(length, capacity int) BufferHandle {
	h := bufferPool.Get().(*handle)

	// Poison pill: detect double-release aliasing
	if !atomic.CompareAndSwapInt32(&h.inUse, 0, 1) {
		panic("bufferpool: ALIASING DETECTED! Acquired a handle that is already inUse=1. " +
				"This means Release() was called twice, putting the same *handle in the pool multiple times, " +
				"and now two goroutines have the same pointer.")
	}

	// Reallocate if pooled buffer is too small
	if cap(h.buf) < capacity {
		h.buf = make([]byte, 0, capacity)
	}

	// Always set to requested length
	h.buf = h.buf[:length]

	return BufferHandle{h: h}
}

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from go vet.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
