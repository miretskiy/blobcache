package blobcache

import (
	"sync"
	"sync/atomic"
)

// handle is the internal pooled buffer structure.
type handle struct {
	buf   []byte
	pool  *sync.Pool
	inUse int32
}

// Size constants
const (
	sizeSmall  = 4 * 1024   // 4KB
	sizeMedium = 64 * 1024  // 64KB
	sizeLarge  = 256 * 1024 // 256KB
)

// 1. Declare the pools (zero-value is safe, New will be assigned in init)
var (
	poolSmall  sync.Pool
	poolMedium sync.Pool
	poolLarge  sync.Pool
)

// 2. Initialize the New functions in init() to break the cyclic dependency
func init() {
	poolSmall.New = func() any {
		return &handle{
			buf:  make([]byte, 0, sizeSmall),
			pool: &poolSmall,
		}
	}
	poolMedium.New = func() any {
		return &handle{
			buf:  make([]byte, 0, sizeMedium),
			pool: &poolMedium,
		}
	}
	poolLarge.New = func() any {
		return &handle{
			buf:  make([]byte, 0, sizeLarge),
			pool: &poolLarge,
		}
	}
}

// BufferHandle is the public wrapper
type BufferHandle struct {
	h *handle
	_ noCopy
}

func (bh *BufferHandle) Bytes() []byte {
	if bh.h == nil {
		return nil
	}
	return bh.h.buf
}

func (bh *BufferHandle) SetBytes(b []byte) {
	if bh.h != nil {
		bh.h.buf = b
	}
}

func (bh *BufferHandle) IsZero() bool {
	return bh.h == nil
}

func (bh *BufferHandle) Release() {
	if bh.h == nil {
		return
	}

	// Poison pill
	if !atomic.CompareAndSwapInt32(&bh.h.inUse, 1, 0) {
		panic("bufferpool: DOUBLE RELEASE detected")
	}

	// Reset length, keep capacity
	bh.h.buf = bh.h.buf[:0:cap(bh.h.buf)]

	// Return to home pool
	if bh.h.pool != nil {
		bh.h.pool.Put(bh.h)
	}

	bh.h = nil
}

// AcquireBuffer selects the right bucket
func AcquireBuffer(length, capacity int) BufferHandle {
	var h *handle

	// Select Pool
	if capacity <= sizeSmall {
		h = poolSmall.Get().(*handle)
	} else if capacity <= sizeMedium {
		h = poolMedium.Get().(*handle)
	} else if capacity <= sizeLarge {
		h = poolLarge.Get().(*handle)
	} else {
		// Fallback for huge allocs
		h = &handle{
			buf:  make([]byte, 0, capacity),
			pool: nil,
		}
	}

	// Detect Aliasing / Set Use
	if !atomic.CompareAndSwapInt32(&h.inUse, 0, 1) {
		if h.pool != nil {
			panic("bufferpool: ALIASING DETECTED! Pool returned in-use handle.")
		}
		h.inUse = 1
	}

	// Safety Valve: Grow if needed (should be rare/impossible with correct logic)
	if cap(h.buf) < capacity {
		h.buf = make([]byte, 0, capacity)
		h.pool = nil
	}

	h.buf = h.buf[:length]

	return BufferHandle{h: h}
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
