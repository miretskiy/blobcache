package blobcache

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

// handle is the internal pooled buffer structure.
type handle struct {
	buf     []byte
	poolIdx int // Index in the pools array; -1 if not pooled
	inUse   int32
}

const (
	minShift    = 12 // 4KB
	maxShift    = 21 // 2MB
	numPools    = maxShift - minShift + 1
	jumboIdx    = numPools         // The extra catch-all bucket
	maxPoolSize = 16 * 1024 * 1024 // 16MB ceiling for pooling
)

// We define (numPools + 1) to include the Jumbo bucket
var pools [numPools + 1]sync.Pool

func init() {
	// 1. Initialize Power-of-Two Buckets (4KB to 2MB)
	for i := 0; i < numPools; i++ {
		idx := i
		capacity := 1 << (i + minShift)
		pools[i].New = func() any {
			return &handle{
				buf:     make([]byte, 0, capacity),
				poolIdx: idx,
			}
		}
	}

	// 2. Initialize the Jumbo Catch-all Bucket
	pools[jumboIdx].New = func() any {
		return &handle{
			buf:     make([]byte, 0, 1<<(maxShift+1)), // Starts at 4MB
			poolIdx: jumboIdx,
		}
	}
}

type BufferHandle struct {
	h *handle
}

func (bh *BufferHandle) Bytes() []byte {
	if bh.h == nil {
		return nil
	}
	return bh.h.buf
}

// AcquireBuffer picks a bucket. If it's a jumbo request, it allows the
// slice to be reallocated and returned to the jumbo pool.
func AcquireBuffer(length, capacity int) BufferHandle {
	var h *handle

	if capacity > maxPoolSize {
		// Outlier: Too big to pool safely without pinning massive RSS
		h = &handle{buf: make([]byte, 0, capacity), poolIdx: -1}
	} else if capacity > (1 << maxShift) {
		// Jumbo: Use the stretching pool
		h = pools[jumboIdx].Get().(*handle)
		if cap(h.buf) < capacity {
			h.buf = make([]byte, 0, capacity)
		}
	} else {
		// Standard: Power-of-two buckets
		effCap := capacity
		if effCap < (1 << minShift) {
			effCap = 1 << minShift
		}
		idx := bits.Len32(uint32(effCap-1)) - minShift
		if idx < 0 {
			idx = 0
		}
		h = pools[idx].Get().(*handle)
	}

	// Safety check for concurrent development/debugging
	if !atomic.CompareAndSwapInt32(&h.inUse, 0, 1) {
		panic("bufferpool: ALIASING DETECTED")
	}

	h.buf = h.buf[:length]
	return BufferHandle{h: h}
}

func (bh *BufferHandle) Release() {
	if bh.h == nil {
		return
	}

	if !atomic.CompareAndSwapInt32(&bh.h.inUse, 1, 0) {
		panic("bufferpool: DOUBLE RELEASE detected")
	}

	idx := bh.h.poolIdx
	if idx >= 0 && idx < len(pools) {
		// Reset length, keep capacity (even if reallocated in Jumbo)
		bh.h.buf = bh.h.buf[:0:cap(bh.h.buf)]
		pools[idx].Put(bh.h)
	}

	bh.h = nil
}

func (bh *BufferHandle) SetBytes(b []byte) {
	if bh.h != nil {
		bh.h.buf = b
	}
}

func (bh *BufferHandle) IsZero() bool {
	return bh.h == nil
}
