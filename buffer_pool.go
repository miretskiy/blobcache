package blobcache

import "sync"

// bufferPool is a shared pool for compression and decompression buffers.
// Reduces GC pressure by reusing byte slices.
// We store pointers to avoid allocations when converting to interface{}.
var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 64*1024)
		return &buf
	},
}

// BufferHandle is a handle to a pooled buffer that must be released after use.
type BufferHandle struct {
	buf []byte
}

// Bytes returns the buffer slice. The slice has length equal to the
// requested size and capacity >= length.
func (h *BufferHandle) Bytes() []byte {
	return h.buf
}

// IsZero returns true if the handle is uninitialized (no buffer acquired).
func (h *BufferHandle) IsZero() bool {
	return h.buf == nil
}

// Release returns the buffer to the pool. Safe to call multiple times.
// After Release, the BufferHandle should not be used.
func (h *BufferHandle) Release() {
	if h.buf == nil {
		return
	}
	buf := h.buf[:0]
	bufferPool.Put(&buf)
	h.buf = nil
}

// AcquireBuffer gets a buffer from the pool with specified length and capacity.
// Works like make([]byte, length, capacity). The returned BufferHandle MUST be released.
func AcquireBuffer(length, capacity int) BufferHandle {
	bufPtr := bufferPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < capacity {
		buf = make([]byte, length, capacity)
	} else {
		buf = buf[:length]
	}
	return BufferHandle{buf: buf}
}
