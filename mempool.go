package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

var ErrBufferFull = errors.New("mmap slabPool: buffer capacity exceeded")

// --- MmapBuffer: The Physical Slab ---

type MmapBuffer struct {
	raw      []byte
	off      atomic.Int64
	refCount atomic.Int64
	isFrozen atomic.Bool
	pool     *MmapPool // Set only for pooled buffers
}

// Append adds p to this buffer, returning the offset where p was written.
func (b *MmapBuffer) Append(p []byte) (_ int64, err error) {
	n := int64(len(p))

	for {
		currentOff := b.off.Load()
		newOff := currentOff + n

		// 1. Check bounds
		if newOff > int64(len(b.raw)) {
			return 0, ErrBufferFull
		}

		// 2. Atomic Reservation (CAS loop)
		if b.off.CompareAndSwap(currentOff, newOff) {
			// 3. We successfully reserved currentOff to newOff.
			// No other writer can touch this range.
			copy(b.raw[currentOff:newOff], p)
			return currentOff, nil
		}
		// If CAS fails, someone else reserved space; loop and try again.
	}
}

func (b *MmapBuffer) AlignedBytes() []byte {
	// Because we allocated (size + 4096), this math
	// will NEVER result in a slice that exceeds len(b.raw).
	return b.raw[:roundToPage(b.off.Load())]
}

func (b *MmapBuffer) Bytes() []byte {
	return b.raw[:b.off.Load()]
}

func (b *MmapBuffer) Available() int { return int(int64(len(b.raw)) - b.off.Load()) }
func (b *MmapBuffer) Len() int       { return int(b.off.Load()) }
func (b *MmapBuffer) Cap() int       { return len(b.raw) }

func (b *MmapBuffer) Reset() {
	b.off.Store(0)
}

// Unpin decrements the refcount. If 0, and a slabPool is present, it returns to slabPool.
func (b *MmapBuffer) Unpin() {
	// Decrement and check if we are the last ones out
	if b.refCount.Add(-1) == 0 {
		// isFrozen should be set to true when the MemTable
		// rotates this buffer into the 'flushing' state.
		if b.isFrozen.Load() {
			b.resetAndRelease()
		}
	}
}

func (b *MmapBuffer) resetAndRelease() {
	b.Reset()
	b.isFrozen.Store(false) // Reset for next slabPool user
	if b.pool != nil {
		b.pool.Release(b)
	}
}

// NewSectionReader creates a ReadCloser for a range of the buffer.
func (b *MmapBuffer) NewSectionReader(offset, size int64) io.ReadCloser {
	// 1. Physical Bounds Check
	// Protect against potential index out of bounds on the raw slice
	if offset < 0 || size < 0 || offset+size > int64(len(b.raw)) {
		return &MmapHandle{Reader: bytes.NewReader(nil)}
	}

	// 2. Increment RefCount BEFORE exposing the handle
	b.refCount.Add(1)

	h := &MmapHandle{
		Reader: bytes.NewReader(b.raw[offset : offset+size]),
		buffer: b,
	}

	// 3. Register Cleanup
	// We pass 'b' (the buffer) to the closure so it doesn't
	// have to reach back into the potentially-not-fully-initialized 'h'
	h.cleanup = runtime.AddCleanup(h, func(buf *MmapBuffer) { buf.Unpin() }, b)

	return h
}

// --- MmapHandle: The User View ---

type MmapHandle struct {
	*bytes.Reader
	buffer  *MmapBuffer
	once    sync.Once
	cleanup runtime.Cleanup
}

func (h *MmapHandle) Close() error {
	h.once.Do(func() {
		// Stop the GC cleanup from running since we are doing it manually
		h.cleanup.Stop()

		if h.buffer != nil {
			h.buffer.Unpin()
		}
	})
	return nil
}

// --- Internal Helper: allocate ---

func roundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) & ^(pageSize - 1)
}

// allocate mmaps requested size, rounded up to the page boundary, and faults
// in that memory.
func allocate(size int64) *MmapBuffer {
	data, err := unix.Mmap(-1, 0, int(roundToPage(size+4096)),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Sprintf("mmap-slabPool: failed to allocate %d bytes: %v", size, err))
	}

	// PRE-WARM: Force physical RAM commitment.
	for i := 0; i < len(data); i += 4096 {
		data[i] = 0
	}

	buf := &MmapBuffer{raw: data}
	runtime.AddCleanup(buf, func(d []byte) {
		_ = unix.Munmap(d)
	}, data)

	return buf
}

// --- MmapPool: The Manager ---

type MmapPool struct {
	buffers  chan *MmapBuffer
	poolSize int64
}

func NewMmapPool(capacity int, bufferSize int64, headroom int64) *MmapPool {
	p := &MmapPool{
		buffers:  make(chan *MmapBuffer, capacity),
		poolSize: bufferSize + headroom,
	}
	for i := 0; i < capacity; i++ {
		buf := allocate(bufferSize + headroom)
		buf.pool = p // Mark as belonging to this slabPool
		buf.refCount.Store(0)
		p.buffers <- buf
	}
	return p
}

func (p *MmapPool) Acquire() *MmapBuffer {
	var buf *MmapBuffer
	select {
	case buf = <-p.buffers:
		buf.Reset()
	default:
		// Pool exhausted: allocate a new standard slab and tag it.
		buf = allocate(p.poolSize)
		buf.pool = p
	}
	buf.refCount.Store(1)
	return buf
}

// AcquireAligned returns an MmapBuffer of at least the requested size.
// It uses the slabPool for standard sizes to avoid syscalls, or performs
// a one-off mmap for pathological cases.
func (p *MmapPool) AcquireAligned(size int64) *MmapBuffer {
	if size <= p.poolSize {
		// Happy Path: fits in our pre-mapped slabPool slabs
		return p.Acquire()
	}

	// Pathological Path: requires a larger one-off mmap
	return p.AcquireUnpooled(size)
}

func (p *MmapPool) AcquireUnpooled(size int64) *MmapBuffer {
	buf := allocate(size)
	// slabPool is left as nil; refCount 1 represents the initial owner.
	buf.refCount.Store(1)
	return buf
}

func (p *MmapPool) Release(buf *MmapBuffer) {
	if buf.pool == nil {
		return
	}

	buf.Reset()
	select {
	case p.buffers <- buf:
		// Hot return: RAM is preserved.
	default:
		// Pool is full. This was likely an "overflow" buffer
		// created during high load. Evict and free RAM.
		_ = unix.Madvise(buf.raw, unix.MADV_DONTNEED)

		// IMPORTANT: Sever the connection so that future Unpin()
		// calls on this specific slab don't hit the slabPool again.
		buf.pool = nil
	}
}
