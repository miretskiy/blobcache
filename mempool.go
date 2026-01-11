package blobcache

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// --- MmapBuffer: The Physical Slab ---

type MmapBuffer struct {
	raw       []byte
	wPos      atomic.Int64
	refCount  atomic.Int64
	pool      *MmapPool
	onRelease []func()

	// SAFETY GUARD:
	// atomic boolean to detect double-free or use-after-free bugs.
	// false = In Pool (Safe to Acquire)
	// true  = Leased (Safe to Use)
	leased atomic.Bool
}

// WriteAt performs a copy into the buffer.
// It relies on Go's native sub-slicing bounds checks to panic if
// the range [off : off+len(p)] is invalid.
func (b *MmapBuffer) WriteAt(p []byte, off int64) {
	copy(b.raw[off:off+int64(len(p))], p)
}

// Bytes returns the full physical slice. Useful for internal serialization.
func (b *MmapBuffer) Bytes() []byte {
	return b.raw
}

// AlignedBytes returns the slice rounded to the nearest 4KB page for O_DIRECT.
// This is used by SegmentWriter to ensure hardware-aligned writes.
func (b *MmapBuffer) AlignedBytes() []byte {
	off := b.wPos.Load()
	if off < 0 {
		return nil
	}
	return b.raw[:roundToPage(off)]
}

// Seal finalizes the buffer size and marks the slab as ready for pool return.
// Once sealed (wPos >= 0), it can be reclaimed when the refCount hits zero.
func (b *MmapBuffer) Seal(finalOffset int64) {
	b.wPos.Store(finalOffset)
}

func (b *MmapBuffer) Len() int {
	off := b.wPos.Load()
	if off < 0 {
		return 0
	}
	return int(off)
}

func (b *MmapBuffer) Cap() int { return len(b.raw) }

func (b *MmapBuffer) Reset() {
	b.wPos.Store(-1) // Set back to Active sentinel
}

func (b *MmapBuffer) Unpin() {
	if b.refCount.Add(-1) == 0 {
		// 1. Execute all registered cleanup hooks
		for _, fn := range b.onRelease {
			fn()
		}

		// 2. Clear hooks so they don't leak or double-run next time
		b.onRelease = b.onRelease[:0]

		// 3. Reset buffer state and return to pool
		b.resetAndRelease()
	}
}

// AddOnRelease registers a callback to be run when the slab is recycled.
func (b *MmapBuffer) AddOnRelease(fn func()) {
	b.onRelease = append(b.onRelease, fn)
}

func (b *MmapBuffer) resetAndRelease() {
	b.Reset()
	if b.pool != nil {
		b.pool.Release(b)
	}
}

// NewSectionReader creates a ReadCloser for a range of the buffer.
// It increments the refCount to ensure the buffer is pinned while in use.
func (b *MmapBuffer) NewSectionReader(offset, size int64) io.ReadCloser {
	if offset < 0 || size < 0 || offset+size > int64(len(b.raw)) {
		return &MmapHandle{Reader: bytes.NewReader(nil)}
	}
	b.refCount.Add(1)
	h := &MmapHandle{
		Reader: bytes.NewReader(b.raw[offset : offset+size]),
		buffer: b,
	}
	h.cleanup = runtime.AddCleanup(h, func(buf *MmapBuffer) { buf.Unpin() }, b)
	return h
}

// --- MmapHandle, Helpers, and MmapPool ---

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
		panic(fmt.Sprintf("mmap-pool: failed to allocate %d bytes: %v", size, err))
	}

	// PRE-WARM: Force physical RAM commitment.
	for i := 0; i < len(data); i += 4096 {
		data[i] = 0
	}

	buf := &MmapBuffer{raw: data}
	buf.Reset()
	runtime.AddCleanup(buf, func(d []byte) { _ = unix.Munmap(d) }, data)
	return buf
}

type MmapPool struct {
	buffers     chan *MmapBuffer
	poolSize    int64
	outstanding atomic.Int64
	name        string
}

func NewMmapPool(name string, bufferSize int64, headroom int64, capacity int) *MmapPool {
	p := &MmapPool{
		buffers:  make(chan *MmapBuffer, capacity),
		poolSize: bufferSize + headroom,
		name:     name,
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
	for buf == nil {
		select {
		case buf = <-p.buffers:
			p.outstanding.Add(1)
		case <-time.After(10 * time.Second):
			log.Error(" timeout acquiring buffers",
				"pool", p.name, "outstanding", p.outstanding.Load())
		}
	}

	// SAFETY GUARD: Verify we aren't acquiring a buffer already in use.
	// This detects pool corruption (e.g., duplicate pointers in channel).
	if !buf.leased.CompareAndSwap(false, true) {
		panic(fmt.Sprintf("pool %s corruption: acquired buffer that is already leased!", p.name))
	}

	buf.Reset()
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
	// Unpooled buffers are not guarded by 'leased' because they don't return to the pool.
	buf.refCount.Store(1)
	return buf
}

func (p *MmapPool) Release(buf *MmapBuffer) {
	if buf.pool == nil {
		return
	}

	// SAFETY GUARD: Verify we are releasing a leased buffer.
	// This detects Double-Free bugs instantly.
	if !buf.leased.CompareAndSwap(true, false) {
		panic(fmt.Sprintf("pool %s double-free detected", p.name))
	}

	buf.Reset()
	p.outstanding.Add(-1)
	select {
	case p.buffers <- buf:
		// Happy Return.
	default:
		// Overflow (Pool is full).
		// This happens if we allocated extras during a peak load spike.
		log.Error("the pool buffer is full")
		_ = unix.Madvise(buf.raw, unix.MADV_DONTNEED)

		// IMPORTANT: Sever the connection so that future Unpin()
		// calls on this specific slab don't hit the slabPool again.
		buf.pool = nil
	}
}
