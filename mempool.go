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
	refCount  atomic.Int32 // Changed to Int32 for CAS compatibility
	pool      *MmapPool
	onRelease []func()
	
	// SAFETY GUARD:
	// atomic boolean to detect double-free logic errors within a single lifecycle.
	// Note: Since structs are no longer pooled, this protects against calling
	// Unpin() twice on the same pointer, but "ABA" is solved by allocation.
	leased atomic.Bool
}

// TryInc attempts to increment the reference count safely.
// It returns true if successful.
// It returns false if the buffer is already closed/recycled (refCount <= 0).
//
// This allows a Reader to "resurrect" a slab from a list without holding a lock,
// protecting against the race where the slab was evicted just as we accessed it.
func (b *MmapBuffer) TryInc() bool {
	for {
		c := b.refCount.Load()
		if c <= 0 {
			// It's dead. The Librarian evicted it before we could grab it.
			return false
		}
		// Attempt to increment from C to C+1
		if b.refCount.CompareAndSwap(c, c+1) {
			return true
		}
		// CAS failed (someone else modified refCount), retry loop
	}
}

// WriteAt performs a copy into the buffer.
func (b *MmapBuffer) WriteAt(p []byte, off int64) {
	copy(b.raw[off:off+int64(len(p))], p)
}

// Bytes returns the full physical slice.
func (b *MmapBuffer) Bytes() []byte {
	return b.raw
}

// AlignedBytes returns the slice rounded to the nearest 4KB page.
func (b *MmapBuffer) AlignedBytes() []byte {
	off := b.wPos.Load()
	if off < 0 {
		return nil
	}
	return b.raw[:roundToPage(off)]
}

// Seal finalizes the buffer size.
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
		// 2. Clear hooks (helpful for GC, though struct is dying anyway)
		b.onRelease = nil
		
		// 3. Reset state and return memory to pool (or Munmap)
		b.resetAndRelease()
	}
}

// AddOnRelease registers a callback to be run when the slab is recycled.
func (b *MmapBuffer) AddOnRelease(fn func()) {
	b.onRelease = append(b.onRelease, fn)
}

func (b *MmapBuffer) resetAndRelease() {
	// Mark as not leased to fail any subsequent Unpin calls
	if !b.leased.CompareAndSwap(true, false) {
		// This should only happen if Unpin is called on an already dead object,
		// which refCount 0 -> -1 protection usually catches first.
		return
	}
	
	b.Reset()
	
	if b.pool != nil {
		// POOLED: Return the raw bytes to the pool.
		// The struct 'b' is abandoned and will be GC'd.
		b.pool.ReleaseBytes(b.raw)
	} else {
		// UNPOOLED: Physical cleanup.
		_ = unix.Munmap(b.raw)
	}
}

// NewSectionReader creates a ReadCloser for a range of the buffer.
func (b *MmapBuffer) NewSectionReader(offset, size int64) io.ReadCloser {
	if offset < 0 || size < 0 || offset+size > int64(len(b.raw)) {
		return &MmapHandle{Reader: bytes.NewReader(nil)}
	}
	
	// Use TryInc for consistency, though callers usually hold a valid ref here.
	if !b.TryInc() {
		return &MmapHandle{Reader: bytes.NewReader(nil)}
	}
	
	h := &MmapHandle{
		Reader: bytes.NewReader(b.raw[offset : offset+size]),
		buffer: b,
	}
	h.cleanup = runtime.AddCleanup(h, func(buf *MmapBuffer) { buf.Unpin() }, b)
	return h
}

// --- MmapHandle, Helpers ---

type MmapHandle struct {
	*bytes.Reader
	buffer  *MmapBuffer
	once    sync.Once
	cleanup runtime.Cleanup
}

func (h *MmapHandle) Close() error {
	h.once.Do(func() {
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

// allocateRaw mmaps requested size. Returns raw bytes.
func allocateRaw(size int64) []byte {
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
	return data
}

// NewMmapBuffer allocates a standalone (unpooled) mmap buffer.
// It will be Unmapped when Unpin() reduces refCount to 0.
func NewMmapBuffer(size int64) *MmapBuffer {
	raw := allocateRaw(size)
	buf := &MmapBuffer{
		raw: raw,
	}
	buf.Reset()
	buf.refCount.Store(1)
	buf.leased.Store(true)
	return buf
}

// --- MmapPool ---

type MmapPool struct {
	// buffers holds the raw byte slices.
	// We pool the memory, NOT the structs, to solve the ABA problem.
	buffers     chan []byte
	poolSize    int64
	outstanding atomic.Int64
	name        string
}

func NewMmapPool(name string, bufferSize int64, headroom int64, capacity int) *MmapPool {
	p := &MmapPool{
		buffers:  make(chan []byte, capacity),
		poolSize: bufferSize + headroom,
		name:     name,
	}
	// Pre-fill
	for i := 0; i < capacity; i++ {
		p.buffers <- allocateRaw(bufferSize + headroom)
	}
	return p
}

func (p *MmapPool) Acquire() *MmapBuffer {
	var raw []byte
	
	// 1. Get Memory (Block if empty)
	for raw == nil {
		select {
		case raw = <-p.buffers:
			p.outstanding.Add(1)
		case <-time.After(10 * time.Second):
			log.Error("timeout acquiring buffers",
				"pool", p.name, "outstanding", p.outstanding.Load())
		}
	}
	
	// 2. Wrap in FRESH struct
	// This ensures that any pointer to an old MmapBuffer (held by a reader)
	// remains distinct from this new one, even if they share the same memory address.
	buf := &MmapBuffer{
		raw:  raw,
		pool: p,
	}
	
	buf.Reset()
	buf.refCount.Store(1)
	buf.leased.Store(true)
	
	return buf
}

// AcquireAligned returns an MmapBuffer of at least the requested size.
func (p *MmapPool) AcquireAligned(size int64) *MmapBuffer {
	if size <= p.poolSize {
		// Happy Path: fits in our pre-mapped pool
		return p.Acquire()
	}
	
	// Pathological Path: requires a larger one-off mmap
	return NewMmapBuffer(size)
}

func (p *MmapPool) ReleaseBytes(raw []byte) {
	p.outstanding.Add(-1)

	select {
	case p.buffers <- raw:
		// Happy Return.
	default:
		// Overflow (Pool is full).
		log.Error("the pool buffer is full")
		_ = unix.Madvise(raw, unix.MADV_DONTNEED)
		_ = unix.Munmap(raw)
	}
}

// Close releases all pre-allocated mmap buffers.
// Must be called when the pool is no longer needed to avoid memory leaks.
func (p *MmapPool) Close() {
	close(p.buffers)
	for raw := range p.buffers {
		_ = unix.Munmap(raw)
	}
}
