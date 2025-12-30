package blobcache

import (
	"errors"
	"fmt"
	"runtime"

	"golang.org/x/sys/unix"
)

var ErrBufferFull = errors.New("mmap pool: buffer capacity exceeded")

// MmapBuffer is an off-heap, page-aligned memory region.
// It is protected from Go heap escapes by encapsulating the slice header.
type MmapBuffer struct {
	raw []byte
	off int
}

// Write copies data into the off-heap region. It is atomic:
// if len(p) > Available(), it returns 0 and ErrBufferFull.
func (b *MmapBuffer) Write(p []byte) (n int, err error) {
	if len(p) > b.Available() {
		return 0, ErrBufferFull
	}
	n = copy(b.raw[b.off:], p)
	b.off += n
	return n, nil
}

func (b *MmapBuffer) Available() int { return len(b.raw) - b.off }
func (b *MmapBuffer) Bytes() []byte  { return b.raw[:b.off] }
func (b *MmapBuffer) Len() int       { return b.off }
func (b *MmapBuffer) Cap() int       { return len(b.raw) }
func (b *MmapBuffer) Reset()         { b.off = 0 }

// --- Internal Allocator ---

func allocate(size int) (*MmapBuffer, error) {
	data, err := unix.Mmap(-1, 0, size,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	// Pre-warm: force physical RAM commitment to avoid page-fault stutter.
	for j := 0; j < len(data); j += 4096 {
		data[j] = 0
	}

	buf := &MmapBuffer{raw: data}

	// Safety net: Ensures Munmap is called when buf is no longer reachable.
	runtime.SetFinalizer(buf, func(b *MmapBuffer) {
		_ = unix.Munmap(data)
	})

	return buf, nil
}

// --- Pool Abstraction ---

type MmapPool struct {
	buffers  chan *MmapBuffer
	poolSize int
}

func NewMmapPool(capacity int, bufferSize int) *MmapPool {
	p := &MmapPool{
		buffers:  make(chan *MmapBuffer, capacity),
		poolSize: bufferSize,
	}

	for i := 0; i < capacity; i++ {
		buf, err := allocate(bufferSize)
		if err != nil {
			panic(fmt.Sprintf("mmap-pool: critical boot failure: %v", err))
		}
		p.buffers <- buf
	}
	return p
}

// Acquire pulls from the pool. Blocks if empty, providing natural backpressure.
func (p *MmapPool) Acquire() *MmapBuffer {
	buf := <-p.buffers
	buf.Reset()
	return buf
}

// AcquireUnpooled allocates a one-off buffer for large blobs.
func (p *MmapPool) AcquireUnpooled(size int) (*MmapBuffer, error) {
	return allocate(size)
}

// Release returns the buffer to the pool if it matches the pool size,
// otherwise it discards the reference for GC reclamation.
func (p *MmapPool) Release(buf *MmapBuffer) {
	if buf.Cap() != p.poolSize {
		buf.raw = nil // Defensive: prevent use-after-release
		return
	}
	p.buffers <- buf
}
