package blobcache

import (
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/metadata"
	"github.com/zhangyunhao116/skipmap"
)

// SharedSlab represents a populated chunk of memory and its index.
type SharedSlab struct {
	buf   *MmapBuffer
	index *skipmap.Uint64Map[metadata.BlobRecord]
}

// Releaser is a zero-allocation handle for releasing a read lock or buffer.
type Releaser struct {
	slab   *SharedSlab
	buffer BufferHandle
}

func (r *Releaser) Release() {
	if r == nil {
		return
	}
	if r.slab != nil {
		r.slab.buf.Unpin()
		r.slab = nil
	}
	r.buffer.Release()
}

// Acquire attempts to get a lease on the data.
// For uncompressed blobs: Zero-copy slice into the slab buffer.
// For compressed blobs: Allocates and returns decompressed data.
// WAIT-FREE: This method uses no locks.
// Returns (data, releaser, found, errno). Caller should check errno even when found=true.
// NB: This is a low level method where the caller is expected to quickly consume
// the returned data and promptly release it via Releaser.
func (s *SharedSlab) Acquire(key Key) ([]byte, Releaser, bool, base.BlobErrno) {
	// 1. Lock-free lookup
	record, ok := s.index.Load(key)
	if !ok {
		return nil, Releaser{}, false, base.ErrNone
	}

	// 2. Check if record has existing error
	if record.HasError() {
		return nil, Releaser{}, true, record.Errno()
	}

	// 3. SAFE PIN (CAS Loop)
	// We attempt to promote our weak reference (from the list) to a strong reference.
	// If the Librarian evicted this slab while we were looking at it,
	// TryInc will return false, preventing us from using dead memory.
	if !s.buf.TryInc() {
		// The buffer died between step 1 and 2. Treat as a miss.
		return nil, Releaser{}, false, base.ErrNone
	}

	// 4. Get Physical Data (stored bytes)
	physicalData := s.buf.raw[record.Pos : record.Pos+record.PhysicalSize]
	releaser := Releaser{slab: s}

	// 5. Handle Decompression
	if record.IsCompressed() {
		defer releaser.Release() // Release slab when done using physicalData

		handle := AcquireBuffer(int(record.LogicalSize), int(record.LogicalSize))
		if err := compression.Decompress(record.Compression(), handle.Bytes(), physicalData); err != nil {
			handle.Release()
			return nil, Releaser{}, false, base.ErrDecompression
		}
		return handle.Bytes(), Releaser{buffer: handle}, true, base.ErrNone
	}

	// Uncompressed: zero-copy slice
	return physicalData, releaser, true, base.ErrNone
}

// ProtectedView handles the lifecycle automatically via a closure.
// Returns (found, errno).
func (s *SharedSlab) ProtectedView(key Key, fn func(data []byte)) (bool, base.BlobErrno) {
	data, releaser, found, errno := s.Acquire(key)
	if !found || errno != base.ErrNone {
		return found, errno
	}
	defer releaser.Release()

	fn(data)
	return true, base.ErrNone
}

// --- Write Access Safety ---

type ActiveSlab struct {
	SharedSlab
	wPos       int64
	writesDone *signal

	pendingWrites atomic.Int64
	retired       atomic.Bool

	// currentMaxSeq tracks the highest SeqID written to this slab.
	// Used during rotation to set maxSealedSeq in MemTable.
	// Accessed only under MemTable.mu.Lock, so no atomics needed.
	currentMaxSeq uint64
}

type FlushTicket struct {
	Active *ActiveSlab
}

func (as *ActiveSlab) PurchaseTicket() FlushTicket {
	// ActiveSlab is guaranteed alive by the MemTable lock,
	// but TryInc is technically safer and consistent.
	if !as.buf.TryInc() {
		panic("critical: attempted to flush a dead slab")
	}
	return FlushTicket{Active: as}
}

func (t FlushTicket) Redeem() {
	t.Active.buf.Unpin()
}

type signal struct {
	sync.Once
	ch chan struct{}
}

func newSignal() *signal {
	return &signal{
		ch: make(chan struct{}),
	}
}

func (s *signal) Close() {
	s.Do(func() {
		close(s.ch)
	})
}
