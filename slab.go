package blobcache

import (
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// SlabEntry is stored in the slab's in-memory index.
// Embeds record.Header to access compression/error flags and sizes.
type SlabEntry struct {
	record.Header       // Flags, SeqID, PhysicalSize, LogicalSize (Magic/KeyLen unused here)
	Pos           int64 // Byte offset within slab buffer
}

// SharedSlab represents a populated chunk of memory and its index.
type SharedSlab struct {
	buf   *MmapBuffer
	index *xmap.Map[SlabEntry, xmap.Pad32]
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
	rec, ok := s.index.Get(key)
	if !ok {
		return nil, Releaser{}, false, base.ErrNone
	}

	// 2. Check if record has existing error
	if rec.HasError() {
		return nil, Releaser{}, true, rec.Errno()
	}

	// 3. SAFE PIN (CAS Loop)
	// We attempt to promote our weak reference (from the list) to a strong reference.
	// If the Librarian evicted this slab while we were looking at it,
	// TryInc will return false, preventing us from using dead memory.
	if !s.buf.TryInc() {
		// The buffer died between step 1 and 2. Treat as a miss.
		return nil, Releaser{}, false, base.ErrNone
	}

	// 4. Get Physical Data (skip past header and key to value bytes)
	valueStart := rec.Pos + int64(record.HeaderSize) + int64(rec.KeyLen)
	physicalData := s.buf.raw[valueStart : valueStart+rec.PhysicalSize]
	releaser := Releaser{slab: s}

	// 5. Handle Decompression
	if rec.IsCompressed() {
		defer releaser.Release() // Release slab when done using physicalData

		handle := AcquireBuffer(int(rec.LogicalSize), int(rec.LogicalSize))
		if err := compression.Decompress(rec.Compression(), handle.Bytes(), physicalData); err != nil {
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

type ActiveSlab struct {
	SharedSlab
	wPos       int64
	writesDone *signal

	pendingWrites atomic.Int64
	retired       atomic.Bool

	// walFileID is the first SeqID of the WAL file containing this slab's records.
	// Set by EnqueueRotation() when the slab is rotated, which returns the ID of
	// the WAL file that was closed. Used for WAL.DeleteFile() after segment flush.
	// 0 means no WAL file to delete (e.g., large writes share the active slab's file).
	walFileID uint64

	// currentMaxSeq tracks the highest SeqID written to this slab.
	// Used during rotation to set maxSealedSeq in MemTable.
	// Accessed only under MemTable.mu.Lock, so no atomics needed.
	currentMaxSeq uint64
}

// Alloc reserves n bytes in the slab.
// Returns:
//   - buf: A slice window into the reserved memory (safe to write to).
//   - offset: The absolute offset of the start of the buffer (for the Index).
//
// If there isn't enough capacity, it returns nil, 0.
// The caller should use EncodeTo methods to write directly into buf.
func (as *ActiveSlab) Alloc(n int) (buf []byte, offset int64) {
	offset = as.wPos
	end := offset + int64(n)
	if end > int64(as.buf.Cap()) {
		return nil, 0
	}
	as.wPos = end
	return as.buf.raw[offset:end], offset
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
