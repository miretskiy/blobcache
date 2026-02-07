package blobcache

import (
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// SlabEntry is stored in the slab's in-memory index.
// Embeds record.Header to access compression/error flags and sizes.
type SlabEntry struct {
	record.Header       // Flags, SeqID, PhysicalSize, LogicalSize (Magic/KeyLen unused here)
	Pos           int64 // Byte offset within slab buffer (for Librarian reads)
	WalPos        int64 // Byte offset within WAL file (for footer when WAL enabled)

	// XLBuf is set for extra large writes that are not part of the normal buffer.
	// XLBuf must be explicitly released to the pool.
	XLBuf *MmapBuffer
}

// SharedSlab represents a populated chunk of memory and its index.
type SharedSlab struct {
	buf   *MmapBuffer
	index *xmap.Map[SlabEntry, xmap.Pad32]
	bloom *bloom.Filter // Fast rejection filter (frozen when slab retired)
}

// Releaser is a zero-allocation handle for releasing a read lock or buffer.
type Releaser struct {
	slab *MmapBuffer
	bh   *BufferHandle
}

func (r *Releaser) Release() {
	if r == nil {
		return
	}
	if r.slab != nil {
		r.slab.Unpin()
		r.slab = nil
	}
	if r.bh != nil {
		r.bh.Release()
	}
	*r = Releaser{}
}

// Acquire attempts to get a lease on the data.
// For uncompressed blobs: Zero-copy slice into the slab buffer.
// For compressed blobs: Allocates and returns decompressed data.
// Returns (data, keyBytes, releaser, found, errno). Caller should check errno even when found=true.
// keyBytes is a zero-copy slice into the slab buffer containing the stored key.
// Caller should verify keyBytes matches the expected key to detect hash collisions.
// NB: This is a low level method where the caller is expected to quickly consume
// the returned data and promptly release it via Releaser.
func (s *SharedSlab) Acquire(key Key) ([]byte, []byte, Releaser, bool, base.BlobErrno) {
	// 0. Bloom filter fast rejection (lock-free)
	if s.bloom != nil && !s.bloom.Test(key) {
		return nil, nil, Releaser{}, false, base.ErrNone
	}

	// 1. Lock-free lookup
	rec, ok := s.index.Get(key)
	if !ok {
		return nil, nil, Releaser{}, false, base.ErrNone
	}

	// 2. Check if record has existing error
	if rec.HasError() {
		return nil, nil, Releaser{}, true, rec.Errno()
	}

	buf := s.buf
	offset := rec.Pos
	if rec.XLBuf != nil {
		buf = rec.XLBuf
		offset = record.FileHeaderSize // XL data always starts after file header in XLBuf
	}

	// 3. SAFE PIN (CAS Loop)
	// We attempt to promote our weak reference (from the list) to a strong reference.
	// If the Librarian evicted this slab while we were looking at it,
	// TryInc will return false, preventing us from using dead memory.
	if !buf.TryInc() {
		// The buffer died between step 1 and 2. Treat as a miss.
		return nil, nil, Releaser{}, false, base.ErrNone
	}

	// 4. Extract key bytes (zero-copy slice for collision detection)
	keyStart := offset + int64(record.HeaderSize)
	keyEnd := keyStart + int64(rec.KeyLen)
	keyBytes := buf.raw[keyStart:keyEnd]

	// 5. Get Physical Data (skip past header and key to value bytes)
	physicalData := buf.raw[keyEnd : keyEnd+rec.PhysicalSize]
	releaser := Releaser{slab: buf}

	// 6. Handle Decompression
	if rec.IsCompressed() {
		handle := AcquireBuffer(int(rec.LogicalSize), int(rec.LogicalSize))
		if err := compression.Decompress(rec.Compression(), handle.Bytes(), physicalData); err != nil {
			handle.Release()
			releaser.Release()
			return nil, nil, Releaser{}, false, base.ErrDecompression
		}
		// Keep slab pinned for keyBytes, add bh for decompressed value
		return handle.Bytes(), keyBytes, Releaser{slab: buf, bh: &handle}, true, base.ErrNone
	}

	// Uncompressed: zero-copy slice
	return physicalData, keyBytes, releaser, true, base.ErrNone
}

// ProtectedView handles the lifecycle automatically via a closure.
// Returns (found, errno). The callback receives the stored key and value.
func (s *SharedSlab) ProtectedView(hashKey Key, fn func(storedKey, value []byte)) (bool, base.BlobErrno) {
	value, storedKey, releaser, found, errno := s.Acquire(hashKey)
	if !found || errno != base.ErrNone {
		return found, errno
	}
	defer releaser.Release()

	fn(storedKey, value)
	return true, base.ErrNone
}

// Invalidate removes a key from the slab's index.
// Used by Delete to prevent serving stale data from the Librarian cache.
func (s *SharedSlab) Invalidate(key Key) {
	s.index.Delete(key)
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

	// xlSize tracks cumulative size of XL (extra large) buffers in this slab.
	// Used to force rotation when XL writes accumulate excessively.
	// Without this, a workload of only XL writes would never rotate
	// (since XL writes don't consume space in the main slab buffer).
	// Accessed only under MemTable.mu.Lock, so no atomics needed.
	xlSize int64
}

// Alloc reserves n bytes in the slab at a block-aligned offset.
// Returns:
//   - buf: A slice window into the reserved memory (safe to write to).
//   - offset: The absolute offset of the start of the buffer (for the Index).
//
// Every record starts at a 4KB boundary so that segments are born with aligned
// offsets, enabling XFS reflinks via copy_file_range during compaction.
//
// If there isn't enough capacity, it returns nil, 0.
// The caller should use EncodeTo methods to write directly into buf.
func (as *ActiveSlab) Alloc(n int) (buf []byte, offset int64) {
	as.wPos = sys.PageAlign(as.wPos)
	offset = as.wPos
	end := offset + int64(n)
	if end > int64(as.buf.Cap()) {
		return nil, 0
	}
	as.wPos = end
	return as.buf.raw[offset:end], offset
}

func (as *ActiveSlab) AlignPosToPageBoundary() int64 {
	as.wPos = sys.PageAlign(as.wPos)
	return as.wPos
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
