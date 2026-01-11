package blobcache

import (
	"sync"
	"sync/atomic"
	
	"github.com/miretskiy/blobcache/metadata"
	"github.com/zhangyunhao116/skipmap"
)

// SharedSlab represents a populated chunk of memory and its index.
type SharedSlab struct {
	buf   *MmapBuffer
	index *skipmap.Uint64Map[metadata.BlobRecord]
}

// Releaser is a zero-allocation handle for releasing a read lock.
type Releaser struct {
	slab *SharedSlab
}

func (r *Releaser) Release() {
	if r == nil || r.slab == nil {
		return
	}
	r.slab.buf.Unpin()
	r.slab = nil
}

// Acquire attempts to get a Zero-Copy lease on the data.
// WAIT-FREE: This method uses no locks.
func (s *SharedSlab) Acquire(key Key) ([]byte, Releaser, bool) {
	// 1. Lock-free lookup
	record, ok := s.index.Load(key)
	if !ok {
		return nil, Releaser{}, false
	}
	
	// 2. SAFE PIN (CAS Loop)
	// We attempt to promote our weak reference (from the list) to a strong reference.
	// If the Librarian evicted this slab while we were looking at it,
	// TryInc will return false, preventing us from using dead memory.
	if !s.buf.TryInc() {
		// The buffer died between step 1 and 2. Treat as a miss.
		return nil, Releaser{}, false
	}
	
	// 3. Construct Return Values
	data := s.buf.raw[record.Pos : record.Pos+record.LogicalSize]
	releaser := Releaser{slab: s}
	
	return data, releaser, true
}

// ProtectedView handles the lifecycle automatically via a closure.
func (s *SharedSlab) ProtectedView(key Key, fn func(data []byte)) bool {
	data, releaser, found := s.Acquire(key)
	if !found {
		return false
	}
	defer releaser.Release()
	
	fn(data)
	return true
}

// --- Write Access Safety ---

type ActiveSlab struct {
	SharedSlab
	wPos       int64
	writesDone *signal

	pendingWrites atomic.Int64
	retired       atomic.Bool
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
