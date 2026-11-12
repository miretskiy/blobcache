package blobstore

import (
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/collections"
)

// Size reports indexed payload bytes. It excludes framing, padding, index
// storage, uncommitted writes, and objects left behind by failed unlinks.
func (st *Store) Size() uint64 { return st.mu.index.size() }

// WriteStats snapshots live indexed payload and cumulative payload published
// since Open. Reclamation reduces liveBytes but never publishedBytes.
func (st *Store) WriteStats() (liveBytes, publishedBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.mu.index.size(), st.mu.index.publishedBytes
}

// TargetSize is the live-payload target. Puts can exceed it until the next
// reservation evicts the excess. data.db retains allocated blocks on wrap.
func (st *Store) TargetSize() uint64 { return uint64(st.cfg.targetSize) }

func (st *Store) excess() uint64 {
	if size, target := st.Size(), st.TargetSize(); size > target {
		return size - target
	}
	return 0
}

// evictLocked retires an oldest prefix holding at least bytes of payload and,
// for an inline reservation of inlineSize, every record its data range would
// overlap. External records in the prefix are retired too. A pending writer
// stops the sweep.
func (st *Store) evictLocked(bytes, inlineSize uint64) []RecordHeader {
	idx := st.mu.index
	start := st.dataStart(inlineSize)
	var retired []RecordHeader
	var evicted uint64
	for evicted < bytes || inlineSize != 0 && idx.covered(start+inlineSize, start < st.mu.dataAppendOffset) {
		first, ok := idx.records.Front()
		if !ok || idx.records.Value(first).pending {
			break
		}
		h := idx.remove(first)
		evicted += h.PhysicalLen
		retired = append(retired, h)
	}
	return retired
}

// reserveRecord assigns the final frame identity during Seal.
func (st *Store) reserveRecord(key []byte, size int64) (collections.Handle, RecordHeader, error) {
	if len(key) == 0 {
		return 0, RecordHeader{}, ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return 0, RecordHeader{}, ErrKeyTooLarge
	}
	if size < 0 {
		return 0, RecordHeader{}, ErrCorrupt
	}
	h := RecordHeader{Flags: st.cfg.flags | FlagKeyHashXXH3, PhysicalLen: uint64(size),
		LogicalLen: uint64(size), KeyLen: uint16(len(key)), Hash: HashKey(key)}
	var inlineSize uint64
	if size > st.cfg.inlineThreshold {
		h.Flags |= FlagExternal
	} else {
		inlineSize = uint64(align.PageAlign(h.FrameLen()))
	}
	reservation, err := st.reserve(&h, inlineSize)
	return reservation, h, err
}

// complete follows cache publication so eviction cannot retire an unregistered value.
func (st *Store) complete(reservation collections.Handle, h RecordHeader, err error) {
	st.mu.Lock()
	if err == nil {
		st.mu.index.publish(reservation, h)
	} else {
		st.mu.index.remove(reservation)
	}
	st.mu.Unlock()
}

// reserve assigns an index entry and inline placement together. A data overlap
// evicts the whole FIFO prefix through that inline record. Active readers do
// not pin disk locations; unfinished writers prevent their reuse with ErrBusy.
func (st *Store) reserve(h *RecordHeader, inlineSize uint64) (collections.Handle, error) {
	st.mu.Lock()
	idx := st.mu.index
	retired := st.evictLocked(st.excess(), inlineSize)
	start := st.dataStart(inlineSize)
	if inlineSize != 0 && idx.covered(start+inlineSize, start < st.mu.dataAppendOffset) {
		st.mu.Unlock()
		st.notifyRetired(retired)
		return 0, ErrBusy
	}
	if inlineSize != 0 {
		if start < st.mu.dataAppendOffset {
			idx.dataWrapID = idx.nextObjectID
		}
		st.mu.dataAppendOffset = start + inlineSize
	}
	h.Offset = start
	handle := idx.reserve(start, h.Flags)
	h.ObjectID = idx.records.Value(handle).header.ObjectID
	st.mu.Unlock()
	st.notifyRetired(retired)
	return handle, nil
}

func (st *Store) dataStart(size uint64) uint64 {
	start := st.mu.dataAppendOffset
	if size != 0 && start+size > max(st.TargetSize(), size) {
		return 0
	}
	return start
}

// notifyRetired runs without Store.mu.
func (st *Store) notifyRetired(records []RecordHeader) {
	for _, h := range records {
		if st.cfg.onRetire != nil {
			st.cfg.onRetire(h)
		}
		if h.Flags.IsExternal() {
			st.unlinkObject(st.writeScheduler(), h.ObjectID)
		}
	}
}

// recoverIndex restores data placement from the newest retained reservation.
func (st *Store) recoverIndex() {
	idx := st.mu.index
	if newest, ok := idx.records.Back(); ok {
		st.mu.dataAppendOffset = idx.records.Value(newest).header.dataEnd()
	}
}
