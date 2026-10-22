package blobstore

import (
	"cmp"
	"errors"
	"io"
	"iter"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/miretskiy/dio/v2/collections"
	"github.com/miretskiy/dio/v2/iosched"
)

type indexEntry struct {
	header  RecordHeader
	slot    uint64 // physical index slot; stable while the writer owns it
	pending bool   // writer owns the reservation until Close
}

// The index is one FIFO for both storage kinds. Cursors count logical slots:
// reuseHead <= head <= tail <= reuseHead + slots. Eviction advances head;
// completion of its wipe advances reuseHead. Only one wipe may be outstanding.
// A cursor's physical slot is cursor % slots.
// The caller serializes access except size.
type diskIndex struct {
	file    *os.File
	records collections.List[indexEntry] // reservation order, including pending writers

	head      uint64 // next logical index slot to retire
	reuseHead uint64 // completed index-wipe boundary
	tail      uint64 // next logical index slot to reserve
	slots     uint64 // index capacity in RecordHeaderSize slots

	dataWrapID   uint64 // first ObjectID in the current data.db lap; zero before wrapping
	nextObjectID uint64 // next reservation's ID; seeded from time and recovered IDs

	wipeErr        error // failed wipe prevents further slot reuse
	dataSize       atomic.Uint64
	publishedBytes uint64
}

// openDiskIndex reconstructs the ring from fixed headers and ObjectIDs, without
// persisted cursor updates. Holes inside the occupied span stay there until
// eviction reaches them. Opening the index never rewrites it.
func openDiskIndex(path string, verifyChecksums bool) (_ *diskIndex, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	idx := &diskIndex{file: f, nextObjectID: uint64(time.Now().UnixNano())}
	defer func() {
		if err != nil {
			idx.file.Close()
		}
	}()
	var entries []indexEntry
	var slot uint64
	buffer := make([]byte, 1024*RecordHeaderSize)
	buffered := 0
	for {
		n, readErr := f.Read(buffer[buffered:])
		data := buffer[:buffered+n]
		for len(data) >= RecordHeaderSize {
			raw := (*RawHeader)(data)
			if *raw != (RawHeader{}) {
				entry := indexEntry{slot: slot}
				if err := raw.Decode(&entry.header, verifyChecksums); err != nil {
					return nil, err
				}
				idx.nextObjectID = max(idx.nextObjectID, entry.header.ObjectID+1)
				if !entry.header.Flags.IsDeleted() {
					entries = append(entries, entry)
				}
			}
			slot++
			data = data[RecordHeaderSize:]
		}
		if readErr == io.EOF {
			break // An incomplete final entry was never published.
		}
		if readErr != nil {
			return nil, readErr
		}
		buffered = copy(buffer, data)
	}
	idx.slots = slot
	slices.SortFunc(entries, func(a, b indexEntry) int { return cmp.Compare(a.header.ObjectID, b.header.ObjectID) })
	var dataEnd uint64
	for _, entry := range entries {
		h := entry.header
		// Replay inline placement, including gaps left by unpublished writers.
		end, wrapping := h.dataEnd(), h.Offset < dataEnd
		for idx.covered(end, wrapping) {
			first, _ := idx.records.Front()
			idx.remove(first)
		}
		if wrapping {
			idx.dataWrapID = h.ObjectID
		}
		dataEnd = end
		idx.records.PushBack(entry)
		idx.dataSize.Add(h.PhysicalLen)
	}
	if first, ok := idx.records.Front(); ok {
		idx.head = idx.records.Value(first).slot
		idx.tail = idx.head
		for _, entry := range idx.records.All() {
			position := idx.position(entry.slot)
			if position < idx.tail {
				return nil, errors.New("blobstore: invalid index order")
			}
			idx.tail = position + 1
		}
	}
	idx.reuseHead = idx.head
	return idx, nil
}

func (idx *diskIndex) close() error { return idx.file.Close() }

// growable permits extending a full ring only at EOF, where live physical
// order is contiguous. Pending writers retain their slots across growth.
func (idx *diskIndex) growable() bool {
	return idx.head == idx.reuseHead && (idx.slots == 0 || idx.tail%idx.slots == 0)
}

func (idx *diskIndex) full() bool { return idx.tail-idx.reuseHead == idx.slots }

// position maps an occupied physical slot to its logical position at or after head.
func (idx *diskIndex) position(slot uint64) uint64 {
	position := idx.head - idx.head%idx.slots + slot
	if position < idx.head {
		position += idx.slots
	}
	return position
}

// reserve requires an available slot, or a full ring that is growable.
func (idx *diskIndex) reserve(dataOffset uint64, flags RecordFlags) collections.Handle {
	if idx.full() {
		if !idx.growable() {
			panic("blobstore: index reservation overlaps occupied slots")
		}
		// The full ring occupies physical slots [0, slots). Rebase the cursors
		// before changing the modulus; entries and pending writers stay put.
		idx.head, idx.reuseHead, idx.tail = 0, 0, idx.slots
		idx.slots++
	}
	handle := idx.records.PushBack(indexEntry{
		header:  RecordHeader{Flags: flags, ObjectID: idx.nextObjectID, Offset: dataOffset},
		slot:    idx.tail % idx.slots,
		pending: true,
	})
	idx.tail++
	idx.nextObjectID++
	return handle
}

func (idx *diskIndex) writeOp(handle collections.Handle, h RecordHeader) iosched.Op {
	var raw RawHeader
	h.Encode(&raw)
	return iosched.WriteOp(idx.file, raw[:], int64(idx.records.Value(handle).slot)*RecordHeaderSize)
}

func (idx *diskIndex) publish(handle collections.Handle, h RecordHeader) {
	idx.records.Value(handle).header = h
	idx.dataSize.Add(h.PhysicalLen)
	idx.publishedBytes += h.PhysicalLen
}

// covered checks the FIFO front against the data range being reused. External
// records carry the data cursor at reservation and leave with the same prefix.
// On wrap, discard the remainder of the older lap before entering the current
// one. The lap boundary distinguishes old offsets from fresh external records
// at the append position, without searching ahead for an inline record.
func (idx *diskIndex) covered(end uint64, wrapping bool) bool {
	first, ok := idx.records.Front()
	if !ok {
		return false
	}
	h := idx.records.Value(first).header
	olderLap := h.ObjectID < idx.dataWrapID
	if wrapping {
		return olderLap || h.Offset < end
	}
	return olderLap && h.Offset < end
}

// remove forgets a record or an abandoned writer. Its slot remains in the
// occupied span until the eviction cursor passes it and its wipe completes.
func (idx *diskIndex) remove(handle collections.Handle) RecordHeader {
	h := idx.records.Value(handle).header
	idx.dataSize.Add(-h.PhysicalLen)
	idx.records.Remove(handle)
	return h
}

// wipeOp clears a prefix of the occupied span, possibly crossing EOF. Its
// immutable buffer remains owned by the op until both writes have completed.
func (idx *diskIndex) wipeOp(start, end uint64) iosched.Op {
	buffer := make([]byte, (end-start)*RecordHeaderSize)
	free := RecordHeader{Flags: FlagDeleted}
	var raw RawHeader
	free.Encode(&raw)
	for offset := 0; offset < len(buffer); offset += RecordHeaderSize {
		copy(buffer[offset:], raw[:])
	}
	slot := start % idx.slots
	offset := int64(slot) * RecordHeaderSize
	first := min(len(buffer), int(idx.slots-slot)*RecordHeaderSize)
	op := iosched.WriteOp(idx.file, buffer[:first], offset)
	if first < len(buffer) {
		op = op.Link(iosched.WriteOp(idx.file, buffer[first:], 0))
	}
	return op
}

func (idx *diskIndex) size() uint64 { return idx.dataSize.Load() }

func (idx *diskIndex) headers() iter.Seq[RecordHeader] {
	return func(yield func(RecordHeader) bool) {
		for _, entry := range idx.records.All() {
			if !yield(entry.header) {
				return
			}
		}
	}
}
