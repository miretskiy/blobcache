package blobstore

import (
	"cmp"
	"io"
	"iter"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/internal/intrusive"
	"github.com/miretskiy/dio/v2/iosched"
)

// A node can be reused independently of its position in index.db.
type indexEntry struct {
	header  RecordHeader
	offset  int64 // byte offset in index.db
	pending bool  // reservation remains protected through publication
}

// diskIndex orders inline and external reservations separately. Freed index
// slots are reused independently of data.db. The caller serializes access except size.
type diskIndex struct {
	file           *os.File
	inline         intrusive.List[indexEntry]
	external       intrusive.List[indexEntry]
	freeSlots      []int64
	nextObjectID   uint64
	appendOffset   int64
	dataSize       atomic.Uint64
	publishedBytes uint64 // cumulative payload admitted since open; protected by Store.mu
}

// openDiskIndex restores FIFO order from ObjectIDs. Removed records whose slots
// have not been overwritten can return on recovery; no reclamation boundary is persisted.
func openDiskIndex(path string, verifyChecksums bool) (_ *diskIndex, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	idx := &diskIndex{file: f, nextObjectID: uint64(time.Now().UnixNano())}
	var entries []indexEntry
	var offset int64
	buffer := make([]byte, 1024*RecordHeaderSize)
	buffered := 0
	for {
		n, readErr := f.Read(buffer[buffered:])
		data := buffer[:buffered+n]
		for len(data) >= RecordHeaderSize {
			raw := (*RawHeader)(data)
			if *raw != (RawHeader{}) { // Sparse slots belong to unpublished reservations.
				entry := indexEntry{offset: offset}
				if err := raw.Decode(&entry.header, verifyChecksums); err != nil {
					return nil, err
				}
				idx.nextObjectID = max(idx.nextObjectID, entry.header.ObjectID+1)
				if entry.header.Flags.IsDeleted() {
					idx.freeSlots = append(idx.freeSlots, offset)
				} else {
					entries = append(entries, entry)
				}
			} else {
				idx.freeSlots = append(idx.freeSlots, offset)
			}
			offset += RecordHeaderSize
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
	slices.SortFunc(entries, func(a, b indexEntry) int { return cmp.Compare(a.header.ObjectID, b.header.ObjectID) })
	var dataEnd uint64
	for _, entry := range entries {
		h := entry.header
		// Replay cursor progress, including gaps left by unpublished reservations.
		// External headers carry the data cursor without advancing it themselves.
		start, end := dataEnd, h.dataEnd()
		if h.Offset < dataEnd {
			start = 0
		}
		for first, covered := idx.coveredInline(start, end, dataEnd); covered; first, covered = idx.coveredInline(start, end, dataEnd) {
			idx.remove(first, 0)
		}
		dataEnd = end
		idx.records(h.Flags).PushBack(entry)
		idx.dataSize.Add(h.PhysicalLen)
	}
	idx.appendOffset = offset
	return idx, nil
}

func (idx *diskIndex) close() error { return idx.file.Close() }

// records selects the reservation queue; the record's storage kind never changes.
func (idx *diskIndex) records(flags RecordFlags) *intrusive.List[indexEntry] {
	if flags.IsExternal() {
		return &idx.external
	}
	return &idx.inline
}

// reserve claims an ObjectID and an index slot. Data placement belongs to Store.
func (idx *diskIndex) reserve(dataOffset uint64, flags RecordFlags) intrusive.Handle {
	offset := idx.appendOffset
	if n := len(idx.freeSlots); n != 0 {
		offset = idx.freeSlots[n-1]
		idx.freeSlots = idx.freeSlots[:n-1]
	} else {
		idx.appendOffset += RecordHeaderSize
	}
	handle := idx.records(flags).PushBack(indexEntry{
		header:  RecordHeader{Flags: flags, ObjectID: idx.nextObjectID, Offset: dataOffset},
		offset:  offset,
		pending: true,
	})
	idx.nextObjectID++
	return handle
}

// publish fills a reservation and encodes its single index write. Publication
// completion releases the reservation.
func (idx *diskIndex) publish(handle intrusive.Handle, h RecordHeader) iosched.Op {
	entry := idx.records(h.Flags).Value(handle)
	idx.dataSize.Add(h.PhysicalLen)
	idx.publishedBytes += h.PhysicalLen
	entry.header = h
	var raw RawHeader
	h.Encode(&raw)
	return iosched.WriteOp(idx.file, raw[:], entry.offset)
}

// coveredInline finds the oldest inline record retired by an allocation.
// A wrap also retires the unused tail of the older lap, preserving physical
// append order. Recovery uses the same rule to discard overwritten headers.
func (idx *diskIndex) coveredInline(start, end, previousEnd uint64) (intrusive.Handle, bool) {
	first, ok := idx.inline.Front()
	if !ok {
		return 0, false
	}
	offset := idx.inline.Value(first).header.Offset
	return first, offset >= start && offset < end || start < previousEnd && offset >= previousEnd
}

// remove retires a completed record or abandons a reservation whose I/O is drained.
func (idx *diskIndex) remove(handle intrusive.Handle, flags RecordFlags) RecordHeader {
	records := idx.records(flags)
	entry := records.Value(handle)
	h := entry.header
	idx.dataSize.Add(-h.PhysicalLen)
	idx.freeSlots = append(idx.freeSlots, entry.offset)
	records.Remove(handle)
	return h
}

func (idx *diskIndex) size() uint64 { return idx.dataSize.Load() }

// headers merges the two reservation queues in ObjectID order for recovery.
func (idx *diskIndex) headers() iter.Seq[RecordHeader] {
	return func(yield func(RecordHeader) bool) {
		a, hasA := idx.inline.Front()
		b, hasB := idx.external.Front()
		for hasA || hasB {
			if !hasB || hasA && idx.inline.Value(a).header.ObjectID < idx.external.Value(b).header.ObjectID {
				if !yield(idx.inline.Value(a).header) {
					return
				}
				a, hasA = idx.inline.Next(a)
			} else {
				if !yield(idx.external.Value(b).header) {
					return
				}
				b, hasB = idx.external.Next(b)
			}
		}
	}
}
