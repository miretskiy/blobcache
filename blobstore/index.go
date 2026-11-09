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
)

type indexEntry struct {
	header  RecordHeader
	pending bool // unpublished: a writer owns the reservation
}

// The index is one in-memory FIFO for both storage kinds, in reservation order.
// It is persisted only by Close as a snapshot; a crash loses records since the
// last snapshot, and stale entries from an older snapshot read as misses.
// The caller serializes access except size.
type diskIndex struct {
	records collections.List[indexEntry] // reservation order, including pending writers

	dataWrapID   uint64 // first ObjectID in the current data.db lap; zero before wrapping
	nextObjectID uint64 // next reservation's ID; seeded from time and recovered IDs

	dataSize       atomic.Uint64
	publishedBytes uint64
}

// loadDiskIndex reads the snapshot written by save, if any. It also accepts
// the earlier slot format, skipping empty and wiped slots. Opening the index
// never rewrites it.
func loadDiskIndex(path string, verifyChecksums bool) (*diskIndex, error) {
	idx := &diskIndex{nextObjectID: uint64(time.Now().UnixNano())}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return idx, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var headers []RecordHeader
	buffer := make([]byte, 1024*RecordHeaderSize)
	buffered := 0
	for {
		n, readErr := f.Read(buffer[buffered:])
		data := buffer[:buffered+n]
		for len(data) >= RecordHeaderSize {
			if raw := (*RawHeader)(data); *raw != (RawHeader{}) {
				var h RecordHeader
				if err := raw.Decode(&h, verifyChecksums); err != nil {
					return nil, err
				}
				idx.nextObjectID = max(idx.nextObjectID, h.ObjectID+1)
				if !h.Flags.IsDeleted() {
					headers = append(headers, h)
				}
			}
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
	slices.SortFunc(headers, func(a, b RecordHeader) int { return cmp.Compare(a.ObjectID, b.ObjectID) })
	var dataEnd uint64
	for _, h := range headers {
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
		idx.records.PushBack(indexEntry{header: h})
		idx.dataSize.Add(h.PhysicalLen)
	}
	return idx, nil
}

// save atomically replaces path with the published records.
func (idx *diskIndex) save(path string) error {
	buffer := make([]byte, 0, idx.records.Len()*RecordHeaderSize)
	for h := range idx.headers() {
		var raw RawHeader
		h.Encode(&raw)
		buffer = append(buffer, raw[:]...)
	}
	temp := path + ".tmp"
	f, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(buffer)
	err = errors.Join(err, f.Sync(), f.Close())
	if err == nil {
		err = os.Rename(temp, path)
	}
	return err
}

func (idx *diskIndex) reserve(dataOffset uint64, flags RecordFlags) collections.Handle {
	handle := idx.records.PushBack(indexEntry{
		header:  RecordHeader{Flags: flags, ObjectID: idx.nextObjectID, Offset: dataOffset},
		pending: true,
	})
	idx.nextObjectID++
	return handle
}

func (idx *diskIndex) publish(handle collections.Handle, h RecordHeader) {
	*idx.records.Value(handle) = indexEntry{header: h}
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

// remove forgets a record or an abandoned writer.
func (idx *diskIndex) remove(handle collections.Handle) RecordHeader {
	h := idx.records.Value(handle).header
	idx.dataSize.Add(-h.PhysicalLen)
	idx.records.Remove(handle)
	return h
}

func (idx *diskIndex) size() uint64 { return idx.dataSize.Load() }

func (idx *diskIndex) headers() iter.Seq[RecordHeader] {
	return func(yield func(RecordHeader) bool) {
		for _, entry := range idx.records.All() {
			if !entry.pending && !yield(entry.header) {
				return
			}
		}
	}
}
