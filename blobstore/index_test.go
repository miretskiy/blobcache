package blobstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/internal/intrusive"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func newTestDiskIndex(t *testing.T) (*diskIndex, *iosched.POSIXScheduler) {
	t.Helper()
	idx, err := openDiskIndex(filepath.Join(t.TempDir(), "index.db"), true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idx.close()) })
	scheduler := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, scheduler.Close()) })
	return idx, scheduler
}

func reopenTestIndex(t *testing.T, idx *diskIndex) *diskIndex {
	t.Helper()
	reopened, err := openDiskIndex(idx.file.Name(), true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.close()) })
	return reopened
}

func submitIndexOp(t *testing.T, scheduler iosched.Scheduler, op iosched.Op) {
	t.Helper()
	ticket, err := scheduler.Submit(op)
	require.NoError(t, err)
	n, err := ticket.Wait()
	require.NoError(t, err)
	require.Equal(t, RecordHeaderSize, n)
}

func readIndexSlot(t *testing.T, idx *diskIndex, slot uint64) RawHeader {
	t.Helper()
	var raw RawHeader
	_, err := idx.file.ReadAt(raw[:], int64(slot)*RecordHeaderSize)
	require.NoError(t, err)
	return raw
}

func indexHeaders(idx *diskIndex) map[uint64]RecordHeader {
	headers := make(map[uint64]RecordHeader)
	for h := range idx.headers() {
		if h.KeyLen != 0 && !h.Flags.IsDeleted() {
			headers[h.ObjectID] = h
		}
	}
	return headers
}

func indexReservations(idx *diskIndex) []RecordHeader {
	var headers []RecordHeader
	for h := range idx.headers() {
		headers = append(headers, h)
	}
	return headers
}

func indexFileSize(t *testing.T, idx *diskIndex) int64 {
	t.Helper()
	info, err := idx.file.Stat()
	require.NoError(t, err)
	return info.Size()
}

func indexTestHeader(offset, size uint64, tag byte) RecordHeader {
	return RecordHeader{
		Flags: FlagHeaderIntegrity, Offset: offset, PhysicalLen: size, LogicalLen: size,
		KeyLen: 1, Hash: KeyHash{Lo: uint64(tag)},
	}
}

func publishIndexHeader(t *testing.T, idx *diskIndex, scheduler iosched.Scheduler, handle intrusive.Handle, h RecordHeader) RecordHeader {
	t.Helper()
	h.ObjectID = idx.records(h.Flags).Value(handle).header.ObjectID
	submitIndexOp(t, scheduler, idx.publish(handle, h))
	idx.records(h.Flags).Value(handle).pending = false // Both index I/O and writer handoff are complete.
	return h
}

func putIndexHeader(t *testing.T, idx *diskIndex, scheduler iosched.Scheduler, h RecordHeader) RecordHeader {
	t.Helper()
	return publishIndexHeader(t, idx, scheduler, idx.reserve(h.Offset, h.Flags), h)
}

func TestIndexReservationAndOutOfOrderCompletion(t *testing.T) {
	before := uint64(time.Now().UnixNano())
	idx, scheduler := newTestDiskIndex(t)
	inline := indexTestHeader(0, 12345, 1)
	external := indexTestHeader(inline.dataEnd(), 1<<33, 2)
	first, second := idx.reserve(0, 0), idx.reserve(external.Offset, FlagExternal)
	inline.ObjectID = idx.inline.Value(first).header.ObjectID
	external.ObjectID = idx.external.Value(second).header.ObjectID
	require.GreaterOrEqual(t, inline.ObjectID, before)
	require.Equal(t, inline.ObjectID+1, external.ObjectID)
	require.Empty(t, indexHeaders(idx))
	require.Zero(t, idx.size())
	require.Zero(t, indexFileSize(t, idx), "no index-specific prefix")
	external.Flags |= FlagExternal
	external.Flags.SetCompression(CompressionZstd)
	external.LogicalLen = 1 << 35
	firstOp := idx.publish(first, inline)
	secondOp := idx.publish(second, external)
	submitIndexOp(t, scheduler, secondOp)
	require.Equal(t, RawHeader{}, readIndexSlot(t, idx, 0))
	partial := reopenTestIndex(t, idx)
	require.Greater(t, partial.nextObjectID, external.ObjectID)
	require.Equal(t, map[uint64]RecordHeader{external.ObjectID: external}, indexHeaders(partial))
	submitIndexOp(t, scheduler, firstOp)
	reopened := reopenTestIndex(t, idx)
	require.Equal(t, []RecordHeader{inline, external}, indexReservations(reopened))
	require.Equal(t, inline.PhysicalLen+external.PhysicalLen, reopened.size())
	var raw RawHeader
	inline.Encode(&raw)
	require.Equal(t, raw, readIndexSlot(t, idx, 0), "the index stores the exact record header")
}

func TestIndexReusesRetiredSlotsWithoutPersistingDeletion(t *testing.T) {
	idx, scheduler := newTestDiskIndex(t)
	var old []RecordHeader
	for i := range 4 {
		old = append(old, putIndexHeader(t, idx, scheduler, indexTestHeader(uint64(i)*4096, 100, byte(i))))
	}
	before, err := os.ReadFile(idx.file.Name())
	require.NoError(t, err)
	for first, ok := idx.inline.Front(); ok; first, ok = idx.inline.Front() {
		idx.remove(first, 0)
	}
	after, err := os.ReadFile(idx.file.Name())
	require.NoError(t, err)
	require.Equal(t, before, after)
	h := putIndexHeader(t, idx, scheduler, indexTestHeader(16384, 200, 4))
	reopened := reopenTestIndex(t, idx)
	require.Equal(t, append(old[:3], h), indexReservations(reopened), "unoverwritten headers can return")
	require.EqualValues(t, 4*RecordHeaderSize, indexFileSize(t, idx), "reuse does not grow index.db")
}

func TestIndexRecoveryHandlesAbandonedReservationAtWrap(t *testing.T) {
	idx, scheduler := newTestDiskIndex(t)
	putIndexHeader(t, idx, scheduler, indexTestHeader(0, 7000, 1))
	tail := putIndexHeader(t, idx, scheduler, indexTestHeader(8192, 100, 2))
	old, _ := idx.inline.Front()
	idx.remove(old, 0) // Rotation retired this header, but its slot still contains it.
	first, second := idx.reserve(0, 0), idx.reserve(4096, 0)
	idx.remove(first, 0) // The first writer after wrapping never publishes.
	h := publishIndexHeader(t, idx, scheduler, second, indexTestHeader(4096, 100, 3))
	reopened := reopenTestIndex(t, idx)
	require.Equal(t, []RecordHeader{tail, h}, indexReservations(reopened))
	require.Greater(t, reopened.nextObjectID, h.ObjectID)
}

func TestIndexOpenBatchesAndPartialTail(t *testing.T) {
	idx, _ := newTestDiskIndex(t)
	const count = 2051
	want := make(map[uint64]RecordHeader)
	data := make([]byte, count*RecordHeaderSize+RecordHeaderSize/2)
	for sequence := range count {
		if sequence%17 == 0 {
			continue
		}
		h := indexTestHeader(uint64(sequence)*4096, uint64(sequence+1), byte(sequence))
		h.ObjectID = idx.nextObjectID + uint64(sequence)
		want[h.ObjectID] = h
		h.Encode((*RawHeader)(data[sequence*RecordHeaderSize:]))
	}
	_, err := idx.file.WriteAt(data, 0)
	require.NoError(t, err)
	idx = reopenTestIndex(t, idx)
	require.Equal(t, want, indexHeaders(idx))
	require.EqualValues(t, count*RecordHeaderSize, idx.appendOffset)
}
