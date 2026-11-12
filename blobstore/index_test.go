package blobstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexRejectsOldRecordFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	old := make([]byte, 64)
	copy(old, []byte{0xc5, 0xca, 0x0b, 0xb1})
	require.NoError(t, os.WriteFile(path, old, 0o600))
	for _, verify := range []bool{false, true} {
		idx, err := loadDiskIndex(path, verify)
		require.Nil(t, idx)
		require.ErrorIs(t, err, ErrBadMagic)
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, old, data, "opening an incompatible cache must not change its files")
}

func indexReservations(idx *diskIndex) []RecordHeader {
	var headers []RecordHeader
	for h := range idx.headers() {
		headers = append(headers, h)
	}
	return headers
}

// indexHeaders returns published records; pending reservations have no key yet.
func indexHeaders(idx *diskIndex) map[uint64]RecordHeader {
	headers := make(map[uint64]RecordHeader)
	for h := range idx.headers() {
		if h.KeyLen != 0 {
			headers[h.ObjectID] = h
		}
	}
	return headers
}

func indexTestHeader(offset, size uint64, tag byte) RecordHeader {
	return RecordHeader{
		Flags: FlagHeaderIntegrity, Offset: offset, PhysicalLen: size, LogicalLen: size,
		KeyLen: 1, Hash: uint64(tag),
	}
}

func writeIndexFile(t *testing.T, path string, headers ...RecordHeader) {
	t.Helper()
	data := make([]byte, len(headers)*RecordHeaderSize)
	for i, h := range headers {
		h.Encode((*RawHeader)(data[i*RecordHeaderSize:]))
	}
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

func TestIndexSnapshotSkipsPendingWriters(t *testing.T) {
	before := uint64(time.Now().UnixNano())
	path := filepath.Join(t.TempDir(), "index.db")
	idx, err := loadDiskIndex(path, true)
	require.NoError(t, err)
	inline := indexTestHeader(0, 12345, 1)
	external := indexTestHeader(inline.dataEnd(), 1<<33, 2)
	external.Flags |= FlagExternal
	first, second := idx.reserve(0, 0), idx.reserve(external.Offset, FlagExternal)
	idx.reserve(1<<20, 0) // A writer still open at Close.
	inline.ObjectID = idx.records.Value(first).header.ObjectID
	external.ObjectID = idx.records.Value(second).header.ObjectID
	require.GreaterOrEqual(t, inline.ObjectID, before)
	require.Equal(t, inline.ObjectID+1, external.ObjectID)
	require.Zero(t, idx.size(), "reservations are not published")
	idx.publish(second, external)
	idx.publish(first, inline)
	idx.records.Value(first).pending = false
	idx.records.Value(second).pending = false
	require.NoError(t, idx.save(path))

	loaded, err := loadDiskIndex(path, true)
	require.NoError(t, err)
	require.Equal(t, []RecordHeader{inline, external}, indexReservations(loaded))
	require.Equal(t, inline.PhysicalLen+external.PhysicalLen, loaded.size())
	require.Greater(t, loaded.nextObjectID, external.ObjectID)
	_, err = os.Stat(path + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestIndexLoadReplaysInlinePlacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	headers := []RecordHeader{
		indexTestHeader(0, 7000, 1),
		indexTestHeader(12288, 100, 2),
		indexTestHeader(20480, 100, 3),
		indexTestHeader(4096, 100, 5), // Wrapped; ObjectID 4 at offset 0 never published.
	}
	for i, id := range []uint64{1, 2, 3, 5} {
		headers[i].ObjectID = id
	}
	writeIndexFile(t, path, headers[3], headers[0], headers[2], headers[1])
	idx, err := loadDiskIndex(path, true)
	require.NoError(t, err)
	require.Equal(t, headers[1:], indexReservations(idx), "the wrapped record retires the older overlap")
	require.EqualValues(t, 5, idx.dataWrapID)
	require.Greater(t, idx.nextObjectID, uint64(5))
}

func TestIndexLoadsSlotFormatInBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	const count = 2051
	base := uint64(time.Now().UnixNano())
	var want []RecordHeader
	data := make([]byte, count*RecordHeaderSize+RecordHeaderSize/2)
	for sequence := range count {
		h := indexTestHeader(uint64(sequence)*8192, uint64(sequence+1), byte(sequence))
		h.ObjectID = base + uint64(sequence)
		switch {
		case sequence%17 == 0:
			continue // An empty slot.
		case sequence%13 == 0:
			h = RecordHeader{Flags: FlagDeleted, ObjectID: h.ObjectID}
		default:
			want = append(want, h)
		}
		h.Encode((*RawHeader)(data[sequence*RecordHeaderSize:]))
	}
	require.NoError(t, os.WriteFile(path, data, 0o644))
	idx, err := loadDiskIndex(path, true)
	require.NoError(t, err)
	require.Equal(t, want, indexReservations(idx))
}
