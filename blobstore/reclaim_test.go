package blobstore

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func putReclaimRecord(t *testing.T, st *Store, key string, value []byte) RecordHeader {
	t.Helper()
	return putReclaimSizedRecord(t, st, key, value, int64(len(value)))
}

func putReclaimSizedRecord(t *testing.T, st *Store, key string, value []byte, size int64) RecordHeader {
	t.Helper()
	w, err := st.NewWriter([]byte(key), size)
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write(value)
	require.NoError(t, err)
	require.NoError(t, st.Put(w))
	return checkWriterRecord(t, st, w, []byte(key), value)
}

func liveStoreHeaders(st *Store) []RecordHeader {
	var headers []RecordHeader
	for h := range st.mu.index.headers() {
		if h.KeyLen != 0 && !h.Flags.IsDeleted() {
			headers = append(headers, h)
		}
	}
	return headers
}

func recoveredStoreHeaders(t *testing.T, st *Store) []RecordHeader {
	t.Helper()
	var headers []RecordHeader
	st.Recover(func(h RecordHeader) {
		headers = append(headers, h)
	})
	return headers
}

func readReclaimRecord(t *testing.T, st *Store, key string, h RecordHeader, value []byte) {
	t.Helper()
	r, err := st.NewReader([]byte(key), h)
	require.NoError(t, err)
	defer r.Close()
	actual, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, actual)
}

func TestEvictionUsesFIFOAcrossStorageKinds(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithTargetSize(64<<10))
	a := putReclaimRecord(t, st, "a", []byte("inline"))
	object := putReclaimRecord(t, st, "recent", bytes.Repeat([]byte("e"), 40000))
	dataOffset, indexTail := st.mu.dataAppendOffset, st.mu.index.tail
	indexSize := indexFileSize(t, st.mu.index)
	var retired []uint64
	st.OnRetire = func(_ KeyHash, id uint64) {
		require.True(t, st.mu.TryLock(), "callbacks run outside the store mutex")
		st.mu.Unlock()
		retired = append(retired, id)
	}
	require.Equal(t, a.PhysicalLen, st.Reclaim(1), "the older inline record goes before the newer external record")
	require.Equal(t, []uint64{a.ObjectID}, retired)
	require.Equal(t, dataOffset, st.mu.dataAppendOffset)
	require.Equal(t, indexTail, st.mu.index.tail)
	readReclaimRecord(t, st, "recent", object, bytes.Repeat([]byte("e"), 40000))
	next := putReclaimRecord(t, st, "next", []byte("value"))
	require.Equal(t, dataOffset, next.Offset, "eviction does not reset data placement")
	require.Equal(t, indexSize, indexFileSize(t, st.mu.index), "the freed index slot is reused")
	require.Equal(t, object.PhysicalLen, st.Reclaim(1))
	path := filepath.Join(st.base, "objects", strconv.FormatUint(object.ObjectID, 10))
	require.NoError(t, st.Close()) // Drain accepted unlinks before checking the filesystem.
	require.NoFileExists(t, path)
}

func TestInlineRotationRetiresOverlapWithoutBackgroundEviction(t *testing.T) {
	const page = align.BlockSize
	dir := t.TempDir()
	st := openTestStore(t, dir, WithTargetSize(16*page), WithInlineThreshold(16*page))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-RecordHeaderSize-len(key)))
	}
	a, b, c, d := put("a", 4), put("b", 4), put("c", 4), put("d", 4)
	var retired []uint64
	st.OnRetire = func(_ KeyHash, id uint64) { retired = append(retired, id) }
	e := put("e", 6)
	require.Zero(t, e.Offset)
	require.Equal(t, []uint64{a.ObjectID, b.ObjectID}, retired)
	require.Equal(t, []RecordHeader{c, d, e}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "c", c, bytes.Repeat([]byte("c"), 4*page-RecordHeaderSize-1))
	f := put("f", 10)
	require.EqualValues(t, 6*page, f.Offset)
	require.Equal(t, []RecordHeader{e, f}, liveStoreHeaders(st))
	g := put("g", 4)
	require.Zero(t, g.Offset)
	require.Equal(t, []RecordHeader{f, g}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "f", f, bytes.Repeat([]byte("f"), 10*page-RecordHeaderSize-1))
	info, err := st.data.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 16*page, info.Size())
	require.LessOrEqual(t, indexFileSize(t, st.mu.index), int64(4*RecordHeaderSize))
	require.NoError(t, st.Close())
	st = openTestStore(t, dir, WithTargetSize(16*page), WithInlineThreshold(16*page))
	require.Equal(t, []RecordHeader{f, g}, recoveredStoreHeaders(t, st))
	readReclaimRecord(t, st, "f", f, bytes.Repeat([]byte("f"), 10*page-RecordHeaderSize-1))
	readReclaimRecord(t, st, "g", g, bytes.Repeat([]byte("g"), 4*page-RecordHeaderSize-1))
}

func TestInlineEvictionClearsIndexWithoutChangingDataOrTail(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	for _, key := range []string{"a", "b", "c", "d"} {
		putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), 100))
	}
	beforeData, err := os.ReadFile(st.data.Name())
	require.NoError(t, err)
	beforeIndex, err := os.ReadFile(st.mu.index.file.Name())
	require.NoError(t, err)
	dataOffset, indexTail := st.mu.dataAppendOffset, st.mu.index.tail
	require.EqualValues(t, 200, st.Reclaim(150))
	require.EqualValues(t, 200, st.Size())
	require.Equal(t, dataOffset, st.mu.dataAppendOffset)
	require.Equal(t, indexTail, st.mu.index.tail)
	st.Wait()
	afterData, err := os.ReadFile(st.data.Name())
	require.NoError(t, err)
	afterIndex, err := os.ReadFile(st.mu.index.file.Name())
	require.NoError(t, err)
	require.Equal(t, beforeData, afterData)
	require.Equal(t, beforeIndex[2*RecordHeaderSize:], afterIndex[2*RecordHeaderSize:])
	for slot := range 2 {
		var h RecordHeader
		require.NoError(t, (*RawHeader)(afterIndex[slot*RecordHeaderSize:]).Decode(&h, true))
		require.Equal(t, RecordHeader{Flags: FlagDeleted}, h)
	}
}

func TestRotationEvictsExternalRecordsInCoveredPrefix(t *testing.T) {
	const page = align.BlockSize
	st := openTestStore(t, t.TempDir(), WithTargetSize(16*page), WithInlineThreshold(16*page))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-RecordHeaderSize-1))
	}
	put("a", 12)
	put("b", 4)
	putReclaimSizedRecord(t, st, "object", []byte("o"), SizeUnknown)
	put("c", 10)      // Wrap, leaving b in the old lap's tail.
	d := put("d", 10) // Wrap again; retire that tail before overwriting c.
	require.Zero(t, d.Offset)
	require.Equal(t, []RecordHeader{d}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "d", d, bytes.Repeat([]byte("d"), 10*page-RecordHeaderSize-1))
}

func TestInlineSweepRetiresInterleavedExternalRecords(t *testing.T) {
	const page = align.BlockSize
	dir := t.TempDir()
	st := openTestStore(t, dir, WithTargetSize(16*page))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-RecordHeaderSize-len(key)))
	}
	a := put("a", 4)
	x := putReclaimSizedRecord(t, st, "x", []byte("x"), SizeUnknown)
	b := put("b", 4)
	y := putReclaimSizedRecord(t, st, "y", []byte("y"), SizeUnknown)
	c := put("c", 8)
	var retired []uint64
	st.OnRetire = func(_ KeyHash, id uint64) { retired = append(retired, id) }
	next := put("n", 6) // Covers a and part of b, retiring x in the same sweep.
	require.Equal(t, []uint64{a.ObjectID, x.ObjectID, b.ObjectID}, retired)
	require.Equal(t, []RecordHeader{y, c, next}, liveStoreHeaders(st))
	xPath, yPath := st.objectPath(x.ObjectID), st.objectPath(y.ObjectID)
	require.NoError(t, st.Close())
	require.NoFileExists(t, xPath)
	require.FileExists(t, yPath)
	st = openTestStore(t, dir, WithTargetSize(16*page))
	require.Equal(t, []RecordHeader{y, c, next}, recoveredStoreHeaders(t, st))
	readReclaimRecord(t, st, "y", y, []byte("y"))
	readReclaimRecord(t, st, "c", c, bytes.Repeat([]byte("c"), 8*page-RecordHeaderSize-1))
	readReclaimRecord(t, st, "n", next, bytes.Repeat([]byte("n"), 6*page-RecordHeaderSize-1))
}

func TestInlineAppendKeepsExternalOnlyIndex(t *testing.T) {
	dir := t.TempDir()
	st := openTestStore(t, dir)
	value := bytes.Repeat([]byte("e"), 40000)
	external := putReclaimRecord(t, st, "external", value)
	require.NoError(t, st.Close())
	st = openTestStore(t, dir)
	inline := putReclaimRecord(t, st, "inline", []byte("new"))
	require.Equal(t, external.Offset, inline.Offset, "external reservations do not advance data placement")
	require.Equal(t, []RecordHeader{external, inline}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "external", external, value)
}

func TestInlineSweepPastExternalFromOlderLap(t *testing.T) {
	const page = align.BlockSize
	st := openTestStore(t, t.TempDir(), WithTargetSize(16*page))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-RecordHeaderSize-len(key)))
	}
	put("a", 5)
	put("b", 7)
	x := putReclaimSizedRecord(t, st, "x", []byte("x"), SizeUnknown) // Old lap, offset 12 pages.
	w, err := st.NewWriter([]byte("unused"), 5*page-RecordHeaderSize-6)
	require.NoError(t, err)
	require.NoError(t, w.Close()) // Wraps and leaves a five-page gap.
	put("c", 8)                   // Reuses b's space and crosses x's old cursor.
	d := put("d", 3)
	e := put("e", 8) // Wraps again and must retire c even if x is still ahead of it.
	require.Equal(t, []RecordHeader{d, e}, liveStoreHeaders(st))
	st.Wait()
	require.NoFileExists(t, st.objectPath(x.ObjectID))
	readReclaimRecord(t, st, "d", d, bytes.Repeat([]byte("d"), 3*page-RecordHeaderSize-1))
	readReclaimRecord(t, st, "e", e, bytes.Repeat([]byte("e"), 8*page-RecordHeaderSize-1))
}

func TestStoreFailedUnlinkIsLogged(t *testing.T) {
	var logs bytes.Buffer
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	h := putReclaimRecord(t, st, "object", []byte("payload"))
	path := filepath.Join(st.base, "objects", strconv.FormatUint(h.ObjectID, 10))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o755))
	require.Equal(t, h.PhysicalLen, st.Reclaim(1))
	require.Zero(t, st.Size())
	require.NoError(t, st.Close()) // Drain the asynchronous error callback before inspecting logs.
	require.Contains(t, logs.String(), "unlink object")
	require.Contains(t, logs.String(), strconv.FormatUint(h.ObjectID, 10))
}

func TestInlineThresholdIndependentOfTarget(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithTargetSize(64<<10), WithInlineThreshold(128<<10))
	h := putReclaimRecord(t, st, "large", bytes.Repeat([]byte("v"), 64<<10))
	require.False(t, h.Flags.IsExternal())
	require.Greater(t, st.mu.dataAppendOffset, st.TargetSize())
	readReclaimRecord(t, st, "large", h, bytes.Repeat([]byte("v"), 64<<10))
}

func TestWriteStatsPreserveIngressAcrossReclamation(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	pending, err := st.NewWriter([]byte("pending"), 100)
	require.NoError(t, err)
	live, published := st.WriteStats()
	require.Zero(t, live)
	require.Zero(t, published)
	pending.Discard()
	putReclaimRecord(t, st, "inline", bytes.Repeat([]byte("i"), 100))
	putReclaimRecord(t, st, "object", bytes.Repeat([]byte("o"), 40000))
	require.EqualValues(t, 100, st.Reclaim(1))
	live, published = st.WriteStats()
	require.EqualValues(t, 40000, live)
	require.EqualValues(t, 40100, published)
}
