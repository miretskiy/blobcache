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
	w, err := st.NewWriter([]byte(key), int64(len(value)))
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write(value)
	require.NoError(t, err)
	require.NoError(t, st.Put(w, nil))
	drainTestIndex(t, st) // These policy tests reclaim completed records.
	return checkWriterRecord(t, st, w, []byte(key), value)
}

func drainTestIndex(t *testing.T, st *Store) {
	t.Helper()
	st.Wait()
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

func TestEvictionPrefersExternalWithoutRotatingData(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithTargetSize(64<<10))
	a := putReclaimRecord(t, st, "a", []byte("inline"))
	object := putReclaimRecord(t, st, "recent", bytes.Repeat([]byte("e"), 40000))
	dataOffset, indexOffset := st.mu.dataAppendOffset, st.mu.index.appendOffset
	var retired []uint64
	st.OnRetire = func(_ KeyHash, id uint64) {
		require.True(t, st.mu.TryLock(), "callbacks run outside the store mutex")
		st.mu.Unlock()
		retired = append(retired, id)
	}
	require.Equal(t, object.PhysicalLen, st.Reclaim(1), "the newer external record goes before the older inline record")
	require.Equal(t, []uint64{object.ObjectID}, retired)
	require.Equal(t, dataOffset, st.mu.dataAppendOffset)
	require.Equal(t, indexOffset, st.mu.index.appendOffset)
	readReclaimRecord(t, st, "a", a, []byte("inline"))
	next := putReclaimRecord(t, st, "next", []byte("value"))
	require.Equal(t, dataOffset, next.Offset, "eviction does not reset data placement")
	require.Equal(t, indexOffset, indexFileSize(t, st.mu.index), "the freed index slot is reused")
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

func TestInlineEvictionDoesNotWriteFilesOrMoveAppendOffsets(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	for _, key := range []string{"a", "b", "c", "d"} {
		putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), 100))
	}
	beforeData, err := os.ReadFile(st.data.Name())
	require.NoError(t, err)
	beforeIndex, err := os.ReadFile(st.mu.index.file.Name())
	require.NoError(t, err)
	dataOffset, indexOffset := st.mu.dataAppendOffset, st.mu.index.appendOffset
	require.EqualValues(t, 200, st.Reclaim(150))
	require.EqualValues(t, 200, st.Size())
	require.Equal(t, dataOffset, st.mu.dataAppendOffset)
	require.Equal(t, indexOffset, st.mu.index.appendOffset)
	afterData, err := os.ReadFile(st.data.Name())
	require.NoError(t, err)
	afterIndex, err := os.ReadFile(st.mu.index.file.Name())
	require.NoError(t, err)
	require.Equal(t, beforeData, afterData)
	require.Equal(t, beforeIndex, afterIndex)
}

func TestRotationSkipsUnusedTailAndPreservesExternalRecords(t *testing.T) {
	const page = align.BlockSize
	st := openTestStore(t, t.TempDir(), WithTargetSize(16*page), WithInlineThreshold(16*page))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-RecordHeaderSize-1))
	}
	put("a", 12)
	put("b", 4)
	object := putReclaimRecord(t, st, "object", bytes.Repeat([]byte("o"), 17*page))
	put("c", 10)      // Wrap, leaving b in the old lap's tail.
	d := put("d", 10) // Wrap again; retire that tail before overwriting c.
	require.Zero(t, d.Offset)
	require.Equal(t, []RecordHeader{object, d}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "d", d, bytes.Repeat([]byte("d"), 10*page-RecordHeaderSize-1))
	readReclaimRecord(t, st, "object", object, bytes.Repeat([]byte("o"), 17*page))
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
	require.EqualValues(t, 40000, st.Reclaim(1))
	live, published = st.WriteStats()
	require.EqualValues(t, 100, live)
	require.EqualValues(t, 40100, published)
}
