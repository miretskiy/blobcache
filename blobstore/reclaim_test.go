package blobstore

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

// reclaim evicts an oldest prefix holding at least bytes of payload, stopping
// at a pending writer, and returns the payload evicted.
func reclaim(st *Store, bytes uint64) uint64 {
	st.mu.Lock()
	retired := st.evictLocked(bytes, 0)
	st.mu.Unlock()
	st.notifyRetired(retired)
	var evicted uint64
	for _, h := range retired {
		evicted += h.PhysicalLen
	}
	return evicted
}

func putReclaimObject(t *testing.T, st *Store, key string, value []byte) RecordHeader {
	t.Helper()
	threshold := st.cfg.inlineThreshold
	st.cfg.inlineThreshold = 0
	defer func() { st.cfg.inlineThreshold = threshold }()
	return putReclaimRecord(t, st, key, value)
}

func putReclaimRecord(t *testing.T, st *Store, key string, value []byte) RecordHeader {
	t.Helper()
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write(value)
	require.NoError(t, err)
	err = putStoreWriter(st, []byte(key), w)
	require.NoError(t, err)

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
	for h := range st.Records() {
		headers = append(headers, h)
	}
	return headers
}

func retireInto(retired *[]uint64) Option {
	return WithRetireFunc(func(h RecordHeader) { *retired = append(*retired, h.ObjectID) })
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
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
	var st *Store
	var retired []uint64
	st = openTestStore(t, t.TempDir(), WithTargetSize(64<<10), WithRetireFunc(func(h RecordHeader) {
		require.True(t, st.mu.TryLock(), "callbacks run outside the store mutex")
		st.mu.Unlock()
		retired = append(retired, h.ObjectID)
	}))
	a := putReclaimRecord(t, st, "a", []byte("inline"))
	object := putReclaimRecord(t, st, "recent", bytes.Repeat([]byte("e"), 40000))
	dataOffset := st.mu.dataAppendOffset
	require.Equal(t, a.PhysicalLen, reclaim(st, 1), "the older inline record goes before the newer external record")
	require.Equal(t, []uint64{a.ObjectID}, retired)
	require.Equal(t, dataOffset, st.mu.dataAppendOffset)
	readReclaimRecord(t, st, "recent", object, bytes.Repeat([]byte("e"), 40000))
	next := putReclaimRecord(t, st, "next", []byte("value"))
	require.Equal(t, dataOffset, next.Offset, "eviction does not reset data placement")
	require.Equal(t, object.PhysicalLen, reclaim(st, 1))
	path := filepath.Join(st.base, "objects", strconv.FormatUint(object.ObjectID, 10))
	require.NoError(t, st.Close()) // Drain accepted unlinks before checking the filesystem.
	require.NoFileExists(t, path)
}

func TestInlineRotationRetiresOverlapWithoutBackgroundEviction(t *testing.T) {
	const page = align.BlockSize
	dir := t.TempDir()
	var retired []uint64
	st := openTestStore(t, dir, WithTargetSize(16*page), WithInlineThreshold(16*page), retireInto(&retired))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-page))
	}
	a, b, c, d := put("a", 4), put("b", 4), put("c", 4), put("d", 4)
	require.Empty(t, retired)
	e := put("e", 6)
	require.Zero(t, e.Offset)
	require.Equal(t, []uint64{a.ObjectID, b.ObjectID}, retired)
	require.Equal(t, []RecordHeader{c, d, e}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "c", c, bytes.Repeat([]byte("c"), 4*page-page))
	f := put("f", 10)
	require.EqualValues(t, 6*page, f.Offset)
	require.Equal(t, []RecordHeader{e, f}, liveStoreHeaders(st))
	g := put("g", 4)
	require.Zero(t, g.Offset)
	require.Equal(t, []RecordHeader{f, g}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "f", f, bytes.Repeat([]byte("f"), 10*page-page))
	info, err := st.data.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 16*page, info.Size())
	require.NoError(t, st.Close())
	st = openTestStore(t, dir, WithTargetSize(16*page), WithInlineThreshold(16*page))
	require.Equal(t, []RecordHeader{f, g}, recoveredStoreHeaders(t, st))
	readReclaimRecord(t, st, "f", f, bytes.Repeat([]byte("f"), 10*page-page))
	readReclaimRecord(t, st, "g", g, bytes.Repeat([]byte("g"), 4*page-page))
}

func TestInlineEvictionLeavesDataAndPlacement(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	var headers []RecordHeader
	for _, key := range []string{"a", "b", "c", "d"} {
		headers = append(headers, putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), 100)))
	}
	before, err := os.ReadFile(st.data.Name())
	require.NoError(t, err)
	dataOffset := st.mu.dataAppendOffset
	require.EqualValues(t, 200, reclaim(st, 150))
	require.EqualValues(t, 200, st.Size())
	require.Equal(t, dataOffset, st.mu.dataAppendOffset)
	require.Equal(t, headers[2:], liveStoreHeaders(st))
	after, err := os.ReadFile(st.data.Name())
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRotationEvictsExternalRecordsInCoveredPrefix(t *testing.T) {
	const page = align.BlockSize
	st := openTestStore(t, t.TempDir(), WithTargetSize(16*page), WithInlineThreshold(16*page))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-page))
	}
	put("a", 12)
	put("b", 4)
	putReclaimObject(t, st, "object", []byte("o"))
	put("c", 10)      // Wrap, leaving b in the old lap's tail.
	d := put("d", 10) // Wrap again; retire that tail before overwriting c.
	require.Zero(t, d.Offset)
	require.Equal(t, []RecordHeader{d}, liveStoreHeaders(st))
	readReclaimRecord(t, st, "d", d, bytes.Repeat([]byte("d"), 10*page-page))
}

func TestInlineSweepRetiresInterleavedExternalRecords(t *testing.T) {
	const page = align.BlockSize
	dir := t.TempDir()
	var retired []uint64
	st := openTestStore(t, dir, WithTargetSize(16*page), retireInto(&retired))
	put := func(key string, pages int) RecordHeader {
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-page))
	}
	a := put("a", 4)
	x := putReclaimObject(t, st, "x", []byte("x"))
	b := put("b", 4)
	y := putReclaimObject(t, st, "y", []byte("y"))
	c := put("c", 8)
	require.Empty(t, retired)
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
	readReclaimRecord(t, st, "c", c, bytes.Repeat([]byte("c"), 8*page-page))
	readReclaimRecord(t, st, "n", next, bytes.Repeat([]byte("n"), 6*page-page))
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
		return putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), pages*page-page))
	}
	put("a", 5)
	put("b", 7)
	x := putReclaimObject(t, st, "x", []byte("x")) // Old lap, offset 12 pages.
	var gap RecordHeader
	handle, err := st.reserve(&gap, 5*page)
	require.NoError(t, err)
	st.mu.Lock()
	st.mu.index.remove(handle)
	st.mu.Unlock()
	put("c", 8) // Reuses b's space and crosses x's old cursor.
	d := put("d", 3)
	e := put("e", 8) // Wraps again and must retire c even if x is still ahead of it.
	require.Equal(t, []RecordHeader{d, e}, liveStoreHeaders(st))
	require.Eventually(t, func() bool { return !fileExists(st.objectPath(x.ObjectID)) }, time.Second, time.Millisecond)
	readReclaimRecord(t, st, "d", d, bytes.Repeat([]byte("d"), 3*page-page))
	readReclaimRecord(t, st, "e", e, bytes.Repeat([]byte("e"), 8*page-page))
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
	require.Equal(t, h.PhysicalLen, reclaim(st, 1))
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
	pending, err := st.NewWriter()
	require.NoError(t, err)
	live, published := st.WriteStats()
	require.Zero(t, live)
	require.Zero(t, published)
	pending.Discard()
	putReclaimRecord(t, st, "inline", bytes.Repeat([]byte("i"), 100))
	putReclaimRecord(t, st, "object", bytes.Repeat([]byte("o"), 40000))
	require.EqualValues(t, 100, reclaim(st, 1))
	live, published = st.WriteStats()
	require.EqualValues(t, 40000, live)
	require.EqualValues(t, 40100, published)
}
