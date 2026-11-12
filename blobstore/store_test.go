package blobstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"

	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newStoreMemory(t testing.TB, size int64) *alloc.Allocator {
	t.Helper()
	a := alloc.NewAllocator(size)
	t.Cleanup(a.Close)
	return a
}

func openTestStore(t *testing.T, path string, opts ...Option) *Store {
	t.Helper()
	memory := newStoreMemory(t, 4<<20)
	opts = append([]Option{
		TestingWithDirectIO(false),
		WithInlineThreshold(32 << 10),
		WithDataIntegrity(DataIntegrityCRC32C),
	}, opts...)
	st, err := Open(path, memory, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func TestOpenCreatesFiles(t *testing.T) {
	st := openTestStore(t, filepath.Join(t.TempDir(), "cache"))
	entries, err := os.ReadDir(st.base)
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	require.Equal(t, []string{"data.db", "objects"}, names, "index.db is written by Close")
	for _, f := range []*os.File{st.data} {
		info, err := f.Stat()
		require.NoError(t, err)
		require.Zero(t, info.Size())
	}
	require.NotNil(t, st.schedulers[0])
	require.Empty(t, indexReservations(st.mu.index))
	require.Zero(t, st.mu.dataAppendOffset)

	require.NoError(t, st.Close())
	info, err := os.Stat(filepath.Join(st.base, "index.db"))
	require.NoError(t, err)
	require.Zero(t, info.Size())
	for _, allocator := range []*alloc.Allocator{st.memory} {
		b, err := allocator.TryAcquire(align.BlockSize)
		require.NoError(t, err, "Store borrows the allocators")
		b.Unpin()
	}
}

func TestStoreReopen(t *testing.T) {
	for name, direct := range map[string]bool{"buffered": false, "direct": true} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			st := openTestStore(t, dir, TestingWithDirectIO(direct))
			var headers []RecordHeader
			var objectPath string
			for _, size := range []int{5000, 40000} {
				key := []byte("key")
				value := bytes.Repeat([]byte("v"), size)
				w, err := st.NewWriter()
				require.NoError(t, err)
				defer w.Discard()
				_, err = w.Write(value)
				require.NoError(t, err)

				err = putStoreWriter(st, key, w)
				require.NoError(t, err)
				if size == 40000 {
					objectPath = st.objectPath(w.header.ObjectID)
				}

				h := checkWriterRecord(t, st, w, key, value)
				w.Discard()
				headers = append(headers, h)
			}
			before, err := os.ReadFile(filepath.Join(dir, "data.db"))
			require.NoError(t, err)
			require.Len(t, before, 3*align.BlockSize)
			objectBefore, err := os.ReadFile(objectPath)
			require.NoError(t, err)
			require.NoError(t, st.Close())

			st = openTestStore(t, dir, TestingWithDirectIO(direct))
			require.Equal(t, headers, indexReservations(st.mu.index))
			require.Equal(t, uint64(3*align.BlockSize), st.mu.dataAppendOffset)
			require.Equal(t, uint64(45000), st.mu.index.size())
			w, err := st.NewWriter()
			require.NoError(t, err)
			defer w.Discard()
			_, err = w.Write([]byte("next value"))
			require.NoError(t, err)

			err = putStoreWriter(st, []byte("next"), w)
			require.NoError(t, err)

			require.Greater(t, w.header.ObjectID, headers[len(headers)-1].ObjectID)
			require.Equal(t, uint64(len(before)), w.header.Offset)
			checkWriterRecord(t, st, w, []byte("next"), []byte("next value"))
			after, err := os.ReadFile(filepath.Join(dir, "data.db"))
			require.NoError(t, err)
			require.Equal(t, before, after[:len(before)])
			objectAfter, err := os.ReadFile(objectPath)
			require.NoError(t, err)
			require.Equal(t, objectBefore, objectAfter)
		})
	}
}

func TestStoreConcurrentWrites(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	const count = 16
	writers := make([]*Writer, count)
	sequences := make([]uint64, count)
	firstID := st.mu.index.nextObjectID
	var group errgroup.Group
	for i := range count {
		group.Go(func() error {
			w, err := st.NewWriter()
			if err != nil {
				return err
			}
			writers[i] = w
			t.Cleanup(func() { w.Discard() })
			if _, err := w.Write([]byte("value")); err != nil {
				return err
			}
			err = putStoreWriter(st, []byte("key"), w)
			if err != nil {
				return err
			}
			sequences[i] = w.header.ObjectID
			return err
		})
	}
	require.NoError(t, group.Wait())
	seen := make(map[uint64]bool, count)
	for i, w := range writers {
		sequence := sequences[i]
		require.False(t, seen[sequence], "ObjectIDs must be unique")
		seen[sequence] = true
		require.Equal(t, (sequence-firstID)*2*align.BlockSize, w.header.Offset)
		checkWriterRecord(t, st, w, []byte("key"), []byte("value"))
		w.Discard()
	}
	require.Len(t, indexReservations(st.mu.index), count)
	require.Equal(t, uint64(count*2*align.BlockSize), st.mu.dataAppendOffset)
	records := indexReservations(st.mu.index)
	require.NoError(t, st.Close())
	index, err := loadDiskIndex(filepath.Join(st.base, "index.db"), true)
	require.NoError(t, err)
	require.Equal(t, records, indexReservations(index))
	require.Equal(t, uint64(count*len("value")), index.size())
}

func TestPutIOError(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	key := []byte("key")
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)

	// Fail asynchronous open after the complete record has been submitted.
	require.NoError(t, os.Remove(filepath.Join(st.base, "objects")))
	err = putStoreWriter(st, key, w)
	objectPath := st.objectPath(w.header.ObjectID)
	require.Error(t, err, "Put waits for the completion error")
	w.Discard()
	require.Empty(t, indexReservations(st.mu.index))
	require.Zero(t, st.Size())
	_, publishedBytes := st.WriteStats()
	require.Zero(t, publishedBytes, "failed writes must not count as published payload")
	require.Eventually(t, func() bool { return !fileExists(objectPath) }, time.Second, time.Millisecond)
}

type rejectNextScheduler struct {
	iosched.Scheduler
	reject error
}

func (s *rejectNextScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	if err := s.reject; err != nil {
		s.reject = nil
		return iosched.Ticket{}, err
	}
	return s.Scheduler.Submit(op)
}

func rejectTestIO(st *Store) *rejectNextScheduler {
	s := &rejectNextScheduler{Scheduler: st.schedulers[0]}
	st.schedulers[0] = s
	return s
}

func TestPendingWriterDefersInlineRotation(t *testing.T) {
	const page = align.BlockSize
	st := openTestStore(t, t.TempDir(), WithTargetSize(16*page))
	var first RecordHeader
	pending, err := st.reserve(&first, 4*page)
	require.NoError(t, err)
	for _, key := range []string{"b", "c", "d"} {
		putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), 4*page-page))
	}
	require.Zero(t, reclaim(st, 1))
	var next RecordHeader
	_, err = st.reserve(&next, page)
	require.ErrorIs(t, err, ErrBusy)
	st.mu.Lock()
	st.mu.index.remove(pending)
	st.mu.Unlock()
	h := putReclaimRecord(t, st, "reuse", []byte("x"))
	require.Zero(t, h.Offset)
}

func TestCloseReleasesRejectedPutReservation(t *testing.T) {
	for kind, threshold := range map[string]int64{"inline": 32 << 10, "external": 0} {
		t.Run(kind, func(t *testing.T) {
			st := openTestStore(t, t.TempDir(), WithInlineThreshold(threshold))
			scheduler := rejectTestIO(st)
			w, err := st.NewWriter()
			require.NoError(t, err)
			_, err = w.Write([]byte("value"))
			require.NoError(t, err)

			failure := errors.New("submission rejected")
			scheduler.reject = failure
			err = putStoreWriter(st, []byte("key"), w)
			require.ErrorIs(t, err, failure)
			path := st.objectPath(w.header.ObjectID)
			w.Discard()
			require.NoFileExists(t, path)
			require.Zero(t, st.Size())
			_, publishedBytes := st.WriteStats()
			require.Zero(t, publishedBytes)
			require.Empty(t, indexReservations(st.mu.index))
			h := putReclaimRecord(t, st, "next", []byte("next"))
			readReclaimRecord(t, st, "next", h, []byte("next"))
		})
	}
}

func TestOpenRemovesUnindexedObjects(t *testing.T) {
	dir := t.TempDir()
	st := openTestStore(t, dir)
	h := putReclaimObject(t, st, "kept", []byte("value"))
	require.NoError(t, st.Close())
	orphan := filepath.Join(dir, "objects", "12345")
	require.NoError(t, os.WriteFile(orphan, []byte("lost by a crash"), 0o644))
	st = openTestStore(t, dir)
	require.NoFileExists(t, orphan)
	readReclaimRecord(t, st, "kept", h, []byte("value"))
}

func putStoreWriter(st *Store, key []byte, w *Writer) error {
	if err := w.Seal(key); err != nil {
		return err
	}
	ticket, err := st.Put(w, nil)
	if err != nil {
		return err
	}
	return ticket.Wait()
}
