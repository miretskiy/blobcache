package blobstore

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func openTestStore(t *testing.T, path string, opts ...Option) *Store {
	t.Helper()
	memory, err := alloc.NewSlabAllocator(128 << 10)
	require.NoError(t, err)
	t.Cleanup(memory.Close)
	staging, err := alloc.NewExtentAllocator(16<<10, 2)
	require.NoError(t, err)
	t.Cleanup(staging.Close)
	opts = append([]Option{
		TestingWithDirectIO(false),
		WithInlineThreshold(32 << 10),
		WithStagingSize(8 << 10),
		WithDataIntegrity(DataIntegrityXXHash128),
	}, opts...)
	st, err := Open(path, memory, staging, opts...)
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
	require.Equal(t, []string{"data.db", "index.db", "objects"}, names)
	for _, f := range []*os.File{st.data} {
		info, err := f.Stat()
		require.NoError(t, err)
		require.Zero(t, info.Size())
	}
	require.EqualValues(t, 0, indexFileSize(t, st.mu.index))
	require.NotNil(t, st.scheduler)
	require.Empty(t, indexReservations(st.mu.index))
	require.Zero(t, st.mu.dataAppendOffset)

	require.NoError(t, st.Close())
	for _, allocator := range []alloc.Allocator{st.memory, st.staging} {
		b, err := allocator.TryAcquire(align.BlockSize)
		require.NoError(t, err, "Store borrows the allocators")
		b.Unpin()
	}
}

func TestStoreReopen(t *testing.T) {
	for name, direct := range map[string]bool{"buffered": false, "direct": true} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			st := openTestStore(t, dir, TestingWithDirectIO(direct), WithDurableIndex(direct))
			var headers []RecordHeader
			var objectPath string
			for _, size := range []int{5000, 40000} {
				key := []byte("key")
				value := bytes.Repeat([]byte("v"), size)
				w, err := st.NewWriter(key, int64(size))
				require.NoError(t, err)
				defer w.Discard()
				_, err = w.Write(value)
				require.NoError(t, err)
				if size == 40000 {
					objectPath = st.objectPath(w.header.ObjectID)
				}
				require.NoError(t, st.Put(w, nil))
				h := checkWriterRecord(t, st, w, key, value)
				w.Discard()
				headers = append(headers, h)
			}
			before, err := os.ReadFile(filepath.Join(dir, "data.db"))
			require.NoError(t, err)
			require.Len(t, before, 2*align.BlockSize)
			objectBefore, err := os.ReadFile(objectPath)
			require.NoError(t, err)
			require.NoError(t, st.Close())

			st = openTestStore(t, dir, TestingWithDirectIO(direct))
			require.Equal(t, headers, indexReservations(st.mu.index))
			require.Equal(t, uint64(2*align.BlockSize), st.mu.dataAppendOffset)
			require.Equal(t, uint64(45000), st.mu.index.size())
			w, err := st.NewWriter([]byte("next"), 100)
			require.NoError(t, err)
			defer w.Discard()
			require.Greater(t, w.header.ObjectID, headers[len(headers)-1].ObjectID)
			require.Equal(t, uint64(len(before)), w.header.Offset)
			_, err = w.Write([]byte("next value"))
			require.NoError(t, err)
			require.NoError(t, st.Put(w, nil))
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

func TestStoreReservationOrderAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	st := openTestStore(t, dir)
	var want []RecordHeader
	for round := range 3 {
		base := len(want)
		firstID := st.mu.index.nextObjectID
		keys := [][]byte{[]byte("first"), []byte("abandoned"), []byte("object"), []byte("last")}
		values := [][]byte{[]byte("first"), nil, bytes.Repeat([]byte("v"), 40000), []byte("last")}
		writers := make([]*Writer, len(keys))
		for i, key := range keys {
			w, err := st.NewWriter(key, int64(len(values[i])))
			require.NoError(t, err)
			t.Cleanup(w.Discard)
			require.Equal(t, firstID+uint64(i), w.header.ObjectID)
			writers[i] = w
		}
		writers[1].Discard() // A reservation hole survives when later slots are written.
		want = append(want, make([]RecordHeader, len(keys))...)
		for _, i := range []int{3, 2, 0} { // Complete in the opposite order to reservation.
			w := writers[i]
			_, err := w.Write(values[i])
			require.NoError(t, err)
			require.NoError(t, st.Put(w, nil))
			want[base+i] = w.header
			want[base+i].Hash = w.key
			w.Discard()
		}
		st.Wait()
		require.Equal(t, expectedPublishedHeaders(want), indexReservations(st.mu.index), "round %d: Put completion does not reorder entries", round)
		require.NoError(t, st.Close()) // Drain asynchronous index writes before reopening.
		st = openTestStore(t, dir)
		st.Wait()
		require.Equal(t, expectedPublishedHeaders(want), indexReservations(st.mu.index), "round %d: restart preserves reservation order", round)
		var live []RecordHeader
		live = liveStoreHeaders(st)
		var wantLive []RecordHeader
		for _, h := range want {
			if h != (RecordHeader{}) {
				wantLive = append(wantLive, h)
			}
		}
		require.Equal(t, wantLive, live, "iteration skips holes without changing FIFO order")
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
			w, err := st.NewWriter([]byte("key"), 100)
			if err != nil {
				return err
			}
			writers[i] = w
			st.mu.Lock()
			sequences[i] = w.header.ObjectID
			st.mu.Unlock()
			t.Cleanup(w.Discard)
			if _, err := w.Write([]byte("value")); err != nil {
				return err
			}
			return st.Put(w, nil)
		})
	}
	require.NoError(t, group.Wait())
	seen := make(map[uint64]bool, count)
	for i, w := range writers {
		sequence := sequences[i]
		require.False(t, seen[sequence], "index slots must be unique")
		seen[sequence] = true
		require.Equal(t, (sequence-firstID)*align.BlockSize, w.header.Offset)
		checkWriterRecord(t, st, w, []byte("key"), []byte("value"))
		w.Discard()
	}
	require.Len(t, indexReservations(st.mu.index), count)
	require.Equal(t, uint64(count*align.BlockSize), st.mu.dataAppendOffset)
	path := st.mu.index.file.Name()
	records := indexReservations(st.mu.index)
	require.NoError(t, st.Close())
	index, err := openDiskIndex(path, true)
	require.NoError(t, err)
	defer index.close()
	require.Equal(t, records, indexReservations(index))
	require.Equal(t, uint64(count*len("value")), index.size())
}

func TestPutIOError(t *testing.T) {
	for _, file := range []string{"data", "index"} {
		t.Run(file, func(t *testing.T) {
			var logs bytes.Buffer
			defer slog.SetDefault(slog.Default())
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			st := openTestStore(t, t.TempDir())
			key := []byte("key")
			w, err := st.NewWriter(key, SizeUnknown)
			require.NoError(t, err)
			defer w.Discard()
			_, err = w.Write([]byte("value"))
			require.NoError(t, err)
			objectPath := st.objectPath(w.header.ObjectID)
			if file == "data" {
				// Leave the slot reserved but remove its descriptor to fail data I/O.
				ticket, err := st.scheduler.Submit(iosched.VCloseOp(uint32(w.vfd)))
				require.NoError(t, err)
				_, err = ticket.Wait()
				require.NoError(t, err)
			} else {
				readOnly, err := os.Open(st.mu.index.file.Name())
				require.NoError(t, err)
				require.NoError(t, st.mu.index.file.Close())
				st.mu.index.file = readOnly
			}
			published := false
			require.NoError(t, st.Put(w, func(RecordHeader, *alloc.Buffer) { published = true }))
			w.Discard()
			st.Wait()
			require.False(t, published, "failed I/O never publishes cache metadata")
			require.Contains(t, logs.String(), "put record")
			require.Empty(t, indexReservations(st.mu.index))
			require.Zero(t, st.Size())
			require.NoFileExists(t, objectPath)
			require.Equal(t, [16]uint64{}, st.mu.vfiles)

		})
	}
}

func expectedPublishedHeaders(headers []RecordHeader) []RecordHeader {
	var published []RecordHeader
	for _, h := range headers {
		if h != (RecordHeader{}) {
			published = append(published, h)
		}
	}
	return published
}
