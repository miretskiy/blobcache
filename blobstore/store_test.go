package blobstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

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
				require.NoError(t, st.Put(w))
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
			require.NoError(t, st.Put(w))
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
			require.NoError(t, st.Put(w))
			want[base+i] = w.header
			want[base+i].Hash = w.key
			w.Discard()
		}
		require.Equal(t, expectedPublishedHeaders(want), indexReservations(st.mu.index), "round %d: Put completion does not reorder entries", round)
		require.NoError(t, st.Close())
		st = openTestStore(t, dir)
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
			return st.Put(w)
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
			err = st.Put(w)
			require.Error(t, err, "Put reports completion errors")
			require.ErrorIs(t, w.Close(), err)
			st.Wait()
			require.Empty(t, indexReservations(st.mu.index))
			require.Zero(t, st.Size())
			_, publishedBytes := st.WriteStats()
			require.Zero(t, publishedBytes, "failed writes must not count as published payload")
			require.Zero(t, indexFileSize(t, st.mu.index), "a failed data write must cancel the linked index write")
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

// Pause or reject the next submission. Holding requires synctest: Wait
// establishes that the caller is blocked before inspecting pending state.
type heldScheduler struct {
	iosched.Scheduler
	holdNext bool
	held     []chan struct{}
	reject   error
}

func holdTestIO(t *testing.T, st *Store) *heldScheduler {
	t.Helper()
	require.NoError(t, st.scheduler.Close())
	backend := iosched.NewPOSIXScheduler()
	s := &heldScheduler{Scheduler: backend}
	st.scheduler = s
	return s
}

func (s *heldScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	if !s.holdNext {
		return s.Scheduler.Submit(op)
	}
	s.holdNext = false
	if s.reject != nil {
		return iosched.Ticket{}, s.reject
	}
	resume := make(chan struct{})
	s.held = append(s.held, resume)
	<-resume
	if s.reject != nil {
		return iosched.Ticket{}, s.reject
	}
	return s.Scheduler.Submit(op)
}

func (s *heldScheduler) complete(t *testing.T, i int) {
	t.Helper()
	close(s.held[i])
	synctest.Wait()
}

func TestPendingIndexWritesPreventEvictionAndSlotReuse(t *testing.T) {
	synctest.Test(t, testPendingIndexWritesPreventEvictionAndSlotReuse)
}

func testPendingIndexWritesPreventEvictionAndSlotReuse(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	scheduler := holdTestIO(t, st)
	var done [2]bool
	for i, key := range []string{"first", "second"} {
		w, err := st.NewWriter([]byte(key), 5)
		require.NoError(t, err)
		_, err = w.Write([]byte("value"))
		require.NoError(t, err)
		scheduler.holdNext = true
		go func() {
			defer w.Discard()
			require.NoError(t, st.Put(w))
			done[i] = true
		}()
		synctest.Wait()
		require.False(t, done[i], "Put waits for its index write")
	}
	require.Zero(t, st.Reclaim(10), "unfinished index I/O defers eviction without waiting")
	scheduler.complete(t, 1)
	require.True(t, done[1], "each Put waits only for its own write")
	require.False(t, done[0])
	require.Zero(t, st.Reclaim(10), "completion order does not change inline eviction order")
	third, err := st.NewWriter([]byte("third"), 5)
	require.NoError(t, err)
	_, err = third.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, st.Put(third))
	require.NoError(t, third.Close())
	require.EqualValues(t, 3*RecordHeaderSize, indexFileSize(t, st.mu.index), "pending slots are not reused")
	scheduler.complete(t, 0)
	require.True(t, done[0])
	require.EqualValues(t, 10, st.Reclaim(10))
	putReclaimRecord(t, st, "reuse", []byte("fresh"))
	require.EqualValues(t, 3*RecordHeaderSize, indexFileSize(t, st.mu.index))
	readReclaimRecord(t, st, "third", third.header, []byte("value"))
}

func TestPendingWriterDefersInlineRotation(t *testing.T) {
	const page = align.BlockSize
	st := openTestStore(t, t.TempDir(), WithTargetSize(16*page))
	first, err := st.NewWriter([]byte("first"), 4*page-RecordHeaderSize-5)
	require.NoError(t, err)
	defer first.Close()
	for _, key := range []string{"b", "c", "d"} {
		putReclaimRecord(t, st, key, bytes.Repeat([]byte(key), 4*page-RecordHeaderSize-1))
	}
	require.Zero(t, st.Reclaim(1))
	w, err := st.NewWriter([]byte("reuse"), 1)
	require.Nil(t, w)
	require.ErrorIs(t, err, ErrBusy)
	require.EqualValues(t, 16*page, st.mu.dataAppendOffset, "a deferred rotation does not move the append position")
	require.NoError(t, first.Close())
	h := putReclaimRecord(t, st, "reuse", []byte("x"))
	require.Zero(t, h.Offset)
}

func TestPutProtectsRecordUntilWriterClose(t *testing.T) {
	for kind, size := range map[string]int64{"inline": 5, "external": SizeUnknown} {
		t.Run(kind, func(t *testing.T) {
			st := openTestStore(t, t.TempDir())
			w, err := st.NewWriter([]byte("key"), size)
			require.NoError(t, err)
			defer w.Close()
			_, err = w.Write([]byte("value"))
			require.NoError(t, err)
			require.NoError(t, st.Put(w))
			require.Zero(t, st.Reclaim(1), "the caller can publish its cache mapping before Close")
			readReclaimRecord(t, st, "key", w.Header(), []byte("value"))
			require.NoError(t, w.Close())
			require.EqualValues(t, 5, st.Reclaim(1), "Close makes the committed record reclaimable")
		})
	}
}

func TestCloseReleasesRejectedPutReservation(t *testing.T) {
	for kind, size := range map[string]int64{"inline": 5, "external": SizeUnknown} {
		t.Run(kind, func(t *testing.T) {
			st := openTestStore(t, t.TempDir())
			scheduler := holdTestIO(t, st)
			w, err := st.NewWriter([]byte("key"), size)
			require.NoError(t, err)
			_, err = w.Write([]byte("value"))
			require.NoError(t, err)
			path := st.objectPath(w.header.ObjectID)
			failure := errors.New("submission rejected")
			scheduler.holdNext, scheduler.reject = true, failure
			require.ErrorIs(t, st.Put(w), failure)
			require.ErrorIs(t, w.Close(), failure)
			st.Wait()
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

func TestIndexReuseWaitsForWipeAndSerializesEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := openTestStore(t, t.TempDir())
		scheduler := holdTestIO(t, st)
		for _, key := range []string{"a", "b", "c"} {
			putReclaimRecord(t, st, key, []byte("value"))
		}
		scheduler.holdNext = true
		var first, second, reserved bool
		go func() {
			require.EqualValues(t, 5, st.Reclaim(1))
			first = true
		}()
		synctest.Wait()
		require.EqualValues(t, 1, st.mu.index.head)
		require.Zero(t, st.mu.index.reuseHead)
		go func() {
			putReclaimRecord(t, st, "new", []byte("fresh"))
			reserved = true
		}()
		go func() {
			require.EqualValues(t, 5, st.Reclaim(1))
			second = true
		}()
		synctest.Wait()
		require.False(t, first)
		require.False(t, second, "another eviction waits for the first wipe")
		require.False(t, reserved, "tail cannot enter slots whose wipe is unfinished")
		require.Len(t, scheduler.held, 1)
		scheduler.complete(t, 0)
		require.True(t, first)
		require.True(t, second)
		require.True(t, reserved)
		st.Wait()
		require.Equal(t, st.mu.index.head, st.mu.index.reuseHead)
		require.Equal(t, liveStoreHeaders(st), indexReservations(reopenTestIndex(t, st.mu.index)))
	})
}

func TestReservationUsesReadySlotsDuringWipe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := openTestStore(t, t.TempDir())
		scheduler := holdTestIO(t, st)
		for _, key := range []string{"a", "b", "c", "d"} {
			putReclaimRecord(t, st, key, []byte("value"))
		}
		require.EqualValues(t, 10, st.Reclaim(10))
		putReclaimRecord(t, st, "e", []byte("value"))
		scheduler.holdNext = true
		go func() { require.EqualValues(t, 5, st.Reclaim(1)) }()
		synctest.Wait()
		putReclaimRecord(t, st, "f", []byte("value")) // The second already-cleared slot.
		require.EqualValues(t, 6, st.mu.index.tail)
		require.EqualValues(t, 2, st.mu.index.reuseHead)
		scheduler.complete(t, 0)
	})
}

func TestConcurrentWriterCloseRechecksSizeAfterWipe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := openTestStore(t, t.TempDir(), WithTargetSize(64<<10), WithInlineThreshold(0))
		st.staging = st.memory // Enough staging for all four open writers.
		var writers []*Writer
		for _, key := range []string{"a", "b", "c", "d"} {
			w, err := st.NewWriter([]byte(key), SizeUnknown)
			require.NoError(t, err)
			_, err = w.Write(bytes.Repeat([]byte(key), 24<<10))
			require.NoError(t, err)
			require.NoError(t, st.Put(w))
			writers = append(writers, w)
		}
		scheduler := holdTestIO(t, st)
		scheduler.holdNext = true
		var closed [4]bool
		go func() { require.NoError(t, writers[0].Close()); closed[0] = true }()
		synctest.Wait()
		// Complete the descriptor close, then hold the eviction batch it triggers.
		scheduler.holdNext = true
		scheduler.complete(t, 0)
		for i := 1; i < len(writers); i++ {
			go func() { require.NoError(t, writers[i].Close()); closed[i] = true }()
		}
		synctest.Wait()
		require.EqualValues(t, 72<<10, st.Size(), "only the first batch has retired a record")
		scheduler.complete(t, 1)
		require.Equal(t, [4]bool{true, true, true, true}, closed)
		require.EqualValues(t, 48<<10, st.Size(), "waiting writers must not repeat a stale byte request")
		require.Equal(t, []RecordHeader{writers[2].Header(), writers[3].Header()}, liveStoreHeaders(st))
	})
}

func TestIndexWipeFailureWakesReservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := openTestStore(t, t.TempDir())
		scheduler := holdTestIO(t, st)
		putReclaimRecord(t, st, "old", []byte("value"))
		scheduler.holdNext = true
		go func() { require.EqualValues(t, 5, st.Reclaim(1)) }()
		synctest.Wait()
		failure := errors.New("index wipe failed")
		var finished bool
		go func() {
			w, err := st.NewWriter([]byte("new"), 5)
			require.Nil(t, w)
			require.ErrorIs(t, err, failure)
			finished = true
		}()
		synctest.Wait()
		require.False(t, finished)
		scheduler.reject = failure
		scheduler.complete(t, 0)
		require.True(t, finished)
		require.Zero(t, st.mu.index.reuseHead)
		require.ErrorIs(t, st.Close(), failure)
	})
}
