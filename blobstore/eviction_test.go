package blobstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func writeRetry(t *testing.T, st *Store, key string, value []byte) Location {
	t.Helper()
	buf := alignedMemory(t, st.RecordSize(len(key), len(value)))
	copy(buf, value)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ticket, err := st.Write([]byte(key), buf, len(value))
		if errors.Is(err, ErrBusy) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			continue
		}
		require.NoError(t, err)
		loc, err := ticket.Wait()
		require.NoError(t, err)
		return loc
	}
}

func TestSegmentIDsAboveUint32(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	st.activeSegment.nextID = 1<<32 + 7
	loc := write(t, st, "large-id", []byte("value"))
	require.Equal(t, uint64(1<<32+7), loc.Segment())
	require.NoError(t, st.Close())
	require.FileExists(t, filepath.Join(dir, "07", "0000000100000007.seg"))
	rec := recorder{}
	st = openStore(t, dir, rec)
	defer func() { require.NoError(t, st.Close()) }()
	require.Equal(t, loc, rec[HashKey([]byte("large-id"))])
	next := write(t, st, "next", []byte("next"))
	require.Equal(t, loc.Segment()+1, next.Segment())
}

func TestFIFOEvictionCallbackAndReadHandles(t *testing.T) {
	const limit = 3 * testSegment
	events := make(chan Eviction, 64)
	var st *Store
	callback := func(e Eviction) {
		// Calling Stats and taking both mutexes here must be safe.
		_ = st.Stats()
		st.activeSegment.Lock()
		if st.activeSegment.nextID <= slices.Max(e) {
			t.Error("retired an unallocated segment")
		}
		st.activeSegment.Unlock()
		st.reads.mu.Lock()
		if len(st.reads.byID) > 2 {
			t.Error("handle limit exceeded")
		}
		st.reads.mu.Unlock()
		events <- e
	}
	st = openStore(t, t.TempDir(), recorder{}, WithMaxSize(limit), WithMaxReadHandles(2), WithEvictionCallback(callback))
	defer func() { require.NoError(t, st.Close()) }()
	old := writeRetry(t, st, "old", randomBytes(1, 100<<10))
	_, err := read(t, st, old, "old")
	require.NoError(t, err)
	for i := 0; i < 18; i++ {
		writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
		require.LessOrEqual(t, st.Stats().DiskBytes, int64(limit))
	}
	require.Eventually(t, func() bool { return st.Stats().EvictedSegments > 0 }, 5*time.Second, time.Millisecond)
	event := <-events
	require.Equal(t, Eviction{old.Segment(), old.Segment() + 1}, event) // one callback for two segments
	require.NoFileExists(t, segmentPath(st.root, old.Segment()))
	_, err = read(t, st, old, "old")
	require.ErrorIs(t, err, ErrNotFound)
	st.reads.mu.Lock()
	require.NotContains(t, st.reads.byID, old.Segment())
	st.reads.mu.Unlock()
}

func TestEvictionUnreadableFooter(t *testing.T) {
	events := make(chan Eviction, 16)
	st := openStore(t, t.TempDir(), recorder{}, WithMaxSize(4*testSegment), WithEvictionCallback(func(e Eviction) { events <- e }))
	defer func() { require.NoError(t, st.Close()) }()
	first := writeRetry(t, st, "first", randomBytes(0, 100<<10))
	for i := 1; i < 3; i++ {
		writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	eraseFooter(t, segmentPath(st.root, first.Segment()))
	for i := 3; i < 10; i++ {
		writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	select {
	case e := <-events:
		require.Equal(t, Eviction{first.Segment(), first.Segment() + 1}, e)
	case <-time.After(5 * time.Second):
		t.Fatal("no eviction")
	}
}

func TestEvictionUnlinkFailureIsBestEffort(t *testing.T) {
	events := make(chan Eviction, 16)
	st := openStore(t, t.TempDir(), recorder{}, WithMaxSize(3*testSegment), WithEvictionCallback(func(e Eviction) { events <- e }))
	defer func() { require.NoError(t, st.Close()) }()
	for i := 0; i < 6; i++ {
		writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	// The first unlink fails, but must not block its peer or the next write.
	path := segmentPath(st.root, 0)
	require.NoError(t, os.Rename(path, path+".saved"))
	require.NoError(t, os.Mkdir(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "block-unlink"), []byte("x"), 0600))
	next := writeRetry(t, st, "new", randomBytes(4, 100<<10))
	require.Equal(t, uint64(2), next.Segment())
	require.Equal(t, Eviction{0, 1}, <-events)
	require.Equal(t, uint64(2), st.Stats().EvictedSegments)
	require.Equal(t, 1, st.Stats().Segments)
	require.Equal(t, int64(testSegment), st.Stats().DiskBytes)
	require.Eventually(t, func() bool { return st.Stats().EvictionErrors > 0 }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { _, err := os.Stat(segmentPath(st.root, 1)); return errors.Is(err, os.ErrNotExist) }, 5*time.Second, time.Millisecond)
	// No retry queue or persistent eviction state: the failed file is left
	// for future startup/capacity handling; later writes still rotate normally.
	for i := 7; i < 18; i++ {
		writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	require.Equal(t, uint64(1), st.Stats().EvictionErrors)
}

func TestShortInitialReadCannotUseOldTrailer(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{}, WithDirectReads(false))
	loc := write(t, st, "key", randomBytes(4, 10000))
	require.NoError(t, st.Close())
	st = openStore(t, st.root, recorder{}, WithDirectReads(false))
	defer func() { require.NoError(t, st.Close()) }()
	buf := alignedMemory(t, loc.Size())
	value, err := st.Read(loc, []byte("key"), buf)
	require.NoError(t, err)
	require.Len(t, value, 10000)
	// Reopen without replacing the caller's formerly valid record buffer.
	require.NoError(t, st.Close())
	st = openStore(t, st.root, recorder{}, WithDirectReads(false))
	require.NoError(t, os.Truncate(segmentPath(st.root, loc.Segment()), int64(loc.offset+loc.size)-1))
	_, err = st.Read(loc, []byte("key"), buf)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestConcurrentHandleLimit(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{}, WithMaxReadHandles(1))
	defer func() { require.NoError(t, st.Close()) }()
	var locs []Location
	for i := 0; i < 12; i++ {
		locs = append(locs, writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10)))
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			buf := align.AllocAligned(128 << 10)
			defer align.FreeAligned(buf)
			for i := 0; i < 50; i++ {
				n := i % len(locs)
				_, err := st.Read(locs[n], []byte(fmt.Sprint(n)), buf)
				if err != nil && !errors.Is(err, ErrBusy) {
					t.Error(err)
				}
				st.reads.mu.Lock()
				if len(st.reads.byID) > 1 {
					t.Error("handle limit exceeded")
				}
				st.reads.mu.Unlock()
			}
		})
	}
	wg.Wait()
}

func TestRejectedSealStillDrainsAndCloses(t *testing.T) {
	var submissions atomic.Int32
	st := openStore(t, t.TempDir(), recorder{}, TestingWithPreSubmit(func(op iosched.Op) iosched.Op {
		if submissions.Add(1) == 3 {
			return iosched.Op{}
		} // invalid: Submit rejects the whole seal
		return op
	}))
	first := write(t, st, "first", randomBytes(1, 100<<10))
	write(t, st, "second", randomBytes(2, 100<<10))
	_, err := st.Write([]byte("last"), alignedMemory(t, st.RecordSize(4, 100<<10)), 100<<10)
	require.Error(t, err)
	st.open.Wait() // cleanup drains the two accepted writes before returning its slot
	require.Equal(t, uint64(1), st.Stats().FailedSegments)
	got, err := read(t, st, first, "first")
	require.NoError(t, err)
	require.Equal(t, randomBytes(1, 100<<10), got)
	write(t, st, "next", []byte("next segment"))
	require.NoError(t, st.Close())
}

func TestRecoveryAppliesCapacityAfterLoading(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	for i := 0; i < 18; i++ {
		write(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	require.NoError(t, st.Close())
	events := make(chan Eviction, 16)
	rec := recorder{}
	st = openStore(t, dir, rec, WithMaxSize(3*testSegment), WithEvictionCallback(func(e Eviction) { events <- e }))
	defer func() { require.NoError(t, st.Close()) }()
	require.Len(t, rec, 18)
	require.Eventually(t, func() bool { return st.Stats().DiskBytes <= 2*testSegment }, 5*time.Second, time.Millisecond)
	require.NotEmpty(t, events)
}

func TestLastWriteFailureStillWritesFooterAndCloses(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	first := write(t, st, "first", randomBytes(1, 100<<10))
	second := write(t, st, "second", randomBytes(2, 100<<10))
	s := st.activeSegment.seg
	off := s.pos
	size := st.RecordSize(4, 100<<10)
	entries := append(slices.Clone(s.entries), footerEntry{hash: HashKey([]byte("last")), off: uint32(off), size: uint32(size)})
	footer := alignedMemory(t, int(footerSize(len(entries))))
	encodeSegmentFooter(footer, s.id, entries)
	// Inject a real failed write followed by the production seal's hard-linked
	// operations. Earlier writes are on the real slot and must survive reopen.
	st.cfg.preSubmit = func(iosched.Op) iosched.Op {
		return iosched.VWriteOp(writeSlots-1, make([]byte, 4096), 0).HardLink(
			iosched.VWriteOp(s.slot, footer, align.PageAlign(off+int64(size))),
			iosched.VFdatasyncOp(s.slot), iosched.VCloseOp(s.slot), iosched.FsyncOp(st.dirs[s.id%shardCount]))
	}
	ticket, err := st.Write([]byte("last"), alignedMemory(t, size), 100<<10)
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.Error(t, err)
	st.cfg.preSubmit = nil
	require.NoError(t, st.Close())
	rec := recorder{}
	st = openStore(t, dir, rec)
	defer func() { require.NoError(t, st.Close()) }()
	require.Equal(t, first, rec[HashKey([]byte("first"))])
	require.Equal(t, second, rec[HashKey([]byte("second"))])
	_, err = read(t, st, rec[HashKey([]byte("last"))], "last")
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestTwoSegmentBudgetMakesProgress(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{}, WithMaxSize(2*testSegment))
	defer func() { require.NoError(t, st.Close()) }()
	for i := 0; i < 18; i++ {
		writeRetry(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
		require.LessOrEqual(t, st.Stats().DiskBytes, int64(2*testSegment))
	}
	require.Positive(t, st.Stats().EvictedSegments)
}
