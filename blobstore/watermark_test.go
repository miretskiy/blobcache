package blobstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

// Hold selected submissions and their callbacks to exercise incomplete I/O.
// Put does not use the returned ticket; give it a real completed one.
type heldIndexScheduler struct {
	iosched.Scheduler
	completed iosched.Ticket
	untilHold int
	held      []iosched.Op
	reject    error
}

func holdTestIndex(t *testing.T, st *Store) *heldIndexScheduler {
	t.Helper()
	require.NoError(t, st.scheduler.Close())
	backend := iosched.NewPOSIXScheduler()
	ticket, err := backend.Submit(iosched.DrainOp(st.mu.index.file))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.NoError(t, err)
	s := &heldIndexScheduler{Scheduler: backend, completed: ticket}
	st.scheduler = s
	return s
}

func (s *heldIndexScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	if s.untilHold != 0 {
		s.untilHold--
		if s.untilHold == 0 {
			if s.reject != nil {
				return iosched.Ticket{}, s.reject
			}
			s.held = append(s.held, op)
			return s.completed, nil
		}
	}
	return s.Scheduler.Submit(op)
}

func (s *heldIndexScheduler) complete(t *testing.T, i int) {
	t.Helper()
	ticket, err := s.Scheduler.Submit(s.held[i])
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.NoError(t, err)
}

func TestPendingIndexWritesPreventEvictionAndSlotReuse(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	scheduler := holdTestIndex(t, st)
	for _, key := range []string{"first", "second"} {
		w, err := st.NewWriter([]byte(key), 5)
		require.NoError(t, err)
		_, err = w.Write([]byte("value"))
		require.NoError(t, err)
		scheduler.untilHold = 2
		require.NoError(t, st.Put(w, nil))
		w.Discard()
	}
	require.Zero(t, st.Reclaim(10), "unfinished index I/O defers eviction without waiting")
	scheduler.complete(t, 1)
	require.Zero(t, st.Reclaim(10), "completion order does not change inline eviction order")
	third, err := st.NewWriter([]byte("third"), 5)
	require.NoError(t, err)
	_, err = third.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, st.Put(third, nil))
	require.NoError(t, third.Close())
	require.EqualValues(t, 3*RecordHeaderSize, indexFileSize(t, st.mu.index), "pending slots are not reused")
	scheduler.complete(t, 0)
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

func TestPutTransfersOwnershipUntilPublication(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	scheduler := holdTestIndex(t, st)
	w, err := st.NewWriter([]byte("key"), 5)
	require.NoError(t, err)
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	buffer := w.memory
	published := false
	scheduler.untilHold = 1 // Neither data nor index has completed when Put returns.
	require.NoError(t, st.Put(w, func(h RecordHeader, resident *alloc.Buffer) {
		published = true
		require.Same(t, buffer, resident)
		require.Zero(t, st.Reclaim(1), "protect the slot until the callback returns")
		resident.Unpin()
	}))
	require.NoError(t, w.Close())
	require.False(t, published)
	require.Zero(t, st.Reclaim(1))
	require.True(t, buffer.TryInc(), "closing the writer must not free pending I/O memory")
	buffer.Unpin()
	scheduler.complete(t, 0)
	st.Wait()
	require.True(t, published)
	require.False(t, buffer.TryInc())
	require.EqualValues(t, 5, st.Reclaim(1))
}

func TestRejectedIndexSubmissionReleasesReservation(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	scheduler := holdTestIndex(t, st)
	w, err := st.NewWriter([]byte("key"), SizeUnknown)
	require.NoError(t, err)
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	path := st.objectPath(w.header.ObjectID)
	scheduler.untilHold, scheduler.reject = 2, errors.New("submission rejected")
	require.NoError(t, st.Put(w, nil), "index submission happens after data completion")
	w.Discard()
	st.Wait()
	require.NoFileExists(t, path)
	require.Zero(t, st.Size())
	require.Empty(t, indexReservations(st.mu.index))
	h := putReclaimRecord(t, st, "next", []byte("next"))
	readReclaimRecord(t, st, "next", h, []byte("next"))
}
