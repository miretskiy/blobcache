package blobstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestUsesIOUring(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{})
	defer func() { require.NoError(t, st.Close()) }()
	require.IsType(t, &iosched.URingScheduler{}, st.queues[0].sched)
}

// A pending replacement already queued the old descriptor's close. Eviction
// can retire that segment immediately without waiting for the chain to run.
func TestRetirementDoesNotWaitForPendingReplacement(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{}, WithMaxReadHandles(1))
	defer func() { require.NoError(t, st.Close()) }()
	old := write(t, st, "old", randomBytes(1, 100<<10))
	for i := 0; i < 2; i++ {
		write(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	next := write(t, st, "next", randomBytes(2, 100<<10))
	_, err := read(t, st, old, "old")
	require.NoError(t, err)
	blocker, release := evictionGate(t)
	defer release()
	var gate atomic.Bool
	st.cfg.preSubmit = func(op iosched.Op) iosched.Op {
		if gate.CompareAndSwap(false, true) {
			return iosched.ReadOp(blocker, make([]byte, 8), 0).Link(op)
		}
		return op
	}
	readDone := make(chan error, 1)
	go func() { _, err := read(t, st, next, "next"); readDone <- err }()
	require.Eventually(t, func() bool {
		st.readers[0].reads.mu.Lock()
		defer st.readers[0].reads.mu.Unlock()
		return st.readers[0].reads.byID[next.segment] != nil
	}, 5*time.Second, time.Millisecond)
	retired := make(chan struct{})
	go func() { st.retireReads(old.segment + 1); close(retired) }()
	select {
	case <-retired:
	case <-time.After(5 * time.Second):
		t.Fatal("retirement waited for a pending replacement")
	}
	_, err = read(t, st, old, "old")
	require.ErrorIs(t, err, ErrNotFound)
	release()
	require.NoError(t, <-readDone)
	got, err := read(t, st, next, "next")
	require.NoError(t, err)
	require.Equal(t, randomBytes(2, 100<<10), got)
}

func TestRetirementClosesPendingOpenWithoutWaiting(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{}, WithMaxReadHandles(1))
	defer func() { require.NoError(t, st.Close()) }()
	loc := write(t, st, "key", []byte("value"))
	blocker, release := evictionGate(t)
	defer release()
	var gate atomic.Bool
	st.cfg.preSubmit = func(op iosched.Op) iosched.Op {
		if gate.CompareAndSwap(false, true) {
			return iosched.ReadOp(blocker, make([]byte, 8), 0).Link(op)
		}
		return op
	}
	readDone := make(chan error, 1)
	go func() { _, err := read(t, st, loc, "key"); readDone <- err }()
	require.Eventually(t, func() bool {
		st.readers[0].reads.mu.Lock()
		defer st.readers[0].reads.mu.Unlock()
		return st.readers[0].reads.byID[loc.segment] != nil
	}, 5*time.Second, time.Millisecond)
	retired := make(chan struct{})
	go func() { st.retireReads(loc.segment + 1); close(retired) }()
	select {
	case <-retired:
	case <-time.After(5 * time.Second):
		t.Fatal("retirement waited for a pending open")
	}
	release()
	require.NoError(t, <-readDone)
	st.readers[0].reads.mu.Lock()
	slot := st.readers[0].reads.slots[0]
	_, mapped := st.readers[0].reads.byID[loc.segment]
	st.readers[0].reads.mu.Unlock()
	require.True(t, slot.closing)
	require.False(t, mapped)
	_, err := slot.opening.Wait()
	require.NoError(t, err)
	// The initial reader submitted a close; no descriptor is retained forever.
	ticket, err := st.queues[0].sched.Submit(iosched.VReadOp(slot.vfd, make([]byte, 4096), 0))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, unix.EBADF)
}

func evictionGate(t *testing.T) (*os.File, func()) {
	t.Helper()
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	blocker := os.NewFile(uintptr(fd), "eviction-gate")
	t.Cleanup(func() { require.NoError(t, blocker.Close()) })
	var once sync.Once
	return blocker, func() {
		once.Do(func() {
			var one [8]byte
			binary.NativeEndian.PutUint64(one[:], 1)
			_, err := blocker.Write(one[:])
			require.NoError(t, err)
		})
	}
}
