package blobstore

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

type rejectNextScheduler struct {
	iosched.Scheduler
	reject atomic.Bool
}

func (s *rejectNextScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	if s.reject.Swap(false) {
		return iosched.Ticket{}, iosched.ErrRejected
	}
	return s.Scheduler.Submit(op)
}

func TestRejectedExternalWriteClosesExistingVFD(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	st := openTestStore(t, t.TempDir())
	scheduler := &rejectNextScheduler{Scheduler: st.schedulers[0]}
	st.schedulers[0] = scheduler
	w, err := st.NewWriter([]byte("external"), SizeUnknown)
	require.NoError(t, err)
	t.Cleanup(w.Discard)
	_, err = w.Write([]byte("first"))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.GreaterOrEqual(t, w.vfd, 0)
	path := st.objectPath(w.header.ObjectID)
	scheduler.reject.Store(true)
	require.ErrorIs(t, w.Flush(), ErrRejected)
	require.ErrorIs(t, w.Close(), ErrRejected)
	st.Wait()
	require.Equal(t, -1, w.vfd)
	require.Nil(t, w.memory)
	require.NoFileExists(t, path)
	require.Empty(t, liveStoreHeaders(st))
}

func TestIndexWipeBypassesRejection(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	st := openTestStore(t, t.TempDir())
	putReclaimRecord(t, st, "key", []byte("value"))
	st.Wait()
	require.NoError(t, st.schedulers[0].Close())
	policy := iosched.NewDiskPolicy(iosched.WithRejectWriteBacklog(1))
	scheduler, err := iosched.NewURingScheduler(iosched.WithDiskPolicy(policy))
	require.NoError(t, err)
	st.schedulers[0] = scheduler
	gate, signal, err := os.Pipe()
	require.NoError(t, err)
	f, err := os.CreateTemp(t.TempDir(), "held-write")
	require.NoError(t, err)
	held, err := scheduler.Submit(iosched.ReadOp(gate, make([]byte, 1), 0).
		Link(iosched.WriteOp(f, []byte{1}, 0)))
	require.NoError(t, err)
	defer func() {
		_, err := signal.Write([]byte{1})
		require.NoError(t, err)
		require.NoError(t, signal.Close())
		_, err = held.Wait()
		require.NoError(t, err)
		require.NoError(t, gate.Close())
		require.NoError(t, f.Close())
	}()
	require.Eventually(t, func() bool { return policy.Stats().Writes.ServiceTime > 0 }, time.Second, time.Millisecond)
	_, err = scheduler.Submit(iosched.WriteOp(f, []byte{1}, 0))
	require.ErrorIs(t, err, iosched.ErrRejected)
	// The wipe's MustAdmit exemption must allow reclamation and slot reuse even
	// while the shared circuit rejects ordinary writes.

	require.EqualValues(t, 5, st.Reclaim(5))
	st.Wait()
	require.NoError(t, st.mu.index.wipeErr)
	require.Equal(t, st.mu.index.head, st.mu.index.reuseHead)
}
