package blobstore

import (
	"encoding/binary"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A real policy rejection after reservation and an earlier external flush must
// release the writer even while unrelated admitted I/O cannot complete.
func TestBacklogRejectionReleasesWriterWhileDiskBlocked(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	const backlog = 64 << 10
	st := openTestStore(t, t.TempDir(), WithRejectWriteBacklog(backlog))
	w, err := st.NewWriter([]byte("external"), SizeUnknown)
	require.NoError(t, err)
	t.Cleanup(w.Discard)
	_, err = w.Write([]byte("first"))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.GreaterOrEqual(t, w.vfd, 0)
	vfd, path, allocated := uint32(w.vfd), st.objectPath(w.header.ObjectID), w.memory.Cap()
	require.NotZero(t, w.reservation)

	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	gate := os.NewFile(uintptr(fd), "held-io")
	t.Cleanup(func() { require.NoError(t, gate.Close()) })
	f, err := os.CreateTemp(t.TempDir(), "write")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	var once sync.Once
	unblock := func() {
		once.Do(func() {
			var value [8]byte
			binary.NativeEndian.PutUint64(value[:], 1)
			_, err := gate.Write(value[:])
			require.NoError(t, err)
		})
	}
	t.Cleanup(unblock)
	held, err := st.schedulers[0].Submit(iosched.ReadOp(gate, make([]byte, 8), 0).
		Link(iosched.WriteOp(f, make([]byte, backlog), 0)))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		q := st.IODiskStats().Writes
		return q.Bytes == backlog && q.ServiceTime > 0
	}, time.Second, time.Millisecond)

	require.ErrorIs(t, w.Flush(), ErrRejected)
	require.ErrorIs(t, w.Close(), ErrRejected)
	done := make(chan struct{})
	go func() { st.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer cleanup waited for unrelated blocked I/O")
	}
	require.Zero(t, w.reservation)
	require.Nil(t, w.memory)
	require.Empty(t, indexReservations(st.mu.index))
	require.NoFileExists(t, path)
	memory, err := st.staging.TryAcquire(allocated)
	require.NoError(t, err)
	memory.Unpin()
	closed, err := st.schedulers[0].Submit(iosched.VReadOp(vfd, make([]byte, 4096), 0))
	require.NoError(t, err)
	_, err = closed.Wait()
	require.ErrorIs(t, err, unix.EBADF)
	require.EqualValues(t, backlog, st.IODiskStats().Writes.Bytes, "cleanup did not release another submission's charge")
	unblock()
	_, err = held.Wait()
	require.NoError(t, err)
	require.Zero(t, st.IODiskStats().Writes.Bytes)
	next := putReclaimRecord(t, st, "next", []byte("next"))
	readReclaimRecord(t, st, "next", next, []byte("next"))
}
