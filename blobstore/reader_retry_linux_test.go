package blobstore

import (
	"bytes"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func leaseFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	_, err = unix.FcntlInt(f.Fd(), unix.F_SETSIG, int(unix.SIGURG))
	require.NoError(t, err)
	_, err = unix.FcntlInt(f.Fd(), unix.F_SETLEASE, unix.F_WRLCK)
	require.NoError(t, err)
	return f
}

// A completion error is returned on first access and remains sticky. The reader
// must not resubmit an operation or expose bytes from the failed ticket.
func TestReaderReportsEAGAIN(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	for _, external := range []bool{false, true} {
		t.Run(strconv.FormatBool(external), func(t *testing.T) {
			threshold := int64(64 << 10)
			if external {
				threshold = 0
			}
			st := openTestStore(t, t.TempDir(), WithInlineThreshold(threshold))
			key, value := []byte("key"), bytes.Repeat([]byte("v"), 40000)
			h := publishForRead(t, st, key, value)
			counted := &countingReadScheduler{Scheduler: st.schedulers[0]}
			st.schedulers[0] = counted
			r, err := st.NewReader(key, h)
			require.NoError(t, err)
			defer r.Close()
			_, err = r.ticket.Wait()
			require.NoError(t, err)
			path := t.TempDir() + "/leased"
			require.NoError(t, os.WriteFile(path, nil, 0600))
			leaseFile(t, path)
			r.ticket, err = counted.Submit(iosched.OpenatOp(unix.AT_FDCWD, path, os.O_RDONLY|unix.O_NONBLOCK, 0))
			require.NoError(t, err)
			for range 2 {
				chunk, err := r.Next()
				require.ErrorIs(t, err, unix.EAGAIN)
				require.Nil(t, chunk)
			}
			require.Equal(t, 2, counted.submissions, "first access must not submit retries")
			require.NoError(t, r.Close())
			require.Equal(t, st.memory.Capacity(), st.memory.Available())
		})
	}
}

func TestReaderCloseDoesNotWaitForIO(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	key, value := []byte("key"), bytes.Repeat([]byte("v"), 48000)
	h := publishForRead(t, st, key, value)
	lease := leaseFile(t, st.objectPath(h.ObjectID))
	r, err := st.NewReader(key, h)
	require.NoError(t, err, "construction submits without waiting for the leased file")
	ticket := r.ticket
	available := st.memory.Available()
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited for pending I/O")
	}
	require.Equal(t, available, st.memory.Available(), "I/O retains the buffer after Close")
	_, err = unix.FcntlInt(lease.Fd(), unix.F_SETLEASE, unix.F_UNLCK)
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.NoError(t, err)
	require.Equal(t, st.memory.Capacity(), st.memory.Available(), "completion releases the last buffer reference")
}
