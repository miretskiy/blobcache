package blobstore

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type readFaultScheduler struct {
	iosched.Scheduler
	submit func(iosched.Op) (iosched.Ticket, error)
}

func (s *readFaultScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	return s.submit(op)
}

// An incompatible file lease makes a nonblocking open complete with EAGAIN.
// Substitute that operation for a refill's read, keeping an external refill's
// open and hard-linked close so their descriptor effects remain real.
func TestReaderRetriesEAGAIN(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	for _, tc := range []struct {
		name            string
		external        bool
		size, at, count int
	}{
		{"inline", false, 5000, 1, 2},
		{"inline-continuation", false, 30000, 2, 1},
		{"inline-exhausted", false, 5000, 1, readRetries + 1},
		{"external", true, 5000, 1, 2},
		{"external-continuation", true, 40000, 2, 1},
		{"external-exhausted", true, 40000, 1, readRetries + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			threshold := int64(64 << 10)
			if tc.external {
				threshold = 0
			}
			st := openTestStore(t, t.TempDir(), WithInlineThreshold(threshold), WithVerifyChecksums(true))
			key := []byte("retry-key")
			value := make([]byte, tc.size)
			for i := range value {
				value[i] = byte(i*31 + i/251)
			}
			h := publishForRead(t, st, key, value)
			require.Equal(t, tc.external, h.Flags.IsExternal())
			leased, err := os.CreateTemp(t.TempDir(), "leased")
			require.NoError(t, err)
			t.Cleanup(func() { leased.Close() })
			// SIGURG is ignored outside Go's preemption handler, unlike the
			// default lease-break SIGIO. The lease is released by Close.
			_, err = unix.FcntlInt(leased.Fd(), unix.F_SETSIG, int(unix.SIGURG))
			require.NoError(t, err)
			_, err = unix.FcntlInt(leased.Fd(), unix.F_SETLEASE, unix.F_WRLCK)
			require.NoError(t, err)
			base := st.schedulers[0]
			calls := 0
			var opened []uint32
			before := metrics.ReadRetries.Load()
			st.schedulers[0] = &readFaultScheduler{Scheduler: base, submit: func(op iosched.Op) (iosched.Ticket, error) {
				calls++
				if calls < tc.at || calls >= tc.at+tc.count {
					return base.Submit(op)
				}
				fault := iosched.OpenatOp(unix.AT_FDCWD, leased.Name(), os.O_RDONLY|unix.O_NONBLOCK, 0)
				if tc.external {
					fault = st.openObject(h.ObjectID, os.O_RDONLY).Link(
						fault.HardLink(iosched.VCloseOp(iosched.AllocateVFD)))
				}
				ticket, err := base.Submit(fault)
				require.NoError(t, err)
				_, err = ticket.Wait()
				require.ErrorIs(t, err, unix.EAGAIN)
				if tc.external {
					results := ticket.AllResults()
					require.NoError(t, results[0].Err)
					require.NoError(t, results[2].Err, "the hard-linked close runs after the failed read")
					opened = append(opened, uint32(results[0].N))
				}
				return ticket, nil
			}}
			reader, err := st.NewReader(key, h)
			if tc.count > readRetries {
				require.ErrorIs(t, err, unix.EAGAIN)
				require.Nil(t, reader)
				require.EqualValues(t, readRetries, metrics.ReadRetries.Load()-before)
			} else {
				require.NoError(t, err)
				got, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.True(t, bytes.Equal(value, got), "retry must not skip or duplicate bytes")
				require.EqualValues(t, tc.count, metrics.ReadRetries.Load()-before)
				require.NoError(t, reader.Close())
			}
			for _, vfd := range opened {
				ticket, err := base.Submit(iosched.VReadOp(vfd, make([]byte, 1), 0))
				require.NoError(t, err)
				_, err = ticket.Wait()
				require.ErrorIs(t, err, unix.EBADF, "retry must not leak an opened slot")
			}
		})
	}
}
