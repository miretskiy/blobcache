package blobstore

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

func schedulerTestCPUs(t *testing.T, n int) []int {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	var mask unix.CPUSet
	require.NoError(t, unix.SchedGetaffinity(0, &mask))
	if mask.Count() < n {
		t.Skipf("need %d allowed CPUs", n)
	}
	var cpus []int
	for i := 0; len(cpus) < n; i++ {
		if mask.IsSet(i) {
			cpus = append(cpus, i)
		}
	}
	return cpus
}

func openMultiSchedulerStore(t *testing.T, path string, cpus []int, writes int, extra ...Option) *Store {
	t.Helper()
	pool := newStoreMemory(t, 2<<20)
	t.Cleanup(pool.Close)
	opts := []Option{TestingWithDirectIO(false),
		WithSchedulerCPUs(cpus...), WithWriteSchedulers(writes),
		WithStagingSize(8 << 10), WithInlineThreshold(16 << 10),
		WithDataIntegrity(DataIntegrityCRC32C), WithVerifyChecksums(true)}
	st, err := Open(path, pool, append(opts, extra...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

// Different rings may use the same VFD number concurrently. Object chains
// and early reader close must retain their owning scheduler.
func TestMultiSchedulerFileOwnershipAndRecovery(t *testing.T) {
	for _, tc := range []struct{ rings, writes int }{{2, 0}, {3, 1}, {4, 2}} {
		t.Run(fmt.Sprintf("rings=%d/writes=%d", tc.rings, tc.writes), func(t *testing.T) {
			cpus := schedulerTestCPUs(t, tc.rings)
			path := t.TempDir()
			st := openMultiSchedulerStore(t, path, cpus, tc.writes)
			writeCount := tc.writes
			if writeCount == 0 {
				writeCount = tc.rings
			}
			writers := make([]*Writer, 2*writeCount)
			for i := range writers {
				w, err := st.NewWriter([]byte(fmt.Sprint(i)))
				require.NoError(t, err)
				t.Cleanup(func() { w.Close() })
				writers[i] = w
				require.Same(t, st.schedulers[i%writeCount], w.wr.(*bufferSink).scheduler)
				_, err = w.Write(bytes.Repeat([]byte{byte(i)}, 40000))
				require.NoError(t, err)
				require.FileExists(t, st.objectPath(w.wr.(*bufferSink).header.ObjectID))
			}
			// Abandon one buffered record while peers on every ring remain usable.
			writers[0].Close()
			abandoned := st.objectPath(writers[0].wr.(*bufferSink).header.ObjectID)
			require.Eventually(t, func() bool { return !fileExists(abandoned) }, time.Second, time.Millisecond)
			var headers []RecordHeader
			for i := len(writers) - 1; i > 0; i-- {
				w := writers[i]
				err := putStoreWriter(st, w)
				require.NoError(t, err)

				require.Nil(t, w.buf)
				headers = append(headers, w.wr.(*bufferSink).header)
				require.NoError(t, w.Close())
			}
			// Every read ring can read an object written by any write ring.
			var readers []*Reader
			for i := 0; i < 2*(tc.rings-tc.writes); i++ {
				r, err := st.NewReader([]byte("1"), writers[1].wr.(*bufferSink).header)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, r.Close()) })
				require.Same(t, st.schedulers[tc.writes+i%(tc.rings-tc.writes)], r.scheduler)
				readers = append(readers, r)
			}
			require.NoError(t, readers[0].Close())
			var group errgroup.Group
			for _, r := range readers[1:] {
				group.Go(func() error {
					got, err := io.ReadAll(r)
					if err != nil {
						return err
					}
					if !bytes.Equal(got, bytes.Repeat([]byte{1}, 40000)) {
						return fmt.Errorf("wrong contents from ring-owned refill")
					}
					return r.Close()
				})
			}
			require.NoError(t, group.Wait())
			// Inline records also share the regular data descriptor safely.
			inline := publishForRead(t, st, []byte("inline"), []byte("inline value"))
			headers = append(headers, inline)
			require.NoError(t, st.Close())
			// Recovery is independent of the original routing and CPU layout.
			st = openMultiSchedulerStore(t, path, cpus[:1], 0)
			var recovered []RecordHeader
			for h := range st.Records() {
				recovered = append(recovered, h)
			}
			require.ElementsMatch(t, headers, recovered)
			for _, h := range headers {
				key, want := []byte("inline"), []byte("inline value")
				if h.Flags.IsExternal() {
					i := h.ObjectID - writers[0].wr.(*bufferSink).header.ObjectID
					key, want = []byte(fmt.Sprint(i)), bytes.Repeat([]byte{byte(i)}, 40000)
				}
				r, err := st.NewReader(key, h)
				require.NoError(t, err)
				got, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, want, got)
				require.NoError(t, r.Close())
			}
		})
	}
}

type gatedScheduler struct {
	iosched.Scheduler
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Scheduler.Submit(op)
}

func TestDedicatedWriteSchedulerAndAffinity(t *testing.T) {
	cpus := schedulerTestCPUs(t, 4)
	st := openMultiSchedulerStore(t, t.TempDir(), cpus, 1,
		WithIOBudget(0))
	h := publishForRead(t, st, []byte("existing"), []byte("value"))
	for i, scheduler := range st.schedulers {
		type observation struct {
			mask unix.CPUSet
			err  error
		}
		observed := make(chan observation, 1)
		ticket, err := iosched.SubmitNotify(scheduler, iosched.ReadOp(st.data, make([]byte, 1), 0), func(_ int, _ error) {
			var o observation
			o.err = unix.SchedGetaffinity(0, &o.mask)
			observed <- o
		})
		require.NoError(t, err)
		_, err = ticket.Wait()
		require.NoError(t, err)
		o := <-observed
		require.NoError(t, o.err)
		require.Equal(t, 1, o.mask.Count())
		require.True(t, o.mask.IsSet(cpus[i]))
	}
	stats := st.IOSchedulerStats()
	require.Equal(t, "write", stats[0].Role)
	require.Zero(t, stats[0].WriteIOBudget)
	for _, s := range stats[1:] {
		require.Equal(t, "read", s.Role)
		require.Zero(t, s.WriteIOBudget)
	}

	gate := &gatedScheduler{Scheduler: st.schedulers[0], entered: make(chan struct{}), release: make(chan struct{})}
	st.schedulers[0] = gate
	var release sync.Once
	unblock := func() { release.Do(func() { close(gate.release) }) }
	t.Cleanup(unblock)
	w, err := st.NewWriter([]byte("pending"))
	require.NoError(t, err)
	_, err = w.Write([]byte("next"))
	require.NoError(t, err)
	written := make(chan struct{})
	var writeErr error
	go func() { writeErr = putStoreWriter(st, w); close(written) }()
	t.Cleanup(func() { unblock(); <-written; w.Close() })
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not reach its scheduler")
	}
	read := make(chan error, 1)
	go func() {
		r, err := st.NewReader([]byte("existing"), h)
		if err == nil {
			_, err = io.Copy(io.Discard, r)
			_ = r.Close()
		}
		read <- err
	}()
	select {
	case err := <-read:
		require.NoError(t, err, "read must progress while writes are held")
	case <-time.After(5 * time.Second):
		unblock()
		<-written
		<-read
		t.Fatal("read was routed to the write-only scheduler")
	}
	unblock()
	<-written
	require.NoError(t, writeErr)

	require.NoError(t, w.Close())
}
