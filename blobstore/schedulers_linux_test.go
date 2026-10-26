package blobstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/alloc"
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
	pool, err := alloc.NewSlabAllocator(2 << 20)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	opts := []Option{TestingWithDirectIO(false),
		WithSchedulerCPUs(cpus...), WithWriteSchedulers(writes),
		WithStagingSize(8 << 10), WithInlineThreshold(16 << 10),
		WithDataIntegrity(DataIntegrityCRC32C), WithVerifyChecksums(true)}
	st, err := Open(path, pool, pool, append(opts, extra...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

// Different rings may use the same VFD number concurrently. Flush, final
// chains, early reader close, and abandoned-writer cleanup must retain owners.
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
				w, err := st.NewWriter([]byte(fmt.Sprint(i)), SizeUnknown)
				require.NoError(t, err)
				t.Cleanup(w.Discard)
				writers[i] = w
				require.Same(t, st.schedulers[i%writeCount], w.scheduler)
				_, err = w.Write(bytes.Repeat([]byte{byte(i)}, 40000))
				require.NoError(t, err)
				require.GreaterOrEqual(t, w.vfd, 0)
			}
			// Abandon one open file while peers on every ring still own slots.
			writers[0].Discard()
			st.Wait()
			require.NoFileExists(t, st.objectPath(writers[0].header.ObjectID))
			var headers []RecordHeader
			for i := len(writers) - 1; i > 0; i-- {
				w := writers[i]
				require.NoError(t, st.Put(w))
				require.Equal(t, -1, w.vfd)
				headers = append(headers, w.Header())
				require.NoError(t, w.Close())
			}
			// Every read ring can read an object written by any write ring.
			var readers []*Reader
			for i := 0; i < 2*(tc.rings-tc.writes); i++ {
				r, err := st.NewReader([]byte("1"), writers[1].Header())
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
			st.Recover(func(h RecordHeader) { recovered = append(recovered, h) })
			require.ElementsMatch(t, headers, recovered)
			for _, h := range headers {
				key, want := []byte("inline"), []byte("inline value")
				if h.Flags.IsExternal() {
					i := h.ObjectID - writers[0].header.ObjectID
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
	w, err := st.NewWriter([]byte("pending"), 4)
	require.NoError(t, err)
	_, err = w.Write([]byte("next"))
	require.NoError(t, err)
	written := make(chan struct{})
	var writeErr error
	go func() { writeErr = st.Put(w); close(written) }()
	t.Cleanup(func() { unblock(); <-written; w.Discard() })
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
	// Retirement and index wipes also use the write pool, then drain before
	// shared descriptors are closed. Verify recovery sees their removal.
	st.Reclaim(st.Size())
	st.Wait()
	require.Zero(t, st.Size())
	path := st.base
	require.NoError(t, st.Close())
	reopened := openMultiSchedulerStore(t, path, cpus, 1)
	require.Zero(t, reopened.Size())
}

func TestPendingWriteOnAnotherRingPreventsReclaim(t *testing.T) {
	st := openMultiSchedulerStore(t, t.TempDir(), schedulerTestCPUs(t, 3), 2)
	gate := &gatedScheduler{Scheduler: st.schedulers[0], entered: make(chan struct{}), release: make(chan struct{})}
	st.schedulers[0] = gate
	var release sync.Once
	unblock := func() { release.Do(func() { close(gate.release) }) }
	w, err := st.NewWriter([]byte("first"), 5)
	require.NoError(t, err)
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	done := make(chan struct{})
	var writeErr error
	go func() { writeErr = st.Put(w); close(done) }()
	t.Cleanup(func() { unblock(); <-done; w.Discard() })
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not reach its ring")
	}
	// This Put completes on the other write ring. Its completion does not
	// release the first ring's pending reservation or its index slot.
	second := publishForRead(t, st, []byte("second"), []byte("other"))
	require.Zero(t, st.Reclaim(st.Size()))
	r, err := st.NewReader([]byte("second"), second)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("other"), got)
	require.NoError(t, r.Close())
	unblock()
	<-done
	require.NoError(t, writeErr)
	require.NoError(t, w.Close())
	require.EqualValues(t, 10, st.Reclaim(10))
	st.Wait()
	require.Zero(t, st.Size())
}

func TestSchedulersShareDiskBacklog(t *testing.T) {
	st := openMultiSchedulerStore(t, t.TempDir(), schedulerTestCPUs(t, 3), 2,
		WithRejectWriteBacklog(8))
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	gate := os.NewFile(uintptr(fd), "held-read")
	t.Cleanup(func() { require.NoError(t, gate.Close()) })
	f, err := os.CreateTemp(t.TempDir(), "writes")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	// Hold the first chain before its write. Its bytes must count against the
	// other writer's admission limit as well.
	ticket, err := st.schedulers[0].Submit(iosched.ReadOp(gate, make([]byte, 8), 0).
		Link(iosched.WriteOp(f, make([]byte, 8), 0)))
	require.NoError(t, err)
	var once sync.Once
	unblock := func() {
		once.Do(func() {
			buf := make([]byte, 8)
			binary.NativeEndian.PutUint64(buf, 1)
			_, err := gate.Write(buf)
			require.NoError(t, err)
		})
	}
	t.Cleanup(unblock)
	require.Eventually(t, func() bool { q := st.IODiskStats().Writes; return q.Bytes == 8 && q.ServiceTime > 0 }, 5*time.Second, time.Millisecond)
	_, err = st.schedulers[1].Submit(iosched.WriteOp(f, []byte{1}, 8))
	require.ErrorIs(t, err, iosched.ErrRejected)
	read, err := st.schedulers[2].Submit(iosched.ReadOp(f, make([]byte, 1), 0))
	require.NoError(t, err)
	_, err = read.Wait()
	require.NoError(t, err)
	unblock()
	_, err = ticket.Wait()
	require.NoError(t, err)
	require.Zero(t, st.IODiskStats().Writes.Bytes)
	ticket, err = st.schedulers[1].Submit(iosched.WriteOp(f, make([]byte, 8), 8))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.NoError(t, err, "returned capacity belongs to either write ring")
}
