package blobstore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

func TestSmallWriteAdmissionKeepsLargeWritesRejected(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	for _, inline := range []int64{0, 32 << 10} {
		t.Run(fmt.Sprint(inline), func(t *testing.T) {
			policy := iosched.NewDiskPolicy(iosched.WithWriteBandwidth(64<<10),
				iosched.WithWriteBacklogLimit(time.Millisecond), iosched.WithWriteBacklogRecovery(0))
			st := openTestStore(t, t.TempDir(), WithInlineThreshold(inline), WithSmallWriteAdmission(32<<10),
				WithDiskPolicy(policy), WithSchedulerCPUs(-1, -1), WithWriteSchedulers(1))
			for _, size := range []int{256 << 10, 16 << 10, 64 << 10} {
				w, err := st.NewWriter()
				require.NoError(t, err)
				value := bytes.Repeat([]byte{byte(size >> 10)}, size)
				_, err = w.Write(value)
				require.NoError(t, err)
				key := []byte(fmt.Sprint(size))
				err = putStoreWriter(st, key, w)
				if size == 64<<10 {
					require.ErrorIs(t, err, ErrRejected)
				} else {
					require.NoError(t, err)
					checkWriterRecord(t, st, w, key, value)
				}
				w.Discard()
				require.ErrorIs(t, policy.CanAdmit(iosched.OpWrite), iosched.ErrRejected)
			}
		})
	}
}

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
		WithInlineThreshold(16 << 10),
		WithDataIntegrity(DataIntegrityCRC32C), WithVerifyChecksums(true)}
	st, err := Open(path, pool, append(opts, extra...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

// Different rings may use the same VFD number concurrently. Object I/O
// and linked open/read/close must remain on the chosen scheduler.
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
				w, err := st.NewWriter()
				require.NoError(t, err)
				t.Cleanup(func() { w.Discard() })
				writers[i] = w
				_, err = w.Write(bytes.Repeat([]byte{byte(i)}, 40000))
				require.NoError(t, err)
				require.Zero(t, w.header.ObjectID, "writes do not reserve storage")
			}
			// Abandon one buffered record while peers on every ring remain usable.
			writers[0].Discard()
			require.Empty(t, indexReservations(st.mu.index), "discard before Put requires no disk cleanup")
			var headers []RecordHeader
			for i := len(writers) - 1; i > 0; i-- {
				w := writers[i]
				err := putStoreWriter(st, []byte(fmt.Sprint(i)), w)
				require.NoError(t, err)

				require.NotNil(t, w.buf, "Put does not release the writer")
				headers = append(headers, w.header)
				w.Discard()
			}
			// Every read ring can read an object written by any write ring.
			counts := make([]*countingReadScheduler, len(st.schedulers))
			for i, scheduler := range st.schedulers {
				counts[i] = &countingReadScheduler{Scheduler: scheduler}
				st.schedulers[i] = counts[i]
			}
			var readers []*Reader
			for i := 0; i < 2*(tc.rings-tc.writes); i++ {
				selected := counts[tc.writes+i%(tc.rings-tc.writes)]
				before := selected.submissions
				r, err := st.NewReader([]byte("1"), writers[1].header)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, r.Close()) })
				require.Equal(t, before+1, selected.submissions)
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
					i := uint64(len(writers)) - (h.ObjectID - headers[0].ObjectID) - 1
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
	w, err := st.NewWriter()
	require.NoError(t, err)
	_, err = w.Write([]byte("next"))
	require.NoError(t, err)
	written := make(chan struct{})
	var writeErr error
	go func() { writeErr = putStoreWriter(st, []byte("pending"), w); close(written) }()
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

	w.Discard()
}

// A fragmented frame is accepted or rejected as one submission, including
// its external open and close. Rejection must leave no disk I/O behind.
func TestPutUsesOneLinkedSubmission(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
			w, err := st.NewWriter()
			require.NoError(t, err)
			defer w.Discard()
			gap, err := st.memory.TryAcquire(align.BlockSize)
			require.NoError(t, err)
			defer gap.Unpin()
			value := make([]byte, 256<<10)
			_, err = w.Write(value)
			require.NoError(t, err)
			h := sealWriter(t, []byte("linked"), w)
			buffer := w.Buffer()
			require.Greater(t, len(w.buf.RawAllocations()), 1)
			base := iosched.Scheduler(st.schedulers[0])
			if reject {
				r := rejectTestIO(st)
				r.reject = ErrRejected
				base = r
			}
			count := &countingReadScheduler{Scheduler: base}
			st.schedulers[0] = count
			completed := 0
			var completeErr error
			ticket, err := st.Put(w, func(err error) {
				completed++
				completeErr = err
				w.Discard()
			})
			if reject {
				require.ErrorIs(t, err, ErrRejected)
				require.ErrorIs(t, ticket.Wait(), ErrInvalidTicket)
				require.ErrorIs(t, completeErr, ErrRejected)
				require.NoFileExists(t, st.objectPath(h.ObjectID))
			} else {
				require.NoError(t, err)
				require.NoError(t, ticket.Wait())
				require.NoError(t, completeErr)
				checkWriterRecord(t, st, w, []byte("linked"), value)
			}
			require.Equal(t, 1, count.submissions)
			require.Equal(t, 1, completed)
			require.Empty(t, buffer.First())
			require.Equal(t, st.memory.Capacity()-align.BlockSize, st.memory.Available())
		})
	}
}

func TestLinkedWriteFailureClosesVFD(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	require.NoError(t, st.schedulers[0].Close())
	st.schedulers[0] = iosched.NewDefaultScheduler(iosched.WithVFiles(3), iosched.WithFiles(st.data, st.readData))
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write(make([]byte, 256<<10))
	require.NoError(t, err)
	h := sealWriter(t, []byte("full"), w)
	require.NoError(t, os.Symlink("/dev/full", st.objectPath(h.ObjectID)))
	ticket, err := st.Put(w, nil)
	require.NoError(t, err)
	require.ErrorIs(t, ticket.Wait(), unix.ENOSPC)
	// Only one external slot exists. A cancelled close would leak it and make
	// the next open fail with ENFILE instead of completing the new write.
	value := []byte("reused after write failure")
	next := publishForRead(t, st, []byte("next"), value)
	readReclaimRecord(t, st, "next", next, value)
}

func TestReadDescriptorsAndBufferedReads(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	for _, buffered := range []bool{false, true} {
		for _, writes := range []int{0, 1} {
			t.Run(fmt.Sprintf("buffered=%t/writes=%d", buffered, writes), func(t *testing.T) {
				st := openTestStore(t, t.TempDir(), TestingWithDirectIO(true), WithBufferedReads(buffered), WithVerifyChecksums(true), WithSchedulerCPUs(-1, -1), WithWriteSchedulers(writes))
				flags, err := unix.FcntlInt(st.data.Fd(), unix.F_GETFL, 0)
				require.NoError(t, err)
				require.Equal(t, unix.O_RDWR, flags&unix.O_ACCMODE)
				require.NotZero(t, flags&unix.O_DIRECT, "writes stay direct")
				flags, err = unix.FcntlInt(st.readData.Fd(), unix.F_GETFL, 0)
				require.NoError(t, err)
				require.Equal(t, unix.O_RDONLY, flags&unix.O_ACCMODE)
				require.Equal(t, !buffered, flags&unix.O_DIRECT != 0)
				for _, size := range []int{13000, 70000} {
					key := []byte(fmt.Sprint(size))
					value := bytes.Repeat([]byte{byte(size)}, size)
					h := publishForRead(t, st, key, value)
					r, err := st.NewReader(key, h)
					require.NoError(t, err)
					vfd := uint32(dataReadVFD)
					if h.Flags.IsExternal() {
						vfd = iosched.AllocateVFD
					}
					probe := iosched.VWriteOp(vfd, []byte("x"), 0)
					if buffered {
						probe = iosched.VReadOp(vfd, make([]byte, 7), 0).HardLink(probe)
					}
					if h.Flags.IsExternal() {
						probe = st.openObject(h.ObjectID, os.O_RDONLY).Link(probe.HardLink(iosched.VCloseOp(vfd)))
					}
					ticket, err := st.readScheduler().Submit(probe)
					require.NoError(t, err)
					_, err = ticket.Wait()
					require.ErrorIs(t, err, unix.EBADF, "read descriptors cannot write")
					if buffered {
						index := 0
						if h.Flags.IsExternal() {
							index = 1
						}
						require.Equal(t, 7, ticket.AllResults()[index].N)
					}

					got, err := io.ReadAll(r)
					require.NoError(t, err)
					require.Equal(t, value, got)
					require.NoError(t, r.Close())
				}
			})
		}
	}
}
