package blobcache

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestBenchmarkPreflightRejectsOnlyExternalValues(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	policy := iosched.NewDiskPolicy(iosched.WithWriteBandwidth(64<<10), iosched.WithoutReadIOBudget(),
		iosched.WithWriteBacklogLimit(time.Millisecond), iosched.WithWriteBacklogRecovery(0))
	c := newPutCache(t, t.TempDir(), WithMemory(16<<20), WithDiskPolicy(policy),
		WithInlineThreshold(benchmarkInlineSize),
		WithSchedulerCPUs(-1, -1), WithWriteSchedulers(1), WithSmallWriteAdmission(64<<10))
	putValue(t, c, "close-admission", make([]byte, benchmarkInlineSize))
	require.ErrorIs(t, policy.CanAdmit(iosched.OpWrite), ErrRejected)
	c.values.reclaimCacheMemory(c.memory.Capacity())
	held := alloc.NewAllocatingBuffer(c.memory)
	require.NoError(t, held.Grow(int(c.memory.Capacity())))
	defer held.Release()
	w := benchmarkWorkload{cache: c, disk: policy, data: make([]byte, benchmarkInlineSize+1),
		config: benchmarkConfig{smallWriteAdmission: 64 << 10}, keys: make([]atomic.Uint32, 3)}
	large := benchmarkRequest{workload: &w}
	large.write(benchmarkJob{id: 1, size: benchmarkInlineSize + 1}, time.Now())
	require.EqualValues(t, 1, large.stats.preflightRejected)
	require.EqualValues(t, 1, large.stats.rejectedWrites)
	require.Zero(t, large.stats.busyWrites, "admission is checked before the exhausted allocator")
	require.Zero(t, large.stats.copiedBytes)
	held.Release()
	inline := benchmarkRequest{workload: &w}
	inline.write(benchmarkJob{id: 2, size: benchmarkInlineSize}, time.Now())
	require.Zero(t, inline.stats.preflightRejected)
	require.EqualValues(t, benchmarkInlineSize, inline.stats.copiedBytes)
	require.EqualValues(t, 1, inline.stats.rejectedWrites, "the disk write still observes admission")
	checkRead(t, c, []byte("key-2"), w.data[:benchmarkInlineSize])

	small := benchmarkRequest{workload: &w}
	small.write(benchmarkJob{id: 3, size: 8192}, time.Now())
	require.True(t, small.stats.writes.recorded)
	require.EqualValues(t, 8192, small.stats.copiedBytes)
	require.Zero(t, small.stats.preflightRejected)
	require.ErrorIs(t, policy.CanAdmit(iosched.OpWrite), ErrRejected)
}

func TestCacheMultipleSchedulers(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	for _, writes := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(writes), func(t *testing.T) {
			dir := t.TempDir()
			c := newPutCache(t, dir, WithSchedulerCPUs(-1, -1, -1, -1),
				WithWriteSchedulers(writes), WithVerifyChecksums(true))
			// Replacement remains readable across write and read rings.
			old, err := c.NewWriter()
			require.NoError(t, err)

			_, err = old.Write([]byte("old"))
			require.NoError(t, err)

			_, err = c.Put([]byte("key"), old)
			require.NoError(t, err)
			putValue(t, c, "key", []byte("new"))

			held, err := c.Read([]byte("key"))
			require.NoError(t, err)
			defer held.Close()
			value := bytes.Repeat([]byte("object"), 7000)
			putValue(t, c, "external", value)
			c.values.reclaimCacheMemory(c.memory.Capacity())
			require.Zero(t, residentCount(c))
			for range 8 {
				checkRead(t, c, []byte("key"), []byte("new"))
				checkRead(t, c, []byte("external"), value)
			}
			got, err := io.ReadAll(held)
			require.NoError(t, err)
			require.Equal(t, []byte("new"), got, "resident reader keeps its pin across demotion and disk reads")
			require.NoError(t, held.Close())
			require.NoError(t, c.Close())
			c = newPutCache(t, dir, WithSchedulerCPUs(-1, -1), WithVerifyChecksums(true))
			checkRead(t, c, []byte("key"), []byte("new"))
			checkRead(t, c, []byte("external"), value)
		})
	}
}

func TestCacheReadRejectionReleasesBuffers(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	dir := t.TempDir()
	c := newPutCache(t, dir, WithMemory(16<<20), WithInlineThreshold(0), WithReadBacklogLimit(1))
	putValue(t, c, "held", make([]byte, 100))
	objects, err := filepath.Glob(filepath.Join(dir, "objects", "*"))
	require.NoError(t, err)
	require.Len(t, objects, 1)
	framed, err := os.ReadFile(objects[0])
	require.NoError(t, err)
	require.NoError(t, os.Remove(objects[0]))
	require.NoError(t, unix.Mkfifo(objects[0], 0600))
	signal, err := os.OpenFile(objects[0], os.O_RDWR, 0)
	require.NoError(t, err)
	defer signal.Close()
	for _, size := range []int{100, 40000} {
		putValue(t, c, fmt.Sprint(size), make([]byte, size))
	}
	c.values.reclaimCacheMemory(c.memory.Capacity())
	// Hold a real external read in the kernel so the coordinator can close read
	// admission. The first request at idle is allowed to exceed the threshold.
	done := make(chan error, 1)
	go func() {
		reader, err := c.Read([]byte("held"))
		if err == nil {
			err = reader.Close()
		}
		done <- err
	}()
	defer func() {
		_, err := signal.Write(framed)
		require.NoError(t, err)
		require.NoError(t, <-done)
		require.Eventually(t, func() bool { return c.memory.Available() == c.memory.Capacity() && c.IODiskStats().Reads.Backlog == 0 }, time.Second, time.Millisecond, "I/O completion releases the closed reader's buffer and admission charge")
	}()
	require.Eventually(t, func() bool { return c.IODiskStats().Reads.Backlog > 0 }, time.Second, time.Millisecond)
	// Observing the counters may precede publication of the rejection token.
	require.Eventually(t, func() bool {
		reader, err := c.Read([]byte("100"))
		if err == nil {
			require.NoError(t, reader.Close())
			return false
		}
		require.ErrorIs(t, err, ErrRejected)
		return true
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return c.memory.Capacity()-c.memory.Available() == int64(len(framed)) }, time.Second, time.Millisecond, "completed incidental reads release their buffers")
	heldMemory := (c.memory.Capacity() - c.memory.Available())
	for _, size := range []int{100, 40000} {
		reader, err := c.Read([]byte(fmt.Sprint(size)))
		require.ErrorIs(t, err, ErrRejected)
		require.Nil(t, reader)
		require.Equal(t, heldMemory, (c.memory.Capacity() - c.memory.Available()), "rejected readers retain no buffer")
	}
}
