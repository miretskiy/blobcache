package blobcache

import (
	"bytes"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestCacheMultipleSchedulers(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	for _, writes := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(writes), func(t *testing.T) {
			dir := t.TempDir()
			c := newPutCache(t, dir, WithSchedulerCPUs(-1, -1, -1, -1),
				WithWriteSchedulers(writes), WithVerifyChecksums(true))
			// Publishing an older writer last must not replace newer metadata,
			// including when the writers use different rings.
			old, err := c.NewWriter([]byte("key"), 3)
			require.NoError(t, err)
			defer old.Close()
			_, err = old.Write([]byte("old"))
			require.NoError(t, err)
			putValue(t, c, "key", []byte("new"))
			require.NoError(t, c.Put(old))
			require.NoError(t, old.Close())
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
	c := newPutCache(t, dir, WithMemory(16<<20), WithRejectReadBacklog(1))
	putValue(t, c, "held", make([]byte, 40000))
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
		_, err := signal.Write(framed[:8192])
		require.NoError(t, err)
		require.NoError(t, <-done)
		c.store.Wait()
		require.Zero(t, c.memory.used.Load())
		require.Zero(t, c.IODiskStats().Reads.Bytes)
	}()
	require.Eventually(t, func() bool { return c.IODiskStats().Reads.Bytes > 0 }, time.Second, time.Millisecond)
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
	heldMemory := c.memory.used.Load()
	for _, size := range []int{100, 40000} {
		reader, err := c.Read([]byte(fmt.Sprint(size)))
		require.ErrorIs(t, err, ErrRejected)
		require.Nil(t, reader)
		require.Equal(t, heldMemory, c.memory.used.Load(), "rejected readers return their staging memory")
	}
}
