package blobcache

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/nvme"
	"github.com/stretchr/testify/require"
)

func TestNewRejectsInvalidOptions(t *testing.T) {
	device := testDevice(t, 64<<20)
	for _, opts := range [][]Option{
		{WithMemory(-1)}, {WithMemory(0)}, {WithMemory(minMemory - 1)}, {WithInlineThreshold(-1)},
		{WithReadSchedulers(0)}, {WithWriteSchedulers(0)},
	} {
		c, err := New(device, opts...)
		require.Error(t, err)
		require.Nil(t, c)
	}
}

func TestCacheMultipleSchedulers(t *testing.T) {
	for _, writes := range []int{1, 2} {
		t.Run(fmt.Sprint(writes), func(t *testing.T) {
			device := testDevice(t, 64<<20)
			c := newPutCache(t, device, WithReadSchedulers(3), WithWriteSchedulers(writes))
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
			c = newPutCache(t, device, WithReadSchedulers(2))
			checkRead(t, c, []byte("key"), []byte("new"))
			checkRead(t, c, []byte("external"), value)
		})
	}
}

// TestOpenExistingRange opens the range a previous run left, without
// formatting, and reports what recovery found. It runs only with
// BLOBCACHE_NVME_OPEN_EXISTING=1 and BLOBCACHE_NVME_{DEVICE,OFFSET,SIZE}.
func TestOpenExistingRange(t *testing.T) {
	if os.Getenv("BLOBCACHE_NVME_OPEN_EXISTING") != "1" {
		t.Skip("set BLOBCACHE_NVME_OPEN_EXISTING=1 to open an existing range")
	}
	env := func(name string) uint64 {
		v, err := strconv.ParseUint(os.Getenv(name), 10, 64)
		require.NoError(t, err, name)
		return v
	}
	device, err := nvme.Open(os.Getenv("BLOBCACHE_NVME_DEVICE"), nvme.WithRange(env("BLOBCACHE_NVME_OFFSET"), env("BLOBCACHE_NVME_SIZE")))
	require.NoError(t, err)
	defer device.Close()
	start := time.Now()
	c, err := New(device, WithMemory(1<<30), WithSealInterval(0))
	require.NoError(t, err)
	elapsed := time.Since(start)
	var records int
	var payload uint64
	for h := range c.store.Records() {
		records++
		payload += h.PhysicalLen
	}
	t.Logf("RECOVERY: elapsed=%s records=%d keys=%d payload=%.1fGiB live-window=%.1fGiB",
		elapsed, records, c.values.entries.Len(), float64(payload)/(1<<30), float64(c.store.Size())/(1<<30))
	require.NoError(t, c.Close())
}
