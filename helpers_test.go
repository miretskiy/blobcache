package blobcache

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/nvme"
	"github.com/stretchr/testify/require"
)

var testOffset atomic.Uint64

// testDevice formats a fresh size-byte slice of the dedicated test range named
// by BLOBCACHE_NVME_DEVICE, BLOBCACHE_NVME_OFFSET, and BLOBCACHE_NVME_SIZE,
// using its second half so blobstore's tests can run concurrently in the
// first. Tests skip unless BLOBCACHE_NVME_WRITE=1.
func testDevice(t testing.TB, size uint64) *nvme.Device {
	t.Helper()
	if os.Getenv("BLOBCACHE_NVME_WRITE") != "1" {
		t.Skip("set BLOBCACHE_NVME_WRITE=1 and BLOBCACHE_NVME_{DEVICE,OFFSET,SIZE} to a dedicated range")
	}
	env := func(name string) uint64 {
		v, err := strconv.ParseUint(os.Getenv(name), 10, 64)
		require.NoError(t, err, name)
		return v
	}
	base, total := env("BLOBCACHE_NVME_OFFSET"), env("BLOBCACHE_NVME_SIZE")
	half := total / 2 &^ (1<<20 - 1)
	start := half + testOffset.Add(size) - size
	require.LessOrEqual(t, start+size, total, "test range exhausted")
	device, err := nvme.Open(os.Getenv("BLOBCACHE_NVME_DEVICE"), nvme.WithRange(base+start, size))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, device.Close()) })
	if serial := os.Getenv("BLOBCACHE_NVME_SERIAL"); serial != "" {
		require.Equal(t, serial, device.Info().Serial)
	}
	require.NoError(t, Format(device))
	return device
}

func newPutCache(t *testing.T, device *nvme.Device, opts ...Option) *Cache {
	t.Helper()
	c, err := New(device, append([]Option{
		WithMemory(minMemory), WithInlineThreshold(32 << 10), WithSealInterval(0),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// putValue publishes value and waits until the cache owns any resident copy,
// so callers can inspect memory or force eviction.
func putValue(t *testing.T, c *Cache, key string, value []byte) {
	t.Helper()
	w, err := c.NewWriter()
	require.NoError(t, err)
	n, err := w.Write(value)
	require.NoError(t, err)
	require.Equal(t, len(value), n)
	ticket, err := c.Put([]byte(key), w)
	require.NoError(t, err)
	require.NoError(t, ticket.Wait())
	hash := blobstore.HashKey([]byte(key))
	require.Eventually(t, func() bool {
		s := c.values.entries.Shard(hash)
		s.RLock()
		defer s.RUnlock()
		e, ok := s.Items[hash]
		return !ok || e.resident == 0 || !s.Extra.list.Value(e.resident).pending
	}, 5*time.Second, time.Millisecond)
}

// overwrite puts fillers until the log has overwritten key's record.
func overwrite(t *testing.T, c *Cache, key string) {
	t.Helper()
	_, h, ok := c.values.lookup(blobstore.HashKey([]byte(key)))
	require.True(t, ok)
	for i := 0; c.store.Live(h); i++ {
		putValue(t, c, fmt.Sprintf("filler-%d", i), bytes.Repeat([]byte("f"), 16<<10))
	}
}

// checkRead reads key's whole value, mixing Read and WriteTo on one reader.
func checkRead(t *testing.T, c *Cache, key, value []byte) {
	t.Helper()
	r, err := c.Read(key)
	require.NoError(t, err)
	defer r.Close()
	require.Equal(t, int64(len(value)), r.Size())
	var got bytes.Buffer
	prefix := make([]byte, min(7, len(value)))
	_, err = io.ReadFull(r, prefix)
	require.NoError(t, err)
	got.Write(prefix)
	n, err := io.Copy(&got, r)
	require.NoError(t, err)
	require.Equal(t, int64(len(value)-len(prefix)), n)
	require.True(t, bytes.Equal(value, got.Bytes()), "payload mismatch")
	_, err = r.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}

func requireMiss(t *testing.T, c *Cache, key []byte) {
	t.Helper()
	r, err := c.Read(key)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
}

// retainedValue returns the value bytes of key's resident frame.
func retainedValue(t *testing.T, c *Cache, key []byte) []byte {
	t.Helper()
	s := c.values.entries.Shard(blobstore.HashKey(key))
	s.RLock()
	defer s.RUnlock()
	e, ok := s.Items[blobstore.HashKey(key)]
	require.True(t, ok)
	require.NotZero(t, e.resident)
	buffer := s.Extra.list.Value(e.resident).buffer
	var data []byte
	view := buffer.View(0, buffer.Cap())
	for chunk, err := view.Next(); err == nil; chunk, err = view.Next() {
		data = append(data, chunk...)
	}
	require.Equal(t, key, data[e.header.KeyOffset():][:len(key)])
	return data[e.header.ValueOffset():][:e.header.PhysicalLen]
}

// residentBuffer returns key's resident buffer without taking a reference, so
// it cannot mask an erroneous retain or release.
func residentBuffer(t *testing.T, c *Cache, key []byte) *alloc.ReadOnlyBuffer {
	t.Helper()
	s := c.values.entries.Shard(blobstore.HashKey(key))
	s.RLock()
	defer s.RUnlock()
	e := s.Items[blobstore.HashKey(key)]
	require.NotZero(t, e.resident)
	return s.Extra.list.Value(e.resident).buffer
}

func residentCount(c *Cache) int {
	count := 0
	for i := range c.values.entries.ShardCount() {
		s := c.values.entries.ShardAt(i)
		s.RLock()
		count += s.Extra.list.Len()
		s.RUnlock()
	}
	return count
}

func used(c *Cache) int64 { return c.memory.Capacity() - c.memory.Available() }

func newTestPool(t testing.TB, size int64) *alloc.Allocator {
	a := alloc.NewAllocator(size)
	t.Cleanup(a.Close)
	return a
}

func acquire(t testing.TB, a *alloc.Allocator, n int64) *alloc.ReadOnlyBuffer {
	t.Helper()
	b := alloc.NewAllocatingBuffer(a)
	require.NoError(t, b.Grow(int(n)))
	return b.Freeze()
}

func newTestMemory(t *testing.T, size int64) *alloc.ReadOnlyBuffer {
	return acquire(t, newTestPool(t, size), size)
}

func memorySize(b *alloc.ReadOnlyBuffer) int64 { return int64(b.Cap()) }
func unpinMemory(b *alloc.ReadOnlyBuffer) bool { return b.Release() == int64(b.Cap()) }
