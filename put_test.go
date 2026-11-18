package blobcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestPutReadReopen publishes values at the frame's size boundaries, reads
// them from memory and from disk, and recovers them after Close.
func TestPutReadReopen(t *testing.T) {
	device := testDevice(t, 64<<20)
	c := newPutCache(t, device)
	type value struct {
		key, value []byte
		external   bool
	}
	pattern := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i*37 + i/4096)
		}
		return b
	}
	values := []value{
		{key: []byte("empty"), value: []byte{}},
		{key: []byte("exact page"), value: pattern(align.BlockSize - blobstore.RecordHeaderSize - 10)},
		{key: []byte("inline"), value: pattern(20000)},
		{key: bytes.Repeat([]byte("k"), MaxKeyLen), value: []byte("max key")},
		{key: []byte("external"), value: pattern(300001), external: true},
	}
	putValue(t, c, "inline", []byte("superseded"))
	for _, v := range values {
		w, err := c.NewWriter()
		require.NoError(t, err)
		split := len(v.value) / 2
		_, err = w.Write(v.value[:split])
		require.NoError(t, err)
		_, err = io.Copy(w, bytes.NewReader(v.value[split:]))
		require.NoError(t, err)
		key := bytes.Clone(v.key)
		ticket, err := c.Put(key, w)
		require.NoError(t, err)
		clear(key) // Put does not retain the caller's key.
		require.NoError(t, ticket.Wait())
		checkRead(t, c, v.key, v.value)
	}
	for _, v := range values {
		if v.external {
			e, _ := c.values.entries.Get(blobstore.HashKey(v.key))
			require.Zero(t, e.resident, "large values are read from disk")
		} else {
			require.Equal(t, v.value, retainedValue(t, c, v.key))
		}
	}

	// A failed download is discarded and leaves no record.
	w, err := c.NewWriter()
	require.NoError(t, err)
	failure := errors.New("download failed")
	_, err = io.Copy(w, io.MultiReader(bytes.NewReader([]byte("partial")), errorReader{failure}))
	require.ErrorIs(t, err, failure)
	w.Discard()

	require.Eventually(t, func() bool {
		c.values.reclaimCacheMemory(c.memory.Capacity())
		return residentCount(c) == 0
	}, 5*time.Second, time.Millisecond)
	for _, v := range values {
		checkRead(t, c, v.key, v.value)
	}
	require.Zero(t, residentCount(c), "disk reads do not repopulate memory")
	putValue(t, c, "inline", values[2].value) // Resident again, for Close to release.
	buffer := residentBuffer(t, c, []byte("inline"))
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	require.Empty(t, buffer.First(), "Close releases resident buffers")

	c = newPutCache(t, device)
	records := 0
	for range c.store.Records() {
		records++
	}
	require.Equal(t, len(values)+2, records, "both superseded records, and no discarded one")
	require.Equal(t, len(values), c.values.entries.Len(), "recovery keeps the newest record per key")
	var last uint64
	for h := range c.store.Records() {
		last = max(last, h.Position)
	}
	for _, v := range values {
		checkRead(t, c, v.key, v.value)
	}
	putValue(t, c, "next", []byte("next"))
	_, h, _ := c.values.lookup(blobstore.HashKey([]byte("next")))
	require.Greater(t, h.Position, last, "positions keep growing after reopen")
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestPutReplacementReleasesMemory(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	putValue(t, c, "key", []byte("first"))
	first := residentBuffer(t, c, []byte("key"))
	putValue(t, c, "key", []byte("second"))
	require.Empty(t, first.First())
	require.Equal(t, []byte("second"), retainedValue(t, c, []byte("key")))
	second := residentBuffer(t, c, []byte("key"))
	putValue(t, c, "key", make([]byte, 40000))
	require.Equal(t, 1, c.values.entries.Len(), "disk metadata stays indexed")
	require.Zero(t, residentCount(c), "a disk-only replacement drops the inline buffer")
	require.Empty(t, second.First())
	putValue(t, c, "key", []byte("inline again"))
	require.Equal(t, 1, residentCount(c))
	require.Equal(t, []byte("inline again"), retainedValue(t, c, []byte("key")))
}

func TestPutCompletionWithoutWaiting(t *testing.T) {
	for _, size := range []int{100, 40000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := newPutCache(t, testDevice(t, 64<<20))
			w, err := c.NewWriter()
			require.NoError(t, err)
			value := bytes.Repeat([]byte("v"), size)
			_, err = w.Write(value)
			require.NoError(t, err)
			_, err = c.Put([]byte("key"), w) // Intentionally drop the ticket.
			require.NoError(t, err)
			hash := blobstore.HashKey([]byte("key"))
			require.Eventually(t, func() bool {
				s := c.values.entries.Shard(hash)
				s.RLock()
				defer s.RUnlock()
				e, ok := s.Items[hash]
				if !ok {
					return false
				}
				if e.resident == 0 {
					return used(c) == 0
				}
				resident := s.Extra.list.Value(e.resident)
				return !resident.pending && used(c) == memorySize(resident.buffer)
			}, time.Second, time.Millisecond)
			checkRead(t, c, []byte("key"), value)
		})
	}
}

func TestPutInvalidKey(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	for _, key := range [][]byte{nil, make([]byte, MaxKeyLen+1)} {
		w, err := c.NewWriter()
		require.NoError(t, err)
		_, err = c.Put(key, w)
		require.Error(t, err)
	}
	require.Zero(t, used(c), "Put consumes the writer on failure")
}

func TestPutConcurrent(t *testing.T) {
	device := testDevice(t, 64<<20)
	c := newPutCache(t, device)
	const workers, iterations = 16, 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := range workers {
		wg.Go(func() {
			for iteration := range iterations {
				key := []byte(fmt.Sprintf("key-%d-%d", worker, iteration))
				if iteration%2 == 0 {
					key = []byte("shared")
				}
				w, err := c.NewWriter()
				for errors.Is(err, ErrBusy) {
					time.Sleep(time.Millisecond) // Reclamation runs in the background.
					w, err = c.NewWriter()
				}
				if err != nil {
					errs <- err
					return
				}
				if _, err = w.Write(bytes.Repeat([]byte{byte(worker)}, 5000)); err != nil {
					w.Discard()
					errs <- err
					return
				}
				if _, err = c.Put(key, w); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1+workers*iterations/2, c.values.entries.Len())
	require.NoError(t, c.Close())
	c = newPutCache(t, device)
	records := 0
	for range c.store.Records() {
		records++
	}
	require.Equal(t, workers*iterations, records, "Close waits for every write")
}

func TestPutConcurrentAcrossLogWraps(t *testing.T) {
	device := testDevice(t, 2<<20)
	c := newPutCache(t, device, WithInlineThreshold(0))
	group, ctx := errgroup.WithContext(context.Background())
	for worker := range 4 {
		group.Go(func() error {
			value := bytes.Repeat([]byte{byte(worker)}, 8<<10)
			for i := range 80 {
				if err := ctx.Err(); err != nil {
					return err
				}
				w, err := c.NewWriter()
				if err != nil {
					return err
				}
				if _, err = w.Write(value); err != nil {
					w.Discard()
					return err
				}
				ticket, err := c.Put([]byte(fmt.Sprintf("%d-%d", worker, i)), w)
				if err != nil {
					return err
				}
				if err := ticket.Wait(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	_, published := c.store.WriteStats()
	require.Greater(t, published, uint64(2<<20), "the workload wraps the log")
	require.NoError(t, c.Close())

	c = newPutCache(t, device, WithInlineThreshold(0))
	recovered := 0
	for h := range c.store.Records() {
		recovered++
		require.True(t, c.store.Live(h))
	}
	require.NotZero(t, recovered)
	read := 0
	for worker := range 4 {
		for i := range 80 {
			r, err := c.Read([]byte(fmt.Sprintf("%d-%d", worker, i)))
			if err != nil {
				require.ErrorIs(t, err, ErrNotFound)
				continue
			}
			got, err := io.ReadAll(r)
			require.NoError(t, r.Close())
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte{byte(worker)}, 8<<10), got)
			read++
		}
	}
	require.Equal(t, recovered, read, "every recovered record reads its own value")
}
