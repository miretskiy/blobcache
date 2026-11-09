package blobcache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"

	"github.com/miretskiy/dio/v2/collections"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestMemoryShardAlignment(t *testing.T) {
	require.NoError(t, collections.VerifyAlignment[blobstore.KeyHash, memoryEntry, memoryQueue]())
}

func TestMemoryIgnoresOlderPublication(t *testing.T) {
	m := newMemoryCache()
	defer m.clear()
	key := blobstore.HashKey([]byte("key"))
	newer := newTestMemory(4096)
	m.replace(key, newer, blobstore.RecordHeader{ObjectID: 2}, false)
	older := newTestMemory(4096)
	m.replace(key, older, blobstore.RecordHeader{ObjectID: 1}, false)
	require.False(t, older.TryInc(), "discard the older completion's reference")
	buffer, header, ok := m.lookup(key)
	require.True(t, ok)
	require.Equal(t, newer, buffer)
	require.EqualValues(t, 2, header.ObjectID)
	buffer.Unpin()
}

func TestMemoryHandlesSurviveGrowthAndEviction(t *testing.T) {
	m := newMemoryCache()
	const count = 512
	pool := newTestPool(t, count*4096)
	defer m.clear()
	keys := make([]blobstore.KeyHash, count)
	buffers := make([]blobstore.Memory, count)
	for i := range keys {
		// Exercise repeated list growth within one shard.
		keys[i] = blobstore.KeyHash(i * m.entries.ShardCount())
		var err error
		buffers[i], err = pool.TryAcquire(4096)
		require.NoError(t, err)
		m.replace(keys[i], buffers[i], blobstore.RecordHeader{Offset: uint64(i * 4096)}, false)
	}
	s := m.entries.Shard(keys[0])
	for i, key := range keys {
		e := s.Items[key]
		require.Equal(t, uint64(i*4096), e.header.Offset)
		r := s.Extra.list.Value(e.resident)
		require.Equal(t, key, r.key)
		require.Equal(t, buffers[i].Size(), r.buffer.Size())
		require.Same(t, &singleMemoryChunk(buffers[i])[0], &singleMemoryChunk(r.buffer)[0])
	}
	old := s.Items[keys[0]].resident
	for range count {
		require.Equal(t, int64(4096), m.reclaimCacheMemory(4096))
	}
	require.Zero(t, m.reclaimCacheMemory(4096), "disk-only metadata is not an eviction candidate")
	require.Zero(t, s.Extra.list.Len())
	require.Zero(t, s.Extra.hand)
	for i, key := range keys {
		e, ok := s.Items[key]
		require.True(t, ok)
		require.Equal(t, uint64(i*4096), e.header.Offset)
		require.Zero(t, e.resident)
	}
	buffer, err := pool.TryAcquire(4096)
	require.NoError(t, err)
	m.replace(keys[0], buffer, blobstore.RecordHeader{Offset: 999}, false)
	require.NotEqual(t, old, s.Items[keys[0]].resident)
	_, ok := s.Extra.list.TryValue(old)
	require.False(t, ok, "reusing a slot does not resurrect an old handle")
	require.Equal(t, uint64(999), s.Items[keys[0]].header.Offset)
	require.Equal(t, 1, s.Extra.list.Len())
	require.Equal(t, count, m.entries.Len())
}

func TestMemoryConcurrentReplacementAndEviction(t *testing.T) {
	m := newMemoryCache()
	defer m.clear()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			buffer := newTestMemory(4096)
			defer buffer.Unpin()
			for i := range 256 {
				key := blobstore.KeyHash(i % 32)
				buffer.TryInc() // This goroutine owns a reference throughout.
				m.replace(key, buffer, blobstore.RecordHeader{Offset: uint64(worker * 4096)}, false)
				if pinned, _, _ := m.lookup(key); isResident(pinned) {
					pinned.Unpin()
				}
				if i%2 == 0 {
					m.reclaimCacheMemory(4096)
				}
			}
		})
	}
	wg.Wait()
	require.Equal(t, 32, m.entries.Len())
	for i := range m.entries.ShardCount() {
		s := m.entries.ShardAt(i)
		var residentBytes int64
		for handle, r := range s.Extra.list.All() {
			require.Equal(t, handle, s.Items[r.key].resident)
			residentBytes += r.buffer.Size()
		}
		require.Equal(t, residentBytes, s.Extra.residentBytes)
		for key, e := range s.Items {
			if e.resident != 0 {
				require.Equal(t, key, s.Extra.list.Value(e.resident).key)
			}
		}
	}
	m.reclaimCacheMemory(32 * 4096)
	require.Equal(t, 32, m.entries.Len())
}

func TestMemoryReclaimProportionalShares(t *testing.T) {
	const page = int64(4096)
	m := newMemoryCache()
	pool := newTestPool(t, 40*page)
	defer m.clear()
	for shard, pages := range []int{4, 8, 12, 16} {
		for i := range pages {
			b, err := pool.TryAcquire(page)
			require.NoError(t, err)
			m.replace(blobstore.KeyHash(i*m.entries.ShardCount()+shard), b, blobstore.RecordHeader{}, false)
		}
	}
	require.Equal(t, 10*page, m.reclaimCacheMemory(10*page))
	for shard, remaining := range []int64{3, 6, 9, 12} {
		require.Equal(t, remaining*page, m.entries.ShardAt(shard).Extra.residentBytes,
			"each shard contributes one quarter of its resident bytes")
	}
	require.Equal(t, 40, m.entries.Len())
	require.Equal(t, pool.Capacity()-30*page, pool.Available())
}

func TestMemoryReclaimRedistributesPinnedShare(t *testing.T) {
	const page = int64(4096)
	m := newMemoryCache()
	pool := newTestPool(t, 16*page)
	defer m.clear()
	var pinned []blobstore.Memory
	defer func() {
		for _, b := range pinned {
			b.Unpin()
		}
	}()
	for shard, pages := range []int{4, 12} {
		for i := range pages {
			b, err := pool.TryAcquire(page)
			require.NoError(t, err)
			if shard == 0 {
				require.True(t, b.TryInc())
				pinned = append(pinned, b)
			}
			m.replace(blobstore.KeyHash(i*m.entries.ShardCount()+shard), b, blobstore.RecordHeader{}, false)
		}
	}
	require.Equal(t, 8*page, m.reclaimCacheMemory(8*page))
	require.Zero(t, m.entries.ShardAt(0).Extra.residentBytes, "pinned victims were demoted")
	require.Equal(t, 4*page, m.entries.ShardAt(1).Extra.residentBytes,
		"the other shard supplies the pinned shard's unmet share")
	require.Equal(t, pool.Capacity()-8*page, pool.Available(), "only actual releases count")
	require.Equal(t, 16, m.entries.Len())
}

func TestStartCacheReclaimNeverBlocks(t *testing.T) {
	c := &Cache{reclaimRequests: make(chan int64, 1)}
	queued := make(chan struct{})
	go func() {
		for range 100 {
			c.startCacheReclaim(4096)
		}
		close(queued)
	}()
	select {
	case <-queued:
	case <-time.After(5 * time.Second):
		t.Fatal("reclamation requests blocked on a full queue")
	}
	require.EqualValues(t, 4096, <-c.reclaimRequests)
}

func TestCacheProactiveReclamation(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	for i := range 26 { // 26 of 32 pages crosses 80% usage.
		putValue(t, c, fmt.Sprint(i), []byte("v"))
	}
	require.Eventually(t, func() bool {
		return c.memory.Available() >= c.memory.Capacity()/4
	}, 5*time.Second, time.Millisecond, "reclamation starts without a failed allocation")
	require.Equal(t, 26, c.values.entries.Len(), "all disk metadata survives")
}

func TestPutConcurrentMemoryPressure(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir)
	const workers, iterations = 4, 40
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	for worker := range workers {
		group.Go(func() error {
			for i := range iterations {
				key := []byte(fmt.Sprintf("%d-%d", worker, i))
				var w *Writer
				var err error
				for {
					if err := ctx.Err(); err != nil {
						return err
					}
					w, err = c.NewWriter(key)
					if !errors.Is(err, ErrBusy) {
						break
					}
					runtime.Gosched()
				}
				if err != nil {
					return err
				}
				_, err = w.Write([]byte("v"))
				if err == nil {
					err = c.Put(w)
				}
				if err = errors.Join(err, w.Close()); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	require.Equal(t, workers*iterations, c.values.entries.Len())
	require.NoError(t, c.Close())
	require.Len(t, diskRecords(t, dir), workers*iterations)
}
