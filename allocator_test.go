package blobcache

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestMemoryReclaimCountsOnlyReleasedBytes(t *testing.T) {
	const page = int64(align.BlockSize)
	pool, err := alloc.NewSlabAllocator(9 * page)
	require.NoError(t, err)
	defer pool.Close()
	m := newMemoryCache()
	defer m.clear()
	var pinned []*alloc.Buffer
	defer func() {
		for _, b := range pinned {
			if b != nil {
				b.Unpin()
			}
		}
	}()
	for i, pages := range []int64{4, 1, 2, 1, 1} {
		b, err := pool.TryAcquire(pages * page)
		require.NoError(t, err)
		if i < 2 {
			require.True(t, b.TryInc())
			pinned = append(pinned, b)
		}
		// Keep order deterministic in one shard. The logical value is smaller
		// than its reservation: reclamation must count allocated bytes.
		m.replace(xmap.Key{Lo: uint64(i * m.entries.ShardCount())}, b,
			blobstore.RecordHeader{PhysicalLen: 1})
	}
	require.Zero(t, pool.Available())
	require.Zero(t, m.reclaimCacheMemory(0))
	require.Zero(t, m.reclaimCacheMemory(-1))
	require.Equal(t, int64(3*page), m.reclaimCacheMemory(3*page), "pinned victims contribute no reclaimed bytes")
	require.Equal(t, int64(3*page), pool.Available())
	s := m.entries.Shard(xmap.Key{})
	require.Equal(t, 1, s.Extra.list.Len(), "stop after reaching the byte target")
	require.Equal(t, 5, m.entries.Len(), "all disk metadata survives")
	for i, b := range pinned {
		require.True(t, b.Unpin(), "borrower's late release makes its allocation reusable")
		pinned[i] = nil
	}
	require.Equal(t, int64(8*page), pool.Available())
	require.Equal(t, page, m.reclaimCacheMemory(1), "whole buffers can overshoot the target")
	require.Zero(t, m.reclaimCacheMemory(page))
	require.Equal(t, pool.Capacity(), pool.Available())
}

func TestCacheAllocatorHeadroom(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	for i := range 32 {
		// Fill without proactive requests to exercise required reclamation.
		buffer, err := c.memory.tryAcquire(align.BlockSize)
		require.NoError(t, err)
		c.values.replace(xmap.Key{Lo: uint64(i)}, buffer, blobstore.RecordHeader{})
	}
	require.Zero(t, c.memory.Available())
	key := []byte("next")
	size := int64(2*align.BlockSize - blobstore.RecordHeaderSize - len(key))
	first, err := c.NewWriter(key, size)
	require.NoError(t, err)
	defer first.Close()
	residents := residentCount(c)
	available := c.memory.Available()
	require.Less(t, residents, 32)
	require.GreaterOrEqual(t, available, int64(2*align.BlockSize))
	second, err := c.NewWriter(key, size)
	require.NoError(t, err)
	defer second.Close()
	require.Equal(t, residents, residentCount(c), "the next request uses the reclaimed headroom")
	require.Equal(t, available-2*align.BlockSize, c.memory.Available())
	require.Equal(t, 32, c.values.entries.Len())
}

func TestCacheAllocatorOversizedRequest(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprint(external), func(t *testing.T) {
			c := newPutCache(t, t.TempDir(), WithInlineThreshold(1<<20), WithStagingSize(128<<10))
			putValue(t, c, "retained", []byte("v"))
			size := int64(128 << 10) // Framing puts this over the total capacity.
			if external {
				size = SizeUnknown // The staging allocation includes a header page.
			}
			w, err := c.NewWriter([]byte("too big"), size)
			require.ErrorIs(t, err, ErrBusy)
			require.Nil(t, w)
		})
	}
}

func TestCacheAllocatorPinnedExhaustionAndLateRelease(t *testing.T) {
	const capacity = int64(3 * align.BlockSize)
	for _, kind := range []string{"slab", "extent"} {
		t.Run(kind, func(t *testing.T) {
			var pool alloc.Allocator
			var err error
			if kind == "slab" {
				pool, err = alloc.NewSlabAllocator(capacity, alloc.WithChunkSize(capacity))
			} else {
				pool, err = alloc.NewExtentAllocator(capacity, 1)
			}
			require.NoError(t, err)
			defer pool.Close()
			m := newMemoryCache()
			defer m.clear()
			a := &cacheAllocator{Allocator: pool, capacity: capacity, reclaim: func(size int64, bestEffort bool) {
				if !bestEffort {
					m.reclaimCacheMemory(size)
				}
			}}
			b, err := a.TryAcquire(1)
			require.NoError(t, err)
			require.Equal(t, capacity, b.Cap(), "the allocation is larger than the request")
			require.Zero(t, a.Available(), "charge the buffer's entire capacity")
			require.True(t, b.TryInc())
			m.replace(xmap.Key{}, b, blobstore.RecordHeader{PhysicalLen: 1})
			_, err = a.TryAcquire(1)
			require.ErrorIs(t, err, alloc.ErrExhausted)
			require.Zero(t, a.Available(), "dropping a cache reference did not release pinned storage")
			e, ok := m.entries.Get(xmap.Key{})
			require.True(t, ok)
			require.Zero(t, e.resident)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = a.Acquire(ctx, 1)
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, b.Unpin())
			require.Equal(t, capacity, a.Available())
			b, err = a.Acquire(context.Background(), 1)
			require.NoError(t, err)
			require.Zero(t, a.Available())
			require.True(t, b.Unpin())
			require.Equal(t, capacity, a.Available())
		})
	}
}

func TestCacheAllocatorReclaimCallback(t *testing.T) {
	const page = int64(align.BlockSize)
	pool, err := alloc.NewSlabAllocator(8 * page)
	require.NoError(t, err)
	defer pool.Close()
	a := &cacheAllocator{Allocator: pool, capacity: pool.Capacity()}
	var held *alloc.Buffer
	defer func() {
		if held != nil {
			held.Unpin()
		}
	}()
	var targets []int64
	a.reclaim = func(size int64, bestEffort bool) {
		require.False(t, bestEffort)
		targets = append(targets, size)
		require.Equal(t, 2*page, a.Available(), "the old buffer remains charged when reclamation starts")
		require.True(t, held.Unpin())
		held = nil
	}
	held, err = a.TryAcquire(6*page - 7)
	require.NoError(t, err)
	require.Equal(t, 2*page, a.Available(), "count chunk rounding, not the requested bytes")
	b, err := a.TryAcquire(2*page + 1)
	require.NoError(t, err)
	require.Equal(t, []int64{3 * page}, targets, "the callback receives the pending allocation size")
	require.Equal(t, 5*page, a.Available(), "the successful retry must also be counted")
	require.True(t, b.Unpin())
	require.Equal(t, a.Capacity(), a.Available())
	require.Equal(t, pool.Capacity(), pool.Available(), "the original release callback returned the chunks")
}

func TestCacheAllocatorConcurrentAccounting(t *testing.T) {
	pool, err := alloc.NewSlabAllocator(128 * align.BlockSize)
	require.NoError(t, err)
	defer pool.Close()
	a := &cacheAllocator{Allocator: pool, capacity: pool.Capacity(), reclaim: func(int64, bool) {
		t.Error("the pool has room for every concurrent allocation")
	}}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 100 {
				b, err := a.TryAcquire(int64(i%3*align.BlockSize + 1))
				if err != nil {
					t.Error(err)
					return
				}
				if !b.TryInc() {
					t.Error("new buffer cannot be pinned")
					return
				}
				if b.Unpin() {
					t.Error("the buffer still has another reference")
				}
				available := a.Available()
				if available < 0 || available > a.Capacity() || available%align.BlockSize != 0 {
					t.Errorf("invalid availability: %d", available)
				}
				if !b.Unpin() {
					t.Error("the final reference did not release storage")
				}
			}
		})
	}
	wg.Wait()
	require.Equal(t, a.Capacity(), a.Available())
	require.Equal(t, pool.Capacity(), pool.Available())
}
