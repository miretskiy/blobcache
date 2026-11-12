package blobcache

import (
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestMemoryReclaimCountsOnlyReleasedBytes(t *testing.T) {
	const page = int64(align.BlockSize)
	pool := newTestPool(t, 9*page)
	m := newMemoryCache()
	defer m.clear()
	var pinned []*alloc.ReadOnlyBuffer
	defer func() {
		for _, b := range pinned {
			if b != nil {
				unpinMemory(b)
			}
		}
	}()
	for i, pages := range []int64{4, 1, 2, 1, 1} {
		b, err := pool.TryAcquire(pages * page)
		require.NoError(t, err)
		if i < 2 {
			b.Retain()
			pinned = append(pinned, b)
		}
		// Keep order deterministic in one shard. The logical value is smaller
		// than its reservation: reclamation must count allocated bytes.
		m.replace(blobstore.KeyHash(i*m.entries.ShardCount()), b,
			blobstore.RecordHeader{PhysicalLen: 1}, false)
	}
	require.Equal(t, pool.Capacity()-9*page, pool.Available())
	require.Zero(t, m.reclaimCacheMemory(0))
	require.Zero(t, m.reclaimCacheMemory(-1))
	require.Equal(t, int64(3*page), m.reclaimCacheMemory(3*page), "pinned victims contribute no reclaimed bytes")
	require.Equal(t, pool.Capacity()-6*page, pool.Available())
	s := m.entries.Shard(blobstore.KeyHash(0))
	require.Equal(t, 1, s.Extra.list.Len(), "stop after reaching the byte target")
	require.Equal(t, 5, m.entries.Len(), "all disk metadata survives")
	for i, b := range pinned {
		require.True(t, unpinMemory(b), "borrower's late release makes its allocation reusable")
		pinned[i] = nil
	}
	require.Equal(t, pool.Capacity()-page, pool.Available())
	require.Equal(t, page, m.reclaimCacheMemory(1), "whole buffers can overshoot the target")
	require.Zero(t, m.reclaimCacheMemory(page))
	require.Equal(t, pool.Capacity(), pool.Available())
}

func TestCacheAllocatorHeadroom(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	filled := 0
	for ; ; filled++ {
		// Fill without proactive requests to exercise reclamation on exhaustion.
		buffer := alloc.NewAllocatingBuffer(c.memory)
		err := buffer.Grow(align.BlockSize)
		if err != nil {
			require.ErrorIs(t, err, alloc.ErrExhausted)
			buffer.Release()
			break
		}
		c.values.replace(blobstore.KeyHash(filled), buffer.Freeze(), blobstore.RecordHeader{}, false)
	}
	_, err := c.NewWriter()
	require.ErrorIs(t, err, ErrBusy, "exhaustion does not wait for reclamation")
	require.Eventually(t, func() bool {
		return c.memory.Available() >= c.memory.Capacity()/4
	}, 5*time.Second, time.Millisecond, "exhaustion starts background reclamation")
	require.Less(t, residentCount(c), filled)
	first, err := c.NewWriter()
	require.NoError(t, err)
	defer first.Discard()
	second, err := c.NewWriter()
	require.NoError(t, err, "the next request uses the reclaimed headroom")
	defer second.Discard()
	require.Equal(t, filled, c.values.entries.Len())
}
