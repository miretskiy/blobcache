package blobcache

import (
	"sync"
	"testing"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestMemoryIgnoresOlderPublication(t *testing.T) {
	m := newMemoryCache()
	defer m.clear()
	key := blobstore.HashKey([]byte("key"))
	newer := alloc.NewBuffer(4096)
	m.replace(key, newer, blobstore.RecordHeader{ObjectID: 2})
	older := alloc.NewBuffer(4096)
	m.replace(key, older, blobstore.RecordHeader{ObjectID: 1})
	require.False(t, older.TryInc(), "discard the older completion's reference")
	buffer, header, ok := m.lookup(key)
	require.True(t, ok)
	require.Same(t, newer, buffer)
	require.EqualValues(t, 2, header.ObjectID)
	buffer.Unpin()
}

func TestMemoryHandlesSurviveGrowthAndEviction(t *testing.T) {
	m := newMemoryCache()
	const count = 512
	pool, err := alloc.NewSlabAllocator(count * 4096)
	require.NoError(t, err)
	defer pool.Close()
	defer m.clear()
	keys := make([]xmap.Key, count)
	buffers := make([]*alloc.Buffer, count)
	for i := range keys {
		// Exercise repeated list growth within one shard.
		keys[i] = xmap.Key{Lo: uint64(i * m.entries.ShardCount()), Hi: 1}
		buffers[i], err = pool.TryAcquire(4096)
		require.NoError(t, err)
		m.replace(keys[i], buffers[i], blobstore.RecordHeader{Offset: uint64(i * 4096)})
	}
	s := m.entries.Shard(keys[0])
	for i, key := range keys {
		e := s.Items[key]
		require.Equal(t, uint64(i*4096), e.header.Offset)
		r := s.Extra.list.Value(e.resident)
		require.Equal(t, key, r.key)
		require.Same(t, buffers[i], r.buffer)
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
	m.replace(keys[0], buffer, blobstore.RecordHeader{Offset: 999})
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
			buffer := alloc.NewBuffer(4096)
			defer buffer.Unpin()
			for i := range 256 {
				key := xmap.Key{Lo: uint64(i % 32), Hi: 1}
				buffer.TryInc() // This goroutine owns a reference throughout.
				m.replace(key, buffer, blobstore.RecordHeader{Offset: uint64(worker * 4096)})
				if pinned, _, _ := m.lookup(key); pinned != nil {
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
			residentBytes += r.buffer.Cap()
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
	pool, err := alloc.NewSlabAllocator(40 * page)
	require.NoError(t, err)
	defer pool.Close()
	defer m.clear()
	for shard, pages := range []int{4, 8, 12, 16} {
		for i := range pages {
			b, err := pool.TryAcquire(page)
			require.NoError(t, err)
			m.replace(xmap.Key{Lo: uint64(i*m.entries.ShardCount() + shard)}, b, blobstore.RecordHeader{})
		}
	}
	require.Equal(t, 10*page, m.reclaimCacheMemory(10*page))
	for shard, remaining := range []int64{3, 6, 9, 12} {
		require.Equal(t, remaining*page, m.entries.ShardAt(shard).Extra.residentBytes,
			"each shard contributes one quarter of its resident bytes")
	}
	require.Equal(t, 40, m.entries.Len())
	require.Equal(t, 10*page, pool.Available())
}

func TestMemoryReclaimRedistributesPinnedShare(t *testing.T) {
	const page = int64(4096)
	m := newMemoryCache()
	pool, err := alloc.NewSlabAllocator(16 * page)
	require.NoError(t, err)
	defer pool.Close()
	defer m.clear()
	var pinned []*alloc.Buffer
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
			m.replace(xmap.Key{Lo: uint64(i*m.entries.ShardCount() + shard)}, b, blobstore.RecordHeader{})
		}
	}
	require.Equal(t, 8*page, m.reclaimCacheMemory(8*page))
	require.Zero(t, m.entries.ShardAt(0).Extra.residentBytes, "pinned victims were demoted")
	require.Equal(t, 4*page, m.entries.ShardAt(1).Extra.residentBytes,
		"the other shard supplies the pinned shard's unmet share")
	require.Equal(t, 8*page, pool.Available(), "only actual releases count")
	require.Equal(t, 16, m.entries.Len())
}
