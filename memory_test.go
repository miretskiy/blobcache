package blobcache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/collections"
	"github.com/stretchr/testify/require"
)

func TestMemoryShardAlignment(t *testing.T) {
	require.NoError(t, collections.VerifyAlignment[blobstore.KeyHash, memoryEntry, memoryQueue]())
}

func TestMemoryIgnoresOlderPublication(t *testing.T) {
	m := newMemoryCache(32 << 10)
	defer m.clear()
	key := blobstore.HashKey([]byte("key"))
	newer := newTestMemory(t, 4096)
	m.replace(key, newer, blobstore.RecordHeader{Position: 2}, false)
	older := newTestMemory(t, 4096)
	m.replace(key, older, blobstore.RecordHeader{Position: 1}, false)
	require.Empty(t, older.First(), "discard the older completion's reference")
	buffer, header, ok := m.lookup(key)
	require.True(t, ok)
	require.Equal(t, newer, buffer)
	require.EqualValues(t, 2, header.Position)
	unpinMemory(buffer)
}

func TestMemoryHandlesSurviveGrowthAndEviction(t *testing.T) {
	m := newMemoryCache(32 << 10)
	const count = 512
	pool := newTestPool(t, count*4096)
	defer m.clear()
	keys := make([]blobstore.KeyHash, count)
	buffers := make([]*alloc.ReadOnlyBuffer, count)
	for i := range keys {
		// Exercise repeated list growth within one shard.
		keys[i] = blobstore.KeyHash(i * m.entries.ShardCount())
		buffers[i] = acquire(t, pool, 4096)
		m.replace(keys[i], buffers[i], blobstore.RecordHeader{Position: uint64(i * 4096)}, false)
	}
	s := m.entries.Shard(keys[0])
	for i, key := range keys {
		e := s.Items[key]
		require.Equal(t, uint64(i*4096), e.header.Position)
		r := s.Extra.list.Value(e.resident)
		require.Equal(t, key, r.key)
		require.Equal(t, memorySize(buffers[i]), memorySize(r.buffer))
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
		require.Equal(t, uint64(i*4096), e.header.Position)
		require.Zero(t, e.resident)
	}
	buffer := acquire(t, pool, 4096)
	m.replace(keys[0], buffer, blobstore.RecordHeader{Position: 999}, false)
	require.NotEqual(t, old, s.Items[keys[0]].resident)
	_, ok := s.Extra.list.TryValue(old)
	require.False(t, ok, "reusing a slot does not resurrect an old handle")
	require.Equal(t, uint64(999), s.Items[keys[0]].header.Position)
	require.Equal(t, 1, s.Extra.list.Len())
	require.Equal(t, count, m.entries.Len())
}

func TestMemoryConcurrentReplacementAndEviction(t *testing.T) {
	m := newMemoryCache(32 << 10)
	defer m.clear()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			buffer := newTestMemory(t, 4096)
			defer unpinMemory(buffer)
			for i := range 256 {
				key := blobstore.KeyHash(i % 32)
				buffer.Retain() // This goroutine owns a reference throughout.
				m.replace(key, buffer, blobstore.RecordHeader{Position: uint64(worker * 4096)}, false)
				if pinned, _, _ := m.lookup(key); pinned != nil {
					unpinMemory(pinned)
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
			residentBytes += memorySize(r.buffer)
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
	m := newMemoryCache(32 << 10)
	pool := newTestPool(t, 40*page)
	defer m.clear()
	for shard, pages := range []int{4, 8, 12, 16} {
		for i := range pages {
			b := acquire(t, pool, page)
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
	m := newMemoryCache(32 << 10)
	pool := newTestPool(t, 16*page)
	defer m.clear()
	var pinned []*alloc.ReadOnlyBuffer
	defer func() {
		for _, b := range pinned {
			unpinMemory(b)
		}
	}()
	for shard, pages := range []int{4, 12} {
		for i := range pages {
			b := acquire(t, pool, page)
			if shard == 0 {
				b.Retain()
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

func TestCacheProactiveReclamation(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	for i := range 26 { // 26 of 32 pages crosses 80% usage.
		putValue(t, c, fmt.Sprint(i), []byte("v"))
	}
	require.Eventually(t, func() bool {
		return c.memory.Available() >= c.memory.Capacity()/4
	}, 5*time.Second, time.Millisecond, "reclamation starts without a failed allocation")
	require.Equal(t, 26, c.values.entries.Len(), "all disk metadata survives")
}

func TestPendingMemoryTransfersOriginalOwnership(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(fmt.Sprint(replaced), func(t *testing.T) {
			pool := newTestPool(t, 4<<20)
			m := newMemoryCache(32 << 10)
			defer m.clear()
			buffer := alloc.NewAllocatingBuffer(pool)
			_, err := buffer.Write([]byte("original"))
			require.NoError(t, err)
			buffer.Trim()
			memory := buffer.Freeze()
			size := memorySize(memory)
			h := blobstore.RecordHeader{Hash: 1, Position: 1}
			m.replace(1, memory, h, true)
			shard := m.entries.Shard(1)
			entry := shard.Extra.list.Value(shard.Items[1].resident)
			require.Same(t, memory, entry.buffer, "publication borrows the original buffer")
			if replaced {
				m.replace(1, nil, blobstore.RecordHeader{Hash: 1, Position: 2}, false)
				require.Equal(t, pool.Capacity()-size, pool.Available(), "replacement must not release Put's in-flight buffer")
			}
			moved := memory
			require.Equal(t, !replaced, m.complete(h, nil))
			if replaced {
				require.Equal(t, size, moved.Release(), "Put owns the only references")
			} else {
				require.Equal(t, size, m.reclaimCacheMemory(size), "the cache receives those same references")
			}
			require.Equal(t, pool.Capacity(), pool.Available())
		})
	}
}

func TestReaderOutlivesReplacedPendingMemory(t *testing.T) {
	pool := newTestPool(t, 4<<20)
	m := newMemoryCache(32 << 10)
	defer m.clear()
	buffer := alloc.NewAllocatingBuffer(pool)
	_, err := buffer.Write([]byte("borrowed by reader"))
	require.NoError(t, err)
	buffer.Trim()
	h := blobstore.RecordHeader{Hash: 1, Position: 1}
	memory := buffer.Freeze()
	m.replace(1, memory, h, true)
	reader, _, ok := m.lookup(1)
	require.True(t, ok)
	m.replace(1, nil, blobstore.RecordHeader{Hash: 1, Position: 2}, false)
	moved := memory
	require.False(t, m.complete(h, nil))
	require.Zero(t, moved.Release(), "the reader alone keeps the replaced value alive")
	require.Equal(t, "borrowed by reader", string(reader.First()[:18]))
	require.True(t, unpinMemory(reader))
	require.Equal(t, pool.Capacity(), pool.Available())
}

func TestMemoryReclaimCountsOnlyReleasedBytes(t *testing.T) {
	const page = int64(align.BlockSize)
	pool := newTestPool(t, 9*page)
	m := newMemoryCache(32 << 10)
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
		b := acquire(t, pool, pages*page)
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
	c := newPutCache(t, testDevice(t, 64<<20))
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

func TestMemoryQueueSecondChance(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	var keys []string
	for i := 0; len(keys) < 3; i++ {
		key := fmt.Sprint(i)
		if blobstore.HashKey([]byte(key))%uint64(c.values.entries.ShardCount()) == 0 {
			keys = append(keys, key)
			putValue(t, c, key, []byte(key))
		}
	}
	first := residentBuffer(t, c, []byte(keys[0]))
	second := residentBuffer(t, c, []byte(keys[1]))
	s := c.values.entries.Shard(blobstore.HashKey([]byte(keys[0])))
	s.Lock()
	s.Extra.list.Value(s.Items[blobstore.HashKey([]byte(keys[0]))].resident).visited = 1
	s.Unlock()
	require.Equal(t, memorySize(second), c.values.reclaimCacheMemory(align.BlockSize))
	require.Empty(t, second.First())
	require.Equal(t, []byte(keys[0]), retainedValue(t, c, []byte(keys[0])))
	// Replace the next eviction candidate and verify the hand still progresses.
	putValue(t, c, keys[2], []byte("replacement"))
	c.values.reclaimCacheMemory(c.memory.Capacity())
	require.Empty(t, first.First())
	require.Equal(t, 3, c.values.entries.Len())
	for i := range c.values.entries.ShardCount() {
		s := c.values.entries.ShardAt(i)
		require.Zero(t, s.Extra.list.Len())
		require.Zero(t, s.Extra.hand)
		for _, e := range s.Items {
			require.Zero(t, e.resident)
		}
	}
}

// Values whose disk write is pending are not evictable: with nothing else
// resident, reclamation returns without walking the queue, and completing a
// write makes exactly that value evictable.
func TestReclaimSkipsPendingWithoutWalking(t *testing.T) {
	pool := newTestPool(t, 1<<20)
	m := newMemoryCache(32 << 10)
	buffers := make([]*alloc.ReadOnlyBuffer, 8)
	for i := range buffers {
		buffers[i] = acquire(t, pool, 4096)
		m.replace(blobstore.KeyHash(i), buffers[i], blobstore.RecordHeader{Hash: uint64(i), Position: 1}, true)
	}
	visited := metrics.ReclaimVisited.Load()
	require.Zero(t, m.reclaimCacheMemory(1<<20))
	require.Equal(t, visited, metrics.ReclaimVisited.Load(), "nothing evictable: no queue walk")

	require.True(t, m.complete(blobstore.RecordHeader{Hash: 3, Position: 1}, nil))
	require.EqualValues(t, 4096, m.reclaimCacheMemory(1<<20), "only the completed value is evictable")
	for i := range m.entries.ShardCount() {
		q := &m.entries.ShardAt(i).Extra
		require.Equal(t, q.residentBytes, q.pendingBytes, "every remaining value is pending")
	}
	m.clear()
	for i, b := range buffers {
		if i != 3 {
			b.Release() // Put's references; the cache only borrowed them.
		}
	}
	require.Equal(t, pool.Capacity(), pool.Available())
}
