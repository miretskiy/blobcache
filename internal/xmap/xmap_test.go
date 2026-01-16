package xmap

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// TestShardAlignment verifies the alignment check helper works.
func TestShardAlignment(t *testing.T) {
	// Pad32 should produce a 64-byte aligned shard
	size := unsafe.Sizeof(Shard[int, Pad32]{})
	require.Equal(t, uintptr(64), size, "Shard[int, Pad32] should be 64 bytes")

	// VerifyAlignment should pass for properly padded types
	require.NoError(t, VerifyAlignment[int, Pad32]())

	// Creating a map with Pad32 should work
	m := New[int, Pad32]()
	require.NotNil(t, m)
}

func TestShardAlignment_Misaligned(t *testing.T) {
	// struct{} is 0 bytes, so Shard base (32) is not 64-aligned.
	// VerifyAlignment should return an error (not panic at runtime).
	err := VerifyAlignment[int, struct{}]()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a multiple of 64")
}

func TestMap_BasicOperations(t *testing.T) {
	m := New[int, Pad32]()

	k1 := xxh3.Hash128([]byte("key1"))
	k2 := xxh3.Hash128([]byte("key2"))

	// Initially empty
	require.Equal(t, 0, m.Len())

	_, ok := m.Get(k1)
	require.False(t, ok)

	// Put and Get
	m.Put(k1, 100)
	v, ok := m.Get(k1)
	require.True(t, ok)
	require.Equal(t, 100, v)
	require.Equal(t, 1, m.Len())

	// Update existing
	m.Put(k1, 200)
	v, ok = m.Get(k1)
	require.True(t, ok)
	require.Equal(t, 200, v)
	require.Equal(t, 1, m.Len())

	// Add second key
	m.Put(k2, 300)
	require.Equal(t, 2, m.Len())

	// Delete
	deleted := m.Delete(k1)
	require.True(t, deleted)
	require.Equal(t, 1, m.Len())

	_, ok = m.Get(k1)
	require.False(t, ok)

	// Delete non-existent
	deleted = m.Delete(k1)
	require.False(t, deleted)
}

func TestMap_Collect(t *testing.T) {
	m := New[int, Pad32]()

	// Add 100 entries
	for i := range 100 {
		k := xxh3.Hash128([]byte(fmt.Sprintf("key%d", i)))
		m.Put(k, i*10)
	}

	require.Equal(t, 100, m.Len())

	// Collect all values
	values := m.Collect(nil)
	require.Len(t, values, 100)

	// Verify all values are present (sum should match)
	sum := 0
	for _, v := range values {
		sum += v
	}
	expectedSum := 0
	for i := range 100 {
		expectedSum += i * 10
	}
	require.Equal(t, expectedSum, sum)

	// Test with pre-allocated dst
	dst := make([]int, 0, 200)
	dst = m.Collect(dst)
	require.Len(t, dst, 100)
	require.Equal(t, 200, cap(dst))
}

func TestMap_ForEach(t *testing.T) {
	m := New[int, Pad32]()

	for i := range 50 {
		k := xxh3.Hash128([]byte(fmt.Sprintf("key%d", i)))
		m.Put(k, i)
	}

	// Count all entries
	count := 0
	m.ForEach(func(k Key, v int, extra *Pad32) bool {
		count++
		return true
	})
	require.Equal(t, 50, count)

	// Early termination
	count = 0
	m.ForEach(func(k Key, v int, extra *Pad32) bool {
		count++
		return count < 10
	})
	require.Equal(t, 10, count)
}

func TestMap_Clear(t *testing.T) {
	m := New[int, Pad32]()

	keys := make([]Key, 100)
	for i := range 100 {
		keys[i] = xxh3.Hash128([]byte(fmt.Sprintf("key%d", i)))
		m.Put(keys[i], i)
	}
	require.Equal(t, 100, m.Len())

	m.Clear()
	require.Equal(t, 0, m.Len())

	// Verify all keys are gone
	for i := range 100 {
		_, ok := m.Get(keys[i])
		require.False(t, ok)
	}
}

func TestMap_ShardDistribution(t *testing.T) {
	// With 256 shards and 1000 entries, check distribution is reasonable.
	m := New[int, Pad32](WithShardShift(8))

	for i := range 1000 {
		k := xxh3.Hash128([]byte(fmt.Sprintf("entry%d", i)))
		m.Put(k, i)
	}

	require.Equal(t, 1000, m.Len())

	// Check that entries are distributed across shards
	nonEmptyShards := 0
	for i := range m.ShardCount() {
		s := m.ShardAt(i)
		s.RLock()
		if len(s.Items) > 0 {
			nonEmptyShards++
		}
		s.RUnlock()
	}

	// With 1000 entries and 256 shards, expect most shards non-empty
	require.Greater(t, nonEmptyShards, 200, "entries should be distributed across shards")
}

func TestMap_ShardAccess(t *testing.T) {
	// Test direct shard access for complex usage patterns
	m := New[int, Pad32](WithShardShift(4))

	k := xxh3.Hash128([]byte("test"))
	s := m.Shard(k)

	// Direct manipulation under lock
	s.Lock()
	s.Items[k] = 42
	s.Unlock()

	// Verify via Get
	v, ok := m.Get(k)
	require.True(t, ok)
	require.Equal(t, 42, v)
}

func TestMap_ConcurrentAccess(t *testing.T) {
	m := New[int, Pad32](WithShardShift(4))

	var wg sync.WaitGroup
	numGoroutines := 100
	opsPerGoroutine := 1000

	// Pre-compute keys
	keys := make([][]Key, numGoroutines)
	for g := range numGoroutines {
		keys[g] = make([]Key, opsPerGoroutine)
		for i := range opsPerGoroutine {
			keys[g][i] = xxh3.Hash128([]byte(fmt.Sprintf("g%d-i%d", g, i)))
		}
	}

	// Concurrent writes
	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				m.Put(keys[gid][i], gid*opsPerGoroutine+i)
			}
		}(g)
	}
	wg.Wait()

	require.Equal(t, numGoroutines*opsPerGoroutine, m.Len())

	// Concurrent reads
	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				v, ok := m.Get(keys[gid][i])
				require.True(t, ok)
				require.Equal(t, gid*opsPerGoroutine+i, v)
			}
		}(g)
	}
	wg.Wait()
}

func TestMap_ConcurrentMixed(t *testing.T) {
	m := New[int, Pad32](WithShardShift(4))

	// Pre-compute keys for all operations
	keys := make([]Key, 1000)
	for i := range 1000 {
		keys[i] = xxh3.Hash128([]byte(fmt.Sprintf("concurrent%d", i)))
	}

	var wg sync.WaitGroup

	// Writers
	for g := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 1000 {
				m.Put(keys[i], id*1000+i)
			}
		}(g)
	}

	// Readers
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				m.Get(keys[i]) // Just exercise the read path
			}
		}()
	}

	// Deleters
	for g := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 100 {
				m.Delete(keys[id*100+i+500])
			}
		}(g)
	}

	wg.Wait()
	// Just verify no panics occurred
}

// ArenaState simulates a realistic shard payload like index.ShardState.
// It contains an arena slice and linked-list management fields.
// Size: slice(24) + uint32(4) + uint32(4) = 32 bytes.
// Combined with Shard base (32 bytes), total = 64 bytes.
type ArenaState struct {
	Nodes    []ArenaNode // Arena: contiguous memory
	FreeHead uint32      // Free list stack head
	Head     uint32      // Linked list head
}

type ArenaNode struct {
	Value int
	Next  uint32
	Prev  uint32
}

const nullIdx = 0xFFFFFFFF

func TestMap_ArenaPayload(t *testing.T) {
	// Verify alignment: base(32) + ArenaState(32) = 64
	size := unsafe.Sizeof(Shard[uint32, ArenaState]{})
	require.Equal(t, uintptr(64), size, "Shard with ArenaState should be 64 bytes")

	m := New[uint32, ArenaState]()

	// Initialize arenas
	for i := range m.ShardCount() {
		s := m.ShardAt(i)
		s.Lock()
		s.Extra.Nodes = make([]ArenaNode, 0, 16)
		s.Extra.FreeHead = nullIdx
		s.Extra.Head = nullIdx
		s.Unlock()
	}

	// Simulate index-like operations: allocate arena slots
	keys := make([]Key, 100)
	for i := range 100 {
		keys[i] = xxh3.Hash128([]byte(fmt.Sprintf("arena-key-%d", i)))

		s := m.Shard(keys[i])
		s.Lock()

		// Allocate from arena
		var idx uint32
		if s.Extra.FreeHead != nullIdx {
			idx = s.Extra.FreeHead
			s.Extra.FreeHead = s.Extra.Nodes[idx].Next
		} else {
			s.Extra.Nodes = append(s.Extra.Nodes, ArenaNode{})
			idx = uint32(len(s.Extra.Nodes) - 1)
		}

		// Store in arena and map
		s.Extra.Nodes[idx].Value = i * 10
		s.Items[keys[i]] = idx

		s.Unlock()
	}

	require.Equal(t, 100, m.Len())

	// Verify lookups go through arena
	for i := range 100 {
		s := m.Shard(keys[i])
		s.RLock()
		idx, ok := s.Items[keys[i]]
		require.True(t, ok)
		require.Equal(t, i*10, s.Extra.Nodes[idx].Value)
		s.RUnlock()
	}

	// Simulate deletion with free-list recycling
	for i := range 50 {
		s := m.Shard(keys[i])
		s.Lock()
		idx := s.Items[keys[i]]
		delete(s.Items, keys[i])
		// Push to free list
		s.Extra.Nodes[idx].Next = s.Extra.FreeHead
		s.Extra.FreeHead = idx
		s.Unlock()
	}

	require.Equal(t, 50, m.Len())

	// Verify free list is populated
	totalFree := 0
	for i := range m.ShardCount() {
		s := m.ShardAt(i)
		s.RLock()
		freeIdx := s.Extra.FreeHead
		for freeIdx != nullIdx {
			totalFree++
			freeIdx = s.Extra.Nodes[freeIdx].Next
		}
		s.RUnlock()
	}
	require.Equal(t, 50, totalFree)
}

func BenchmarkMap_Put(b *testing.B) {
	m := New[int, Pad32](WithShardShift(8))
	keys := make([]Key, b.N)
	for i := range keys {
		keys[i] = xxh3.Hash128([]byte(fmt.Sprintf("bench%d", i)))
	}

	b.ResetTimer()
	for i := range b.N {
		m.Put(keys[i], i)
	}
}

func BenchmarkMap_Get(b *testing.B) {
	m := New[int, Pad32](WithShardShift(8))
	keys := make([]Key, b.N)
	for i := range keys {
		keys[i] = xxh3.Hash128([]byte(fmt.Sprintf("bench%d", i)))
		m.Put(keys[i], i)
	}

	b.ResetTimer()
	for i := range b.N {
		m.Get(keys[i%len(keys)])
	}
}

func BenchmarkMap_GetParallel(b *testing.B) {
	m := New[int, Pad32](WithShardShift(8))
	keys := make([]Key, 100000)
	for i := range keys {
		keys[i] = xxh3.Hash128([]byte(fmt.Sprintf("bench%d", i)))
		m.Put(keys[i], i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Get(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkMap_Collect(b *testing.B) {
	m := New[int, Pad32](WithShardShift(8))
	for i := range 10000 {
		k := xxh3.Hash128([]byte(fmt.Sprintf("bench%d", i)))
		m.Put(k, i)
	}

	b.ResetTimer()
	for range b.N {
		dst := make([]int, 0, 10000)
		_ = m.Collect(dst)
	}
}
