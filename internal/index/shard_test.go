package index

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShard_SievePolicy(t *testing.T) {
	// Helper to seed a shard with N items
	seed := func(count int) *shard {
		s := &shard{}
		s.init(count)
		for i := 0; i < count; i++ {
			k := KeyFromHash(uint64(i))
			idx := s.alloc()
			s.nodes[idx] = node{
				key:  k,
				item: Item{SegmentID: 1},
			}
			s.items[k] = idx
			s.link(idx)
		}
		return s
	}

	t.Run("Full_Cycle_Clearing", func(t *testing.T) {
		const size = 100
		s := seed(size)

		// Mark all as visited
		for i := uint32(0); i < uint32(size); i++ {
			s.nodes[i].visited.Store(1)
		}

		// First eviction should clear all visited bits and evict one node
		victimIdx := s.runSieve()
		require.Less(t, victimIdx, uint32(size))

		// Count how many are still visited (should be at most 1 - the victim)
		visitedCount := 0
		for i := uint32(0); i < uint32(size); i++ {
			if s.nodes[i].visited.Load() == 1 {
				visitedCount++
			}
		}
		require.LessOrEqual(t, visitedCount, 1, "Most nodes should have been cleared")
	})

	t.Run("Hand_Persistence_Across_Calls", func(t *testing.T) {
		s := seed(10)

		// Evict 3 times - each should return a different victim
		seen := make(map[uint32]bool)
		for i := 0; i < 3; i++ {
			v := s.runSieve()
			require.False(t, seen[v], "Should not evict same node twice")
			seen[v] = true
			delete(s.items, s.nodes[v].key)
			s.free(v)
		}
		require.Equal(t, 3, len(seen))
	})

	t.Run("Unlink_Reduces_Count", func(t *testing.T) {
		s := seed(3)
		require.Equal(t, 3, len(s.items))

		// Unlink node 0 (need to also delete from items)
		k := s.nodes[0].key
		delete(s.items, k)
		s.unlink(0)

		require.Equal(t, 2, len(s.items))
	})

	t.Run("Hot_Node_Gets_Second_Chance", func(t *testing.T) {
		s := seed(2)

		// Mark node 0 as hot
		s.nodes[0].visited.Store(1)

		// Should skip hot node and evict the cold one
		victim := s.runSieve()

		// The hot node should have its bit cleared
		require.Equal(t, uint32(0), s.nodes[0].visited.Load())

		// Victim should be the other node (the cold one)
		require.NotEqual(t, uint32(0), victim, "Should not evict hot node first")
	})

	t.Run("Single_Node", func(t *testing.T) {
		s := seed(1)

		victim := s.runSieve()
		require.Equal(t, uint32(0), victim)

		delete(s.items, s.nodes[victim].key)
		s.unlink(victim)
		require.Equal(t, uint32(nullIdx), s.head)
		require.Equal(t, uint32(nullIdx), s.hand)
	})

	t.Run("Free_List_Reuse", func(t *testing.T) {
		s := seed(5)
		initialArenaSize := len(s.nodes)

		// Free nodes 1, 3 (must delete from items first)
		delete(s.items, s.nodes[1].key)
		s.free(1)
		delete(s.items, s.nodes[3].key)
		s.free(3)

		// Allocate 2 new nodes - should reuse freed slots
		newIdx1 := s.alloc()
		newIdx2 := s.alloc()

		// Should reuse slots 3, 1 (LIFO order)
		require.Equal(t, uint32(3), newIdx1)
		require.Equal(t, uint32(1), newIdx2)

		// Arena should not have grown
		require.Equal(t, initialArenaSize, len(s.nodes))
	})
}

func TestSieve_Interleaved(t *testing.T) {
	s := &shard{}
	s.init(1000)

	const total = 1000
	for i := 0; i < total; i++ {
		k := KeyFromHash(uint64(i))
		idx := s.alloc()
		s.nodes[idx] = node{
			key:  k,
			item: Item{SegmentID: 1},
		}
		s.items[k] = idx
		s.link(idx)
	}

	// Mark even-indexed nodes as visited
	for i := uint32(0); i < total; i += 2 {
		s.nodes[i].visited.Store(1)
	}

	// Evict 500 nodes - should prefer cold (odd) ones first
	coldEvicted := 0
	for i := 0; i < 500; i++ {
		victim := s.runSieve()
		if victim%2 != 0 {
			coldEvicted++
		}
		delete(s.items, s.nodes[victim].key)
		s.free(victim)
	}

	// Most evictions should be cold nodes
	require.Greater(t, coldEvicted, 400, "Should prefer evicting cold nodes")
}
