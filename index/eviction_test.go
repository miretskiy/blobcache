package index

import (
	"testing"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/stretchr/testify/require"
)

func TestSievePolicy_Advanced(t *testing.T) {
	// Helper to seed the policy with N nodes
	seed := func(count int) (*sievePolicy, []*node) {
		p := &sievePolicy{}
		nodes := make([]*node, count)
		for i := 0; i < count; i++ {
			// FIX: Pass the Entry data to Add, which returns the *node handle
			nodes[i] = p.Add(Entry{
				BlobRecord: metadata.BlobRecord{Hash: uint64(i)},
				SegmentID:  1,
			})
		}
		return p, nodes
	}

	t.Run("Full_Cycle_Clearing", func(t *testing.T) {
		const size = 100
		p, nodes := seed(size)

		for _, n := range nodes {
			n.visited.Store(true)
		}

		node, err := p.EvictNext()
		require.NoError(t, err)
		require.Equal(t, uint64(0), node.entry.Hash)

		for i := 1; i < size; i++ {
			require.False(t, nodes[i].visited.Load(), "Node %d should have been cleared", i)
		}
	})

	t.Run("Hand_Persistence_Across_Calls", func(t *testing.T) {
		p, _ := seed(10)

		h1, _ := p.EvictNext()
		h2, _ := p.EvictNext()
		h3, _ := p.EvictNext()

		require.Equal(t, uint64(0), h1.entry.Hash)
		require.Equal(t, uint64(1), h2.entry.Hash)
		require.Equal(t, uint64(2), h3.entry.Hash)
	})

	t.Run("Remove_Boundary_Conditions", func(t *testing.T) {
		tests := []struct {
			name       string
			removeIdx  int
			expectTail uint64
			expectHead uint64
		}{
			{"Remove_Tail", 0, 1, 2},
			{"Remove_Head", 2, 0, 1},
			{"Remove_Middle", 1, 0, 2},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p, nodes := seed(3)
				p.removeNode(nodes[tt.removeIdx])

				require.Equal(t, 2, p.count)
				require.Equal(t, tt.expectTail, p.tail.entry.Hash)
				require.Equal(t, tt.expectHead, p.head.entry.Hash)
			})
		}
	})

	t.Run("Concurrent_Hit_During_Eviction", func(t *testing.T) {
		p, nodes := seed(2)
		nodes[0].visited.Store(true)

		node, err := p.EvictNext()
		require.NoError(t, err)
		require.Equal(t, uint64(1), node.entry.Hash)
	})
}

func TestSieve_Interleaved(t *testing.T) {
	p := &sievePolicy{}
	const total = 1000
	nodes := make(map[uint64]*node)

	for i := 0; i < total; i++ {
		// FIX: Use p.Add(Entry) instead of manual newNode
		n := p.Add(Entry{
			BlobRecord: metadata.BlobRecord{Hash: uint64(i)},
			SegmentID:  1,
		})
		nodes[uint64(i)] = n
	}

	for i := 0; i < total; i += 2 {
		nodes[uint64(i)].visited.Store(true)
	}

	for i := 0; i < 500; i++ {
		node, err := p.EvictNext()
		require.NoError(t, err)
		require.True(t, node.entry.Hash%2 != 0, "Evicted visited node %d", node.entry.Hash)
	}
}
