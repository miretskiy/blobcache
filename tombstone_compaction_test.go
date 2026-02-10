package blobcache

import (
	"fmt"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// TestTombstoneCompaction_Basic validates the tombstone compaction flow.
func TestTombstoneCompaction_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),             // CAS mode (tombstones accumulate in .meta)
		WithMaxCachedSlabs(0), // Force disk path
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write several blobs
	keys := [][]byte{
		[]byte("key-1"),
		[]byte("key-2"),
		[]byte("key-3"),
		[]byte("key-4"),
	}
	value := make([]byte, 10_000) // 10KB each

	for _, k := range keys {
		require.NoError(t, cache.Put(k, value))
	}
	cache.Drain()

	// Get segment ID
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete keys 1 and 3 (CAS mode - tombstones written to .meta)
	require.NoError(t, cache.Delete(keys[0]))
	require.NoError(t, cache.Delete(keys[2]))

	// Run tombstone compaction directly
	shard := cache.index.SegmentLockShard(segID)
	shard.RLock()
	err = cache.index.CompactTombstones(segID)
	shard.RUnlock()
	require.NoError(t, err)

	// Verify compacted state: keys 1 and 3 should still be deleted
	h1 := xxh3.Hash128(keys[0])
	item1, found1 := cache.index.Get(h1)
	require.True(t, found1)
	require.True(t, item1.IsDeleted(), "key-1 should still be deleted after compaction")

	h2 := xxh3.Hash128(keys[1])
	item2, found2 := cache.index.Get(h2)
	require.True(t, found2)
	require.False(t, item2.IsDeleted(), "key-2 should still be live after compaction")

	h3 := xxh3.Hash128(keys[2])
	item3, found3 := cache.index.Get(h3)
	require.True(t, found3)
	require.True(t, item3.IsDeleted(), "key-3 should still be deleted after compaction")

	// key-2 and key-4 should still be readable
	_, ok := cache.Get(keys[1])
	require.True(t, ok, "key-2 should be readable after compaction")

	_, ok = cache.Get(keys[3])
	require.True(t, ok, "key-4 should be readable after compaction")
}

// TestTombstoneCompaction_SelectSegments validates candidate selection with cooling.
func TestTombstoneCompaction_SelectSegments(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Initially no segments
	segments := cache.selectSegmentsForTombstoneCompaction()
	require.Empty(t, segments, "should be empty with no segments")

	// Write enough data to create tombstones that exceed the threshold.
	value := make([]byte, 1_000)
	numItems := index.TombstoneCompactionThreshold + 20 // 120 items
	for i := range numItems {
		key := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, cache.Put(key, value))
	}
	cache.Drain()

	// No tombstones yet, should select nothing
	segments = cache.selectSegmentsForTombstoneCompaction()
	require.Empty(t, segments, "should be empty with no tombstones")

	// Delete enough keys to cross the threshold (100+)
	numDeletes := index.TombstoneCompactionThreshold + 5 // 105 deletes
	for i := range numDeletes {
		key := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, cache.Delete(key))
	}

	// Create additional segments to push the first segment past the cooling period.
	for range 3 {
		largeValue := make([]byte, 200_000) // Force new segments
		require.NoError(t, cache.Put([]byte("filler-key"), largeValue))
		cache.Drain()
	}

	// Now should select the segment that crossed the threshold
	segments = cache.selectSegmentsForTombstoneCompaction()
	require.Len(t, segments, 1, "should select one segment with 105 tombstones")
}
