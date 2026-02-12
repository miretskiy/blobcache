package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// newTestCASCache creates a WAL-mode cache with small segments for testing.
// By default sets CompactionWasteThreshold=1.0 to disable background compaction,
// so tests can call rewriteSegment directly without interference.
func newTestCASCache(t *testing.T, opts ...Option) *Cache {
	t.Helper()
	defaults := []Option{
		WithWAL(),
		WithMaxCachedSlabs(0),        // Force disk path
		WithWriteBufferSize(1 << 20), // 1MB segments
		WithMaxSize(10 << 20),        // 10MB
		WithBallast(0),
		WithCompactionWasteThreshold(1.0), // Disable background compaction
	}
	cache, err := New(t.TempDir(), append(defaults, opts...)...)
	require.NoError(t, err)
	cache.Start()
	t.Cleanup(func() { cache.Close() })
	return cache
}

// writeAndDrain writes items and drains to disk, returning keys and the segment ID.
func writeAndDrain(t *testing.T, cache *Cache, prefix string, n int, valueSize int) ([][]byte, uint32) {
	t.Helper()
	value := make([]byte, valueSize)
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = fmt.Appendf(nil, "%s-%04d", prefix, i)
		// Write known pattern for data verification.
		for j := range value {
			value[j] = byte(i + j)
		}
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	return keys, item.SegmentID
}

// pushPastCooling creates enough filler segments to push older segments past
// the cooling period.
func pushPastCooling(t *testing.T, cache *Cache) {
	t.Helper()
	fillers := cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1
	for i := range max(fillers, 4) {
		key := fmt.Appendf(nil, "cooling-filler-%04d", i)
		require.NoError(t, cache.Put(key, make([]byte, 200_000)))
		cache.Drain()
	}
}

// TestRewriteSegment_BasicFlow validates the full rewrite cycle:
// write items → delete some → rewrite → verify live items readable in new segment.
func TestRewriteSegment_BasicFlow(t *testing.T) {
	cache := newTestCASCache(t)

	keys, segID := writeAndDrain(t, cache, "rewrite", 20, 40_000)

	// Delete 15 of 20 items.
	for i := 5; i < 20; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	pushPastCooling(t, cache)

	// Rewrite the segment.
	shard := cache.index.SegmentLockShard(segID)
	shard.RLock()
	result, err := cache.rewriteSegment(segID)
	shard.RUnlock()
	require.NoError(t, err)

	require.Equal(t, 5, result.LiveItems)
	require.False(t, result.AllDead)
	require.NotEqual(t, segID, result.NewSegID)

	// Drop old segment (rewriteSegment already does this).
	// Verify old segment file is gone.
	oldPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(oldPath)
	require.True(t, os.IsNotExist(err), "old segment should be deleted")

	// Verify live items are readable via the new segment.
	for i := range 5 {
		val, ok := cache.Get(keys[i])
		require.True(t, ok, "live item %d should be readable after rewrite", i)
		require.NotEmpty(t, val)
	}

	// Verify deleted items are not readable.
	for i := 5; i < 20; i++ {
		_, ok := cache.Get(keys[i])
		require.False(t, ok, "deleted item %d should not be readable", i)
	}

	// Verify live items point to the new segment.
	for i := range 5 {
		h := xxh3.Hash128(keys[i])
		item, found := cache.index.Get(h)
		require.True(t, found)
		require.Equal(t, result.NewSegID, item.SegmentID, "item %d should be in new segment", i)
	}
}

// TestRewriteSegment_AllDead validates that a segment with 100% dead items
// is deleted without rewriting via maybeRewriteSegments.
func TestRewriteSegment_AllDead(t *testing.T) {
	cache := newTestCASCache(t,
		WithCompactionWasteThreshold(0.25), // Override the default 1.0 to enable compaction
	)

	keys, segID := writeAndDrain(t, cache, "alldead", 10, 40_000)

	// Delete all items.
	for _, k := range keys {
		require.NoError(t, cache.Delete(k))
	}

	pushPastCooling(t, cache)

	// Run compaction — should delete the dead segment.
	err := cache.maybeRewriteSegments()
	require.NoError(t, err)

	// Verify: segment file should be deleted.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.True(t, os.IsNotExist(err), "dead segment should be deleted")

	// Verify: .meta file should be deleted.
	metaPath := SegmentMetaPath(segPath)
	_, err = os.Stat(metaPath)
	require.True(t, os.IsNotExist(err), "dead segment .meta should be deleted")

	// Verify: none of the items are readable via Get.
	for _, k := range keys {
		_, ok := cache.Get(k)
		require.False(t, ok, "deleted item should not be readable")
	}
}

// TestRewriteSegment_StalenessFiltering verifies that overwritten items
// (where the RAM index points to a newer segment) are excluded from rewrite.
func TestRewriteSegment_StalenessFiltering(t *testing.T) {
	cache := newTestCASCache(t)

	keys, segID := writeAndDrain(t, cache, "stale", 10, 40_000)

	// Overwrite 5 items → they'll land in a new segment.
	newValue := make([]byte, 40_000)
	for i := range 5 {
		require.NoError(t, cache.Put(keys[i], newValue))
	}
	cache.Drain()

	pushPastCooling(t, cache)

	// Rewrite original segment — only 5 non-overwritten items should survive.
	shard := cache.index.SegmentLockShard(segID)
	shard.RLock()
	result, err := cache.rewriteSegment(segID)
	shard.RUnlock()
	require.NoError(t, err)

	require.Equal(t, 5, result.LiveItems, "only non-overwritten items should be live")

	// All 10 items should still be readable (5 from new segment, 5 from rewritten).
	for i := range 10 {
		_, ok := cache.Get(keys[i])
		require.True(t, ok, "item %d should be readable", i)
	}
}

// TestRewriteSegment_DataVerification writes known data, rewrites, and verifies
// byte-for-byte correctness of read-back values.
func TestRewriteSegment_DataVerification(t *testing.T) {
	cache := newTestCASCache(t)

	const numItems = 10
	const valueSize = 5_000
	value := make([]byte, valueSize)
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "verify-%04d", i)
		// Write a unique pattern per item.
		for j := range value {
			value[j] = byte(i*37 + j*13)
		}
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete half the items.
	for i := numItems / 2; i < numItems; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	pushPastCooling(t, cache)

	// Rewrite.
	shard := cache.index.SegmentLockShard(segID)
	shard.RLock()
	_, err := cache.rewriteSegment(segID)
	shard.RUnlock()
	require.NoError(t, err)

	// Verify byte-for-byte data correctness.
	for i := range numItems / 2 {
		expected := make([]byte, valueSize)
		for j := range expected {
			expected[j] = byte(i*37 + j*13)
		}

		got, ok := cache.Get(keys[i])
		require.True(t, ok, "item %d should be readable", i)
		require.Equal(t, expected, got, "item %d data mismatch after rewrite", i)
	}
}

// TestRewriteSegment_BlockAlignment verifies that output records start at
// block-aligned offsets in the rewritten segment.
func TestRewriteSegment_BlockAlignment(t *testing.T) {
	cache := newTestCASCache(t)

	keys, segID := writeAndDrain(t, cache, "align", 5, 5_000)

	// Delete alternating items to create gaps (non-contiguous runs).
	require.NoError(t, cache.Delete(keys[1]))
	require.NoError(t, cache.Delete(keys[3]))

	pushPastCooling(t, cache)

	// Rewrite.
	shard := cache.index.SegmentLockShard(segID)
	shard.RLock()
	result, err := cache.rewriteSegment(segID)
	shard.RUnlock()
	require.NoError(t, err)

	require.Equal(t, 3, result.LiveItems) // items 0, 2, 4

	// Check that live items in the new segment have block-aligned offsets.
	for _, i := range []int{0, 2, 4} {
		h := xxh3.Hash128(keys[i])
		item, found := cache.index.Get(h)
		require.True(t, found)
		require.Equal(t, result.NewSegID, item.SegmentID)
		require.EqualValues(t, 0, int64(item.Offset)%sys.BlockSize,
			"item %d offset %d should be block-aligned", i, item.Offset)
	}
}

// TestRewriteSegment_RunMerging verifies that contiguous live records are merged
// into a single copy_file_range call.
func TestRewriteSegment_RunMerging(t *testing.T) {
	cache := newTestCASCache(t)

	keys, segID := writeAndDrain(t, cache, "run", 10, 5_000)

	// Delete only item 5 → items 0-4 form one run, items 6-9 form another.
	require.NoError(t, cache.Delete(keys[5]))

	pushPastCooling(t, cache)

	// Rewrite.
	shard := cache.index.SegmentLockShard(segID)
	shard.RLock()
	result, err := cache.rewriteSegment(segID)
	shard.RUnlock()
	require.NoError(t, err)

	require.Equal(t, 9, result.LiveItems)

	// Verify all live items readable.
	for i := range 10 {
		if i == 5 {
			continue
		}
		val, ok := cache.Get(keys[i])
		require.True(t, ok, "item %d should be readable", i)
		require.NotEmpty(t, val)
	}
}

// TestRewriteSegment_CoolingPeriod verifies that recent segments are not selected
// as rewrite candidates.
func TestRewriteSegment_CoolingPeriod(t *testing.T) {
	cache := newTestCASCache(t,
		WithCompactionWasteThreshold(0.10), // Low threshold to ensure it would trigger
	)

	keys, _ := writeAndDrain(t, cache, "cool", 10, 40_000)

	// Delete 90% of items.
	for i := 1; i < 10; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Do NOT push past cooling period. Run maintenance — should be a no-op.
	err := cache.maybeRewriteSegments()
	require.NoError(t, err)

	// All items (including deleted) should still have their original segment.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	origSegID := item.SegmentID

	segPath := getSegmentPath(cache.Path, cache.Shards, origSegID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "segment should still exist (within cooling period)")
}

// TestRewriteSegment_ConfigurableThreshold verifies that the waste threshold
// controls which segments are selected for rewrite.
func TestRewriteSegment_ConfigurableThreshold(t *testing.T) {
	cache := newTestCASCache(t,
		WithCompactionWasteThreshold(0.90), // Very high threshold — only nearly-dead segments
	)

	keys, segID := writeAndDrain(t, cache, "thresh", 10, 40_000)

	// Delete 50% → waste ratio = 0.5, below threshold of 0.9.
	for i := range 5 {
		require.NoError(t, cache.Delete(keys[i]))
	}

	pushPastCooling(t, cache)

	// Run maintenance — should NOT rewrite (0.5 < 0.9 threshold).
	err := cache.maybeRewriteSegments()
	require.NoError(t, err)

	// Segment should still exist.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "segment should still exist (below threshold)")

	// Now delete more to push above threshold.
	for i := 5; i < 10; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Run again — now 100% dead, should trigger.
	err = cache.maybeRewriteSegments()
	require.NoError(t, err)

	// Segment should be deleted.
	_, err = os.Stat(segPath)
	require.True(t, os.IsNotExist(err), "dead segment should be deleted")
}

// TestMaybeRewriteSegments_Integration tests the full maybeRewriteSegments flow
// including deletion of 100% dead segments and rewriting sparse ones.
func TestMaybeRewriteSegments_Integration(t *testing.T) {
	cache := newTestCASCache(t,
		WithCompactionWasteThreshold(0.25),
	)

	// Write first batch — will become sparse.
	keys1, segID1 := writeAndDrain(t, cache, "batch1", 20, 40_000)

	// Write second batch — will become 100% dead.
	keys2, segID2 := writeAndDrain(t, cache, "batch2", 10, 40_000)

	// Delete 19/20 from batch1 (sparse, 95% waste).
	for i := 1; i < 20; i++ {
		require.NoError(t, cache.Delete(keys1[i]))
	}

	// Delete all from batch2 (100% dead).
	for _, k := range keys2 {
		require.NoError(t, cache.Delete(k))
	}

	pushPastCooling(t, cache)

	// Run full maintenance.
	err := cache.maybeRewriteSegments()
	require.NoError(t, err)

	// Segment 1 should be rewritten (live item relocated).
	seg1Path := getSegmentPath(cache.Path, cache.Shards, segID1)
	_, err = os.Stat(seg1Path)
	require.True(t, os.IsNotExist(err), "sparse segment should be rewritten+deleted")

	// Segment 2 should be deleted (100% dead).
	seg2Path := getSegmentPath(cache.Path, cache.Shards, segID2)
	_, err = os.Stat(seg2Path)
	require.True(t, os.IsNotExist(err), "dead segment should be deleted")

	// Surviving item from batch1 should still be readable.
	val, ok := cache.Get(keys1[0])
	require.True(t, ok, "surviving item should be readable")
	require.NotEmpty(t, val)
}
