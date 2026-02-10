package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// TestSegmentDrain_Basic validates the full segment drain flow in cache mode:
// write items, delete 90%+, drain sparse segments, verify cleanup.
func TestSegmentDrain_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),         // Force disk path
		WithDrainWasteThreshold(0.90), // Drain segments that are 90%+ dead
		WithWriteBufferSize(1<<20),    // 1MB segments for faster testing
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write enough items to fill a segment.
	const numItems = 20
	value := make([]byte, 40_000) // 40KB each → ~800KB total in one segment
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "drain-key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Identify the segment ID from one of the items.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete 19 of 20 items (95% waste → above 90% threshold).
	for i := 1; i < numItems; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Create several more segments to push the target segment past cooling period.
	for range cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1 {
		require.NoError(t, cache.Put([]byte("filler"), make([]byte, 200_000)))
		cache.Drain()
	}

	// Run drain directly.
	err = cache.maybeDrainSegments()
	require.NoError(t, err)

	// Verify: the segment file should be deleted.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.True(t, os.IsNotExist(err), "segment file should be deleted after drain")

	// Verify: remaining live item (keys[0]) should be a cache miss (drained from RAM).
	_, ok := cache.Get(keys[0])
	require.False(t, ok, "drained live item should become a cache miss")

	// Verify: the key is no longer in the RAM index.
	_, found = cache.index.Get(h)
	require.False(t, found, "drained item should be removed from RAM index")
}

// TestSegmentDrain_BelowThreshold verifies drain is a no-op for segments below
// the waste threshold.
func TestSegmentDrain_BelowThreshold(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),
		WithDrainWasteThreshold(0.90),
		WithWriteBufferSize(1<<20),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write items.
	const numItems = 20
	value := make([]byte, 40_000)
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "below-key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Get segment ID.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete only 50% of items (below 90% threshold).
	for i := range numItems / 2 {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Create segments to push past cooling.
	for range cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1 {
		require.NoError(t, cache.Put([]byte("filler"), make([]byte, 200_000)))
		cache.Drain()
	}

	// Run drain — should be a no-op (50% waste < 90% threshold).
	err = cache.maybeDrainSegments()
	require.NoError(t, err)

	// Verify: segment file still exists.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "segment file should still exist (below threshold)")

	// Verify: live items still accessible.
	for i := numItems / 2; i < numItems; i++ {
		_, ok := cache.Get(keys[i])
		require.True(t, ok, "live item %d should still be readable", i)
	}
}

// TestSegmentDrain_CoolingPeriod verifies that recent segments are not drained
// even if they exceed the waste threshold.
func TestSegmentDrain_CoolingPeriod(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),
		WithDrainWasteThreshold(0.90),
		WithWriteBufferSize(1<<20),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write items.
	const numItems = 20
	value := make([]byte, 40_000)
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "cool-key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Get segment ID.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete 95% of items.
	for i := 1; i < numItems; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Do NOT create enough segments to push past cooling period.
	// Run drain — should be a no-op (segment too recent).
	err = cache.maybeDrainSegments()
	require.NoError(t, err)

	// Verify: segment file still exists (within cooling period).
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "segment file should still exist (within cooling period)")

	// Now create enough segments to push past cooling period.
	for range cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1 {
		require.NoError(t, cache.Put([]byte("filler"), make([]byte, 200_000)))
		cache.Drain()
	}

	// Run drain again — now should drain the sparse segment.
	err = cache.maybeDrainSegments()
	require.NoError(t, err)

	// Verify: segment file should be deleted.
	_, err = os.Stat(segPath)
	require.True(t, os.IsNotExist(err), "segment file should be deleted after cooling period")
}

// TestSegmentDrain_WALModeSkipped verifies that segment drain is NOT triggered
// in WAL/CAS mode (WAL mode uses tombstone compaction instead).
func TestSegmentDrain_WALModeSkipped(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),
		WithMaxCachedSlabs(0),
		WithDrainWasteThreshold(0.90),
		WithWriteBufferSize(1<<20),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write items.
	const numItems = 20
	value := make([]byte, 40_000)
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "wal-key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Get segment ID.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete 95% of items.
	for i := 1; i < numItems; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Create segments to push past cooling.
	for range cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1 {
		require.NoError(t, cache.Put([]byte("filler"), make([]byte, 200_000)))
		cache.Drain()
	}

	// In WAL mode, maintenanceWorker skips drain (Phase 3 is guarded by c.wal == nil).
	// We verify the segment file is still present.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "WAL mode should NOT drain segments")

	// Live item should still be accessible.
	_, ok := cache.Get(keys[0])
	require.True(t, ok, "live item should still be readable in WAL mode")
}

// TestSegmentDrain_Disabled verifies drain is skipped when threshold is 0.
func TestSegmentDrain_Disabled(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),
		WithDrainWasteThreshold(0), // Disabled
		WithWriteBufferSize(1<<20),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write items.
	const numItems = 20
	value := make([]byte, 40_000)
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "disabled-key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Get segment ID.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete 95% of items.
	for i := 1; i < numItems; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Create segments to push past cooling.
	for range cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1 {
		require.NoError(t, cache.Put([]byte("filler"), make([]byte, 200_000)))
		cache.Drain()
	}

	// Run drain — should be a no-op (disabled).
	err = cache.maybeDrainSegments()
	require.NoError(t, err)

	// Verify: segment file still exists.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "segment file should still exist (drain disabled)")
}
