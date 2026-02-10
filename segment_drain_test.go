package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// TestSegmentDrain_Basic validates the full pressure-driven drain flow:
// when on-disk waste exceeds MaxSize/2, drain the sparsest segments.
func TestSegmentDrain_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),      // Force disk path
		WithWriteBufferSize(1<<20), // 1MB segments for faster testing
		WithMaxSize(2<<20),         // 2MB — waste allowance = 1MB
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

	// Delete 19 of 20 items — makes this segment the sparsest (~40KB live).
	for i := 1; i < numItems; i++ {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Create enough filler segments for BOTH cooling period AND waste pressure.
	// Use unique keys so each filler segment retains live data — otherwise the
	// filler segments become empty (sparsest) and get drained before the target.
	coolingFillers := cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1
	for i := range max(coolingFillers, 4) {
		key := fmt.Appendf(nil, "filler-%04d", i)
		require.NoError(t, cache.Put(key, make([]byte, 200_000)))
		cache.Drain()
	}

	// Run drain directly — waste pressure should trigger drain of sparsest segment.
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

// TestSegmentDrain_NoPressure verifies drain is a no-op when on-disk waste
// is within the allowance (MaxSize/2).
func TestSegmentDrain_NoPressure(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),
		WithWriteBufferSize(1<<20),
		WithMaxSize(100<<20), // 100MB — waste allowance = 50MB, way above a few 1MB segments
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write items.
	const numItems = 20
	value := make([]byte, 40_000)
	keys := make([][]byte, numItems)
	for i := range numItems {
		keys[i] = fmt.Appendf(nil, "nopressure-key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Get segment ID.
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete 50% of items — segment has waste, but insufficient pressure.
	for i := range numItems / 2 {
		require.NoError(t, cache.Delete(keys[i]))
	}

	// Create segments to push past cooling.
	for i := range cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1 {
		key := fmt.Appendf(nil, "filler-%04d", i)
		require.NoError(t, cache.Put(key, make([]byte, 200_000)))
		cache.Drain()
	}

	// Run drain — should be a no-op (waste well within allowance).
	err = cache.maybeDrainSegments()
	require.NoError(t, err)

	// Verify: segment file still exists.
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	_, err = os.Stat(segPath)
	require.NoError(t, err, "segment file should still exist (no waste pressure)")

	// Verify: live items still accessible.
	for i := numItems / 2; i < numItems; i++ {
		_, ok := cache.Get(keys[i])
		require.True(t, ok, "live item %d should still be readable", i)
	}
}

// TestSegmentDrain_CoolingPeriod verifies that recent segments are not drained
// even under waste pressure.
func TestSegmentDrain_CoolingPeriod(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0),
		WithWriteBufferSize(1<<20),
		WithMaxSize(1<<20), // 1MB — waste allowance = 512KB, immediate pressure
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
	// Use unique keys so filler segments stay populated.
	coolingFillers := cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1
	for i := range max(coolingFillers, 4) {
		key := fmt.Appendf(nil, "filler-%04d", i)
		require.NoError(t, cache.Put(key, make([]byte, 200_000)))
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
		WithWriteBufferSize(1<<20),
		WithMaxSize(2<<20), // 2MB — would trigger drain in cache mode
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

	// Create enough segments to push past cooling AND waste pressure.
	coolingFillers := cache.MaxCachedSlabs + index.CoolingPeriodMargin + 1
	for i := range max(coolingFillers, 4) {
		key := fmt.Appendf(nil, "filler-%04d", i)
		require.NoError(t, cache.Put(key, make([]byte, 200_000)))
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
