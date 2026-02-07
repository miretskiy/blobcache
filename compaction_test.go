package blobcache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/stretchr/testify/require"
)

// --- Unit Tests ---

func TestCompactor_LeapfrogHazard(t *testing.T) {
	// The Leapfrog Hazard: compacting non-contiguous segments could resurrect deleted keys.
	// Example: Segments [1, 3] (skipping 2) - if segment 2 contains a delete for a key
	// in segment 1, compacting 1 and 3 without 2 would resurrect the deleted key.

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	segIDs.counter.Store(100)

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	// Case 1: Gap with segment present - should fail
	// Create segment 2 (exists in gap between 1 and 3)
	seg2Items := []index.Item{
		{Key: index.Key{Lo: 200, Hi: 0}, SegmentID: 2, Offset: 0, PhysicalLen: 100},
	}
	// Write .meta file so the gap check can find it
	writeTestMeta(t, GetFooterPath(tmpDir, 0, 2), 2, seg2Items)
	idx.AddSegment(0, seg2Items)

	_, err = c.Compact([]uint32{1, 3}, false)
	require.Error(t, err, "should fail when segment exists in gap")
	require.Contains(t, err.Error(), "segment 2 exists in gap")
	require.Contains(t, err.Error(), "Leapfrog Hazard")

	// Case 2: Gap without segment - should succeed
	// Delete segment 2 from index
	require.NoError(t, idx.DeleteSegment(2))

	_, err = c.Compact([]uint32{1, 3}, false)
	require.NoError(t, err, "should succeed when gap is empty (segments were deleted)")
}

func TestCompactor_ContiguityValidation(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	segIDs.counter.Store(100)

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	testCases := []struct {
		name      string
		segments  []uint32
		wantError bool
		errRegex  string
	}{
		{"empty", []uint32{}, false, ""},
		{"single", []uint32{5}, false, ""},
		{"contiguous pair", []uint32{1, 2}, false, ""},
		{"contiguous triple", []uint32{10, 11, 12}, false, ""},
		{"gap of 1 (empty)", []uint32{1, 3}, false, ""},                             // Gap OK if no segment exists
		{"gap of 2 (empty)", []uint32{1, 4}, false, ""},                             // Gap OK if no segment exists
		{"out of order", []uint32{3, 2, 1}, true, "ascending order.*got 2 after 3"}, // Not ascending
		{"duplicate", []uint32{1, 1}, true, "ascending order.*got 1 after 1"},       // Not ascending
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Compact(tc.segments, false)
			if tc.wantError {
				require.Error(t, err, "expected error for segments %v", tc.segments)
				if tc.errRegex != "" {
					require.Regexp(t, tc.errRegex, err)
				}
			} else {
				require.NoError(t, err, "unexpected error for segments %v", tc.segments)
			}
		})
	}
}

func TestCompactor_EmptySegments(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	// Ingest empty batches to create segment manifests with no items
	idx.AddSegment(0, nil)
	idx.AddSegment(0, nil)

	result, err := c.Compact([]uint32{1, 2}, false)
	require.NoError(t, err)
	require.Equal(t, uint32(0), result.NewSegmentID, "no new segment should be created for empty input")
	require.Equal(t, 0, result.ItemsCompacted)
	require.Equal(t, 0, result.TombstonesKept)
}

func TestCompactor_StalenessFiltering(t *testing.T) {
	// Items that have been overwritten (stale) should be filtered out during compaction.
	// The RAM index has the authoritative location.

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID() // Allocate source segment ID

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	key1 := index.Key{Lo: 1, Hi: 1}
	key2 := index.Key{Lo: 2, Hi: 2}

	// Write source segment with two items
	blob1 := makeTestBlob(t, key1, []byte("value1"), 1)
	blob2 := makeTestBlob(t, key2, []byte("value2"), 2)

	segPath := getSegmentPath(tmpDir, 0, sourceSegID)
	writeTestSegment(t, segPath, blob1, blob2)

	items := []index.Item{
		{Key: key1, SegmentID: sourceSegID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob1))},
		{Key: key2, SegmentID: sourceSegID, Offset: uint32(record.FileHeaderSize + len(blob1)), PhysicalLen: uint32(len(blob2))},
	}
	// Write .meta file so compaction can read the manifest
	writeTestMeta(t, SegmentMetaPath(segPath), sourceSegID, items)
	idx.AddSegment(0, items)

	// Simulate key2 being overwritten to a future segment by updating RAM index
	// The manifest still has key2 at sourceSegID, but RAM index says segment 99
	idx.Put(index.Item{Key: key2, SegmentID: 99, Offset: 500, PhysicalLen: 150})

	result, err := c.Compact([]uint32{sourceSegID}, false)
	require.NoError(t, err)

	// Only key1 should be compacted (key2 is stale - RAM says it's in segment 5)
	require.Equal(t, 1, result.ItemsCompacted)
	require.Equal(t, 0, result.TombstonesKept)

	// Verify key1 was relocated to new segment
	item, found := idx.Get(key1)
	require.True(t, found)
	require.Equal(t, result.NewSegmentID, item.SegmentID)

	// key2 should still point to segment 99 (unchanged)
	item, found = idx.Get(key2)
	require.True(t, found)
	require.Equal(t, uint32(99), item.SegmentID)
}

func TestCompactor_TombstonePreservation(t *testing.T) {
	// Tombstones must be preserved during non-tail compaction when older segments
	// exist that might contain the key. This prevents resurrection of deleted keys.
	//
	// Setup: segment 1 has live data for keyDeleted, segment 3 has a tombstone.
	// Compacting only segment 3 must preserve the tombstone because segment 1
	// (older shadow) still exists.

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	segIDs.counter.Store(100) // Output segments start at 101

	// Segment 1: older segment with live data for keyDeleted (the "older shadow")
	const seg1ID uint32 = 1
	keyDeleted := index.Key{Lo: 2, Hi: 2}
	blob1 := makeTestBlob(t, keyDeleted, []byte("old-value"), 1)
	segPath1 := getSegmentPath(tmpDir, 0, seg1ID)
	writeTestSegment(t, segPath1, blob1)

	items1 := []index.Item{
		{Key: keyDeleted, SegmentID: seg1ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob1))},
	}
	writeTestMeta(t, SegmentMetaPath(segPath1), seg1ID, items1)
	idx.AddSegment(seg1ID, items1)

	// Segment 3: newer segment with one live item + tombstone for keyDeleted
	const seg3ID uint32 = 3
	key1 := index.Key{Lo: 1, Hi: 1}
	blob3 := makeTestBlob(t, key1, []byte("value1"), 3)
	segPath3 := getSegmentPath(tmpDir, 0, seg3ID)
	writeTestSegment(t, segPath3, blob3)

	deletedItem := index.Item{Key: keyDeleted, SegmentID: seg3ID, Offset: 0, PhysicalLen: 0}
	deletedItem.SetDeleted()

	items3 := []index.Item{
		{Key: key1, SegmentID: seg3ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob3))},
		deletedItem,
	}
	writeTestMeta(t, SegmentMetaPath(segPath3), seg3ID, items3)
	idx.AddSegment(seg3ID, items3)

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	// Compact only segment 3 — segment 1 is the older shadow
	result, err := c.Compact([]uint32{seg3ID}, false)
	require.NoError(t, err)

	require.Equal(t, 1, result.ItemsCompacted)
	require.Equal(t, 1, result.TombstonesKept, "tombstone must be preserved with older shadow")

	// Verify the tombstone was persisted in the new segment's manifest
	manifest, found := idx.GetSegmentManifest(result.NewSegmentID)
	require.True(t, found)

	var foundTombstone bool
	for _, item := range manifest.Items {
		if item.Key == keyDeleted {
			require.True(t, item.IsDeleted())
			foundTombstone = true
		}
	}
	require.True(t, foundTombstone, "tombstone should be in compacted segment manifest")
}

func TestCompactor_TombstoneDissolution(t *testing.T) {
	// Tombstone dissolution allows safe GC of tombstones during compaction when
	// no older segment contains the key (via HasOlderShadow RAM index check).
	//
	// Test scenarios:
	// 1. Key only in segment 5, compacting [5] -> dissolve (no older shadow)
	// 2. Key in segments 3 and 5, compacting [5] -> preserve (segment 3 is older shadow)
	// 3. Key in segments 3 and 5, compacting [3,5] -> dissolve (floor=3, no segment < 3)

	t.Run("DissolveWhenNoOlderShadow", func(t *testing.T) {
		tmpDir := t.TempDir()
		setupTestDirs(t, tmpDir)

		idx, err := index.OpenIndex(tmpDir, 0, 100)
		require.NoError(t, err)
		defer idx.Close()

		archivist := NewArchivist(config{Path: tmpDir}, idx)
		defer archivist.Close()

		segIDs := &segmentIDProvider{}
		sourceSegID := segIDs.NextSegmentID()

		c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

		// Create source segment with one live item and one tombstone
		keyLive := index.Key{Lo: 1, Hi: 1}
		keyDeleted := index.Key{Lo: 2, Hi: 2}

		blob1 := makeTestBlob(t, keyLive, []byte("value1"), 1)
		segPath := getSegmentPath(tmpDir, 0, sourceSegID)
		writeTestSegment(t, segPath, blob1)

		deletedItem := index.Item{Key: keyDeleted, SegmentID: sourceSegID, Offset: 0, PhysicalLen: 0}
		deletedItem.SetDeleted()

		items := []index.Item{
			{Key: keyLive, SegmentID: sourceSegID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob1))},
			deletedItem,
		}
		writeTestMeta(t, SegmentMetaPath(segPath), sourceSegID, items)
		idx.AddSegment(sourceSegID, items)

		// Mark the key as deleted in RAM (simulating what Delete() does)
		idx.MarkDeleted(keyDeleted)

		// Compact with dropTombstones=false (NOT tail segment)
		// Since key is not in RAM (or only in compaction range), tombstone should be DISSOLVED
		result, err := c.Compact([]uint32{sourceSegID}, false)
		require.NoError(t, err)

		// Tombstone should be dissolved, not kept
		require.Equal(t, 1, result.ItemsCompacted)
		require.Equal(t, 0, result.TombstonesKept, "tombstone should be dissolved, not kept")
		require.Equal(t, 1, result.TombstonesDissolved, "tombstone should be dissolved")
		require.Equal(t, 0, result.TombstonesDropped, "not a tail GC")

		// Verify tombstone not in new segment's manifest
		manifest, found := idx.GetSegmentManifest(result.NewSegmentID)
		require.True(t, found)
		for _, item := range manifest.Items {
			require.NotEqual(t, keyDeleted, item.Key, "dissolved tombstone should not be in manifest")
		}

		// Verify tombstone removed from RAM
		_, foundInRAM := idx.Get(keyDeleted)
		require.False(t, foundInRAM, "dissolved tombstone should be removed from RAM")
	})

	t.Run("PreserveWhenOlderShadowExists", func(t *testing.T) {
		tmpDir := t.TempDir()
		setupTestDirs(t, tmpDir)

		idx, err := index.OpenIndex(tmpDir, 0, 100)
		require.NoError(t, err)
		defer idx.Close()

		archivist := NewArchivist(config{Path: tmpDir}, idx)
		defer archivist.Close()

		// Use fixed segment IDs for clarity; set counter high for compaction output
		segIDs := &segmentIDProvider{}
		segIDs.counter.Store(100)

		// Create segment 3 with keyDeleted (this is the "older shadow")
		const seg3ID uint32 = 3
		keyDeleted := index.Key{Lo: 100, Hi: 100}
		blob3 := makeTestBlob(t, keyDeleted, []byte("old-value"), 1)
		segPath3 := getSegmentPath(tmpDir, 0, seg3ID)
		writeTestSegment(t, segPath3, blob3)

		items3 := []index.Item{
			{Key: keyDeleted, SegmentID: seg3ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob3))},
		}
		writeTestMeta(t, SegmentMetaPath(segPath3), seg3ID, items3)
		idx.AddSegment(seg3ID, items3)

		// Create segment 5 with tombstone for keyDeleted
		const seg5ID uint32 = 5
		keyLive := index.Key{Lo: 200, Hi: 200}
		blob5 := makeTestBlob(t, keyLive, []byte("value5"), 5)
		segPath5 := getSegmentPath(tmpDir, 0, seg5ID)
		writeTestSegment(t, segPath5, blob5)

		deletedItem := index.Item{Key: keyDeleted, SegmentID: seg5ID, Offset: 0, PhysicalLen: 0}
		deletedItem.SetDeleted()

		items5 := []index.Item{
			{Key: keyLive, SegmentID: seg5ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob5))},
			deletedItem,
		}
		writeTestMeta(t, SegmentMetaPath(segPath5), seg5ID, items5)
		idx.AddSegment(seg5ID, items5)
		idx.MarkDeleted(keyDeleted)

		c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

		// Compact only segment 5 - segment 3 is the older shadow
		result, err := c.Compact([]uint32{seg5ID}, false)
		require.NoError(t, err)

		// Tombstone should be PRESERVED because segment 3 might have the key
		require.Equal(t, 1, result.ItemsCompacted)
		require.Equal(t, 1, result.TombstonesKept, "tombstone must be preserved when older shadow exists")
		require.Equal(t, 0, result.TombstonesDissolved, "should not dissolve with older shadow")

		// Verify tombstone IS in new segment's manifest
		manifest, found := idx.GetSegmentManifest(result.NewSegmentID)
		require.True(t, found)

		var foundTombstone bool
		for _, item := range manifest.Items {
			if item.Key == keyDeleted && item.IsDeleted() {
				foundTombstone = true
			}
		}
		require.True(t, foundTombstone, "tombstone must be in compacted manifest")
	})

	t.Run("DissolveWhenCompactingWithOlderShadow", func(t *testing.T) {
		// Key exists in segments 3 and 5, compacting [3,4,5] together
		// floor=3, and no segment < 3 exists, so dissolution should happen
		tmpDir := t.TempDir()
		setupTestDirs(t, tmpDir)

		idx, err := index.OpenIndex(tmpDir, 0, 100)
		require.NoError(t, err)
		defer idx.Close()

		archivist := NewArchivist(config{Path: tmpDir}, idx)
		defer archivist.Close()

		// Use fixed segment IDs for clarity; set counter high for compaction output
		segIDs := &segmentIDProvider{}
		segIDs.counter.Store(100)

		// Create segment 3 with keyDeleted
		const seg3ID uint32 = 3
		keyDeleted := index.Key{Lo: 300, Hi: 300}
		blob3 := makeTestBlob(t, keyDeleted, []byte("old-value"), 1)
		segPath3 := getSegmentPath(tmpDir, 0, seg3ID)
		writeTestSegment(t, segPath3, blob3)

		items3 := []index.Item{
			{Key: keyDeleted, SegmentID: seg3ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob3))},
		}
		writeTestMeta(t, SegmentMetaPath(segPath3), seg3ID, items3)
		idx.AddSegment(seg3ID, items3)

		// Create segment 4 (to ensure contiguity) with unrelated key
		const seg4ID uint32 = 4
		keyOther := index.Key{Lo: 400, Hi: 400}
		blob4 := makeTestBlob(t, keyOther, []byte("other"), 4)
		segPath4 := getSegmentPath(tmpDir, 0, seg4ID)
		writeTestSegment(t, segPath4, blob4)

		items4 := []index.Item{
			{Key: keyOther, SegmentID: seg4ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob4))},
		}
		writeTestMeta(t, SegmentMetaPath(segPath4), seg4ID, items4)
		idx.AddSegment(seg4ID, items4)

		// Create segment 5 with tombstone for keyDeleted
		const seg5ID uint32 = 5
		keyLive := index.Key{Lo: 500, Hi: 500}
		blob5 := makeTestBlob(t, keyLive, []byte("value5"), 5)
		segPath5 := getSegmentPath(tmpDir, 0, seg5ID)
		writeTestSegment(t, segPath5, blob5)

		deletedItem := index.Item{Key: keyDeleted, SegmentID: seg5ID, Offset: 0, PhysicalLen: 0}
		deletedItem.SetDeleted()

		items5 := []index.Item{
			{Key: keyLive, SegmentID: seg5ID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob5))},
			deletedItem,
		}
		writeTestMeta(t, SegmentMetaPath(segPath5), seg5ID, items5)
		idx.AddSegment(seg5ID, items5)
		idx.MarkDeleted(keyDeleted)

		c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

		// Compact segments [3,4,5] - floor=3, no segment < 3 exists
		result, err := c.Compact([]uint32{seg3ID, seg4ID, seg5ID}, false)
		require.NoError(t, err)

		// The tombstone should be dissolved because:
		// - floor = 3 (minimum segment being compacted)
		// - No segment with ID < 3 exists
		// - Therefore no older shadow
		require.Equal(t, 2, result.ItemsCompacted) // keyOther from seg4, keyLive from seg5
		require.Equal(t, 0, result.TombstonesKept, "no tombstones should be kept")
		require.Equal(t, 1, result.TombstonesDissolved, "tombstone should be dissolved")

		// Verify tombstone not in manifest
		manifest, found := idx.GetSegmentManifest(result.NewSegmentID)
		require.True(t, found)
		for _, item := range manifest.Items {
			require.False(t, item.IsDeleted(), "no tombstones should exist in compacted manifest")
		}
	})
}

func TestCompactor_TombstoneDropping(t *testing.T) {
	// When dropTombstones=true (tail segment), tombstones should be garbage collected
	// from BOTH disk (manifest) AND RAM (BlobIndex).
	//
	// This test verifies the fix for the memory leak where tombstones were removed
	// from the manifest but left as zombie entries in the RAM index.

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID()

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	key1 := index.Key{Lo: 1, Hi: 1}
	keyDeleted := index.Key{Lo: 2, Hi: 2}

	// Write source segment with one live item
	blob1 := makeTestBlob(t, key1, []byte("value1"), 1)
	segPath := getSegmentPath(tmpDir, 0, sourceSegID)
	writeTestSegment(t, segPath, blob1)

	// Create items: one live, one tombstone
	deletedItem := index.Item{Key: keyDeleted, SegmentID: sourceSegID, Offset: 0, PhysicalLen: 0}
	deletedItem.SetDeleted()

	items := []index.Item{
		{Key: key1, SegmentID: sourceSegID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob1))},
		deletedItem,
	}
	// Write .meta file so compaction can read the manifest
	writeTestMeta(t, SegmentMetaPath(segPath), sourceSegID, items)
	idx.AddSegment(sourceSegID, items)

	// Simulate real deletion scenario: markDeleted sets the flag but KEEPS item in RAM.
	// This is what happens when Delete() is called - the item stays in RAM with IsDeleted=true.
	// We use MarkDeleted (not Delete) to preserve the entry for the memory leak test.
	idx.MarkDeleted(keyDeleted)

	// Verify the tombstone is still in RAM before compaction (marked as deleted)
	itemBefore, foundBefore := idx.Get(keyDeleted)
	require.True(t, foundBefore, "tombstone should be in RAM before compaction")
	require.True(t, itemBefore.IsDeleted(), "item should be marked as deleted")

	// Compact with dropTombstones=true (simulating tail segment compaction)
	result, err := c.Compact([]uint32{sourceSegID}, true)
	require.NoError(t, err)

	// At the tail, there are no older segments, so HasOlderShadow returns false
	// and tombstones are dissolved during collectItems (before the dropTombstones
	// path runs). Both dissolution and dropping achieve the same result: tombstone
	// removed from disk and RAM.
	require.Equal(t, 1, result.ItemsCompacted)
	require.Equal(t, 0, result.TombstonesKept, "tombstones should not be kept")
	require.Equal(t, 1, result.TombstonesDissolved, "tail: dissolved because no older shadow")

	// Verify the tombstone was NOT persisted in the new segment's manifest
	manifest, found := idx.GetSegmentManifest(result.NewSegmentID)
	require.True(t, found)

	for _, item := range manifest.Items {
		require.NotEqual(t, keyDeleted, item.Key, "tombstone should not be in compacted manifest")
	}

	// CRITICAL: Verify tombstone is gone from RAM index (fix for memory leak)
	_, foundInRAM := idx.Get(keyDeleted)
	require.False(t, foundInRAM, "tombstone should be removed from RAM index after tail GC")
}

func TestCompactor_ConcurrentWriteRace(t *testing.T) {
	// Test the scenario where a concurrent write happens between reading the manifest
	// and performing the relocation. The relocation should fail (not overwrite newer data).

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID() // Allocate source segment ID

	key1 := index.Key{Lo: 1, Hi: 1}

	// Write source segment with one item
	blob1 := makeTestBlob(t, key1, []byte("value1"), 1)
	segPath := getSegmentPath(tmpDir, 0, sourceSegID)
	writeTestSegment(t, segPath, blob1)

	items := []index.Item{
		{Key: key1, SegmentID: sourceSegID, Offset: record.FileHeaderSize, PhysicalLen: uint32(len(blob1))},
	}
	// Write .meta file so compaction can read the manifest
	writeTestMeta(t, SegmentMetaPath(segPath), sourceSegID, items)
	idx.AddSegment(0, items)

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)

	// Use testing knobs to inject concurrent write during relocation
	c.Knobs = &CompactorKnobs{
		BeforeRelocate: func(k index.Key) {
			if k == key1 {
				// Simulate concurrent write - update RAM index to point to segment 99
				idx.Put(index.Item{Key: key1, SegmentID: 99, Offset: 999, PhysicalLen: 200})
			}
		},
	}

	result, err := c.Compact([]uint32{sourceSegID}, false)
	require.NoError(t, err)

	// Compaction should succeed (data was written to new segment)
	require.Equal(t, 1, result.ItemsCompacted)

	// But RAM index should still point to segment 99 (the "concurrent write" location)
	// because the relocation should have failed
	item, found := idx.Get(key1)
	require.True(t, found)
	require.Equal(t, uint32(99), item.SegmentID, "relocation should fail, RAM should point to concurrent write location")
}

func TestCompactor_XLBlobHandling(t *testing.T) {
	// Test compaction of blobs that are larger than the compaction buffer.
	// This mirrors how MemTable handles XL writes - blobs exceeding WriteBufferSize
	// get special handling (interleaved at page boundaries).
	//
	// For compaction, when a blob is larger than the buffer, we must:
	// 1. Flush any pending data in the buffer
	// 2. Read the XL blob directly and write it directly (bypass buffer)

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)

	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID()

	// Create test data:
	// - Small blob: 1KB (fits in buffer)
	// - XL blob: 32KB (larger than our 8KB test buffer)
	// - Another small blob: 1KB
	key1 := index.Key{Lo: 1, Hi: 1}
	key2 := index.Key{Lo: 2, Hi: 2} // XL
	key3 := index.Key{Lo: 3, Hi: 3}

	smallValue := bytes.Repeat([]byte("A"), 1024)
	xlValue := bytes.Repeat([]byte("X"), 32*1024) // 32KB - larger than 8KB buffer
	smallValue2 := bytes.Repeat([]byte("B"), 1024)

	blob1 := makeTestBlob(t, key1, smallValue, 1)
	blob2 := makeTestBlob(t, key2, xlValue, 2)
	blob3 := makeTestBlob(t, key3, smallValue2, 3)

	// Write source segment with all three blobs
	segPath := getSegmentPath(tmpDir, 0, sourceSegID)
	f, err := os.Create(segPath)
	require.NoError(t, err)

	// Write file header
	_, err = f.Write(record.FileHeaderBytes[:])
	require.NoError(t, err)

	offset1 := record.FileHeaderSize
	_, err = f.Write(blob1)
	require.NoError(t, err)

	offset2 := offset1 + len(blob1)
	_, err = f.Write(blob2)
	require.NoError(t, err)

	offset3 := offset2 + len(blob2)
	_, err = f.Write(blob3)
	require.NoError(t, err)

	require.NoError(t, f.Close())

	items := []index.Item{
		{Key: key1, SegmentID: sourceSegID, Offset: uint32(offset1), PhysicalLen: uint32(len(blob1))},
		{Key: key2, SegmentID: sourceSegID, Offset: uint32(offset2), PhysicalLen: uint32(len(blob2))},
		{Key: key3, SegmentID: sourceSegID, Offset: uint32(offset3), PhysicalLen: uint32(len(blob3))},
	}
	writeTestMeta(t, SegmentMetaPath(segPath), sourceSegID, items)
	idx.AddSegment(0, items)

	c := NewCompactor(idx, segIDs, tmpDir, 0, sys.SyncNone, archivist.DropSegmentCache)
	defer c.Close()

	result, err := c.Compact([]uint32{sourceSegID}, false)
	require.NoError(t, err, "compaction should handle XL blobs")
	require.Equal(t, 3, result.ItemsCompacted, "all three blobs should be compacted")

	// Verify all items are accessible in the new segment
	for _, key := range []index.Key{key1, key2, key3} {
		item, found := idx.Get(key)
		require.True(t, found, "key %v should exist after compaction", key)
		require.Equal(t, result.NewSegmentID, item.SegmentID, "key %v should be in new segment", key)
	}

	// Verify XL blob data integrity by reading from new segment
	xlItem, _ := idx.Get(key2)
	newSegPath := getSegmentPath(tmpDir, 0, xlItem.SegmentID)
	newSegFile, err := os.Open(newSegPath)
	require.NoError(t, err)
	defer newSegFile.Close()

	readBuf := make([]byte, xlItem.PhysicalLen)
	_, err = newSegFile.ReadAt(readBuf, int64(xlItem.Offset))
	require.NoError(t, err)

	// Decode and verify the value
	hdr, err := record.DecodeHeader(readBuf[:record.HeaderSize])
	require.NoError(t, err)
	valueStart := record.HeaderSize + int(hdr.KeyLen)
	recoveredValue := readBuf[valueStart : valueStart+int(hdr.PhysicalSize)]
	require.Equal(t, xlValue, recoveredValue, "XL blob data should be preserved")
}

// TestCompactor_SmallBlobAlignment verifies compaction handles records whose
// headers straddle 4KB block boundaries. Each record is 59 bytes
// (Header=42 + Key=16 + Value=1). After ~69 records, the next header
// starts near offset 4079 and spans the 4096 boundary.
//
// / IMPORTANT: This test uses O_DIRECT and MUST run on a real filesystem
// (XFS/ext4), not tmpfs. tmpfs silently ignores O_DIRECT, masking
// alignment bugs. On Linux with /instance_storage available, the test
// runs there; otherwise it falls back to t.TempDir() (may not catch bugs).
func TestCompactor_SmallBlobAlignment(t *testing.T) {
	tmpDir := t.TempDir()
	// Prefer real filesystem for O_DIRECT testing
	if dir := "/instance_storage"; sys.RequiresAlignment {
		sub := filepath.Join(dir, fmt.Sprintf("compaction_test_%d", time.Now().UnixNano()))
		if err := os.MkdirAll(sub, 0755); err == nil {
			tmpDir = sub
			t.Cleanup(func() { os.RemoveAll(sub) })
		}
	}
	setupTestDirs(t, tmpDir)
	idx, err := index.OpenIndex(tmpDir, 0, 100)
	require.NoError(t, err)
	defer idx.Close()
	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	segIDs.counter.Store(100)

	const sourceSegID = 1
	const numRecords = 100 // Enough to force multiple block-boundary crossings

	// Build records — each ~59 bytes (Header=42 + Key=16 + Value=1)
	type keyVal struct {
		key   index.Key
		value []byte
	}
	records := make([]keyVal, numRecords)
	blobs := make([][]byte, numRecords)
	for i := range numRecords {
		records[i] = keyVal{
			key:   index.Key{Lo: uint64(i + 1), Hi: uint64(i + 1)},
			value: []byte{byte(i)},
		}
		blobs[i] = makeTestBlob(t, records[i].key, records[i].value, uint64(i+1))
	}

	// Write all records into one segment
	segPath := getSegmentPath(tmpDir, 0, sourceSegID)
	writeTestSegment(t, segPath, blobs...)

	// Build index items with correct offsets
	offset := record.FileHeaderSize
	items := make([]index.Item, numRecords)
	for i := range numRecords {
		items[i] = index.Item{
			Key:         records[i].key,
			SegmentID:   sourceSegID,
			Offset:      uint32(offset),
			PhysicalLen: uint32(len(blobs[i])),
		}
		offset += len(blobs[i])
	}
	writeTestMeta(t, SegmentMetaPath(segPath), sourceSegID, items)
	idx.AddSegment(0, items)

	// Use FlDirectIO to exercise real O_DIRECT alignment on Linux.
	// IMPORTANT: This test MUST run on a real filesystem (XFS/ext4), not tmpfs.
	// tmpfs silently ignores O_DIRECT, masking alignment bugs.
	// Set BLOBCACHE_TEST_DIR to a real filesystem path (e.g. /instance_storage/test).
	ioFlags := sys.FlDirectIO
	c := NewCompactor(idx, segIDs, tmpDir, 0, ioFlags, archivist.DropSegmentCache)
	defer c.Close()

	result, err := c.Compact([]uint32{sourceSegID}, false)
	require.NoError(t, err, "compaction should handle small records across block boundaries")
	require.Equal(t, numRecords, result.ItemsCompacted)

	// Verify every record is readable from the compacted segment
	for i := range numRecords {
		item, found := idx.Get(records[i].key)
		require.True(t, found, "key %d should exist after compaction", i)
		require.Equal(t, result.NewSegmentID, item.SegmentID)

		// Read and verify record data from new segment
		newSegPath := getSegmentPath(tmpDir, 0, item.SegmentID)
		f, err := os.Open(newSegPath)
		require.NoError(t, err)
		readBuf := make([]byte, item.PhysicalLen)
		_, err = f.ReadAt(readBuf, int64(item.Offset))
		require.NoError(t, err)
		_ = f.Close()

		hdr, err := record.DecodeHeader(readBuf[:record.HeaderSize])
		require.NoError(t, err)
		valueStart := record.HeaderSize + int(hdr.KeyLen)
		require.Equal(t, records[i].value, readBuf[valueStart:valueStart+int(hdr.PhysicalSize)],
			"value mismatch at record %d", i)
	}
}

// --- Integration Tests ---

func TestCompaction_Integration_BasicCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWriteBufferSize(64*1024), // Small buffer to force multiple segments
		WithFlushConcurrency(1),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Write data to create multiple segments
	keys := make([][]byte, 20)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%03d", i))
		value := bytes.Repeat([]byte{byte(i)}, 4096)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Verify all keys are readable
	for i, key := range keys {
		data, found := cache.Get(key)
		require.True(t, found, "key %s should be found", key)
		expected := bytes.Repeat([]byte{byte(i)}, 4096)
		require.Equal(t, expected, data, "data mismatch for key %s", key)
	}
}

func TestCompaction_Integration_DeletedKeysNotResurrected(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWriteBufferSize(32*1024),
		WithFlushConcurrency(1),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Write some keys
	key1 := []byte("key-to-delete")
	key2 := []byte("key-to-keep")
	require.NoError(t, cache.Put(key1, []byte("value1")))
	require.NoError(t, cache.Put(key2, []byte("value2")))
	cache.Drain()

	// Delete key1
	require.NoError(t, cache.Delete(key1))

	// Verify key1 is deleted, key2 exists
	_, found := cache.Get(key1)
	require.False(t, found, "deleted key should not be found")

	data, found := cache.Get(key2)
	require.True(t, found, "kept key should be found")
	require.Equal(t, []byte("value2"), data)
}

func TestCompaction_Integration_OverwrittenKeysUseLatestValue(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWriteBufferSize(32*1024),
		WithFlushConcurrency(1),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("overwritten-key")

	// Write initial value
	require.NoError(t, cache.Put(key, []byte("value-v1")))
	cache.Drain()

	// Overwrite with new value
	require.NoError(t, cache.Put(key, []byte("value-v2")))
	cache.Drain()

	// Overwrite again
	require.NoError(t, cache.Put(key, []byte("value-v3-final")))
	cache.Drain()

	// Should always get the latest value
	data, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, []byte("value-v3-final"), data)
}

// --- Test Helpers ---

func setupTestDirs(t *testing.T, basePath string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(basePath, "segments", "0000"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(basePath, "db"), 0755))
}

func writeTestSegment(t *testing.T, path string, blobs ...[]byte) {
	t.Helper()

	// Calculate total size
	totalSize := int64(record.FileHeaderSize)
	for _, blob := range blobs {
		totalSize += int64(len(blob))
	}

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	// Write file header
	_, err = f.Write(record.FileHeaderBytes[:])
	require.NoError(t, err)

	// Write blobs
	for _, blob := range blobs {
		_, err = f.Write(blob)
		require.NoError(t, err)
	}
}

// writeTestMeta writes a .meta file (SegmentFooter format) for testing.
func writeTestMeta(t *testing.T, metaPath string, segID uint32, items []index.Item) {
	t.Helper()

	entries := make([]record.FooterEntry, len(items))
	for i, item := range items {
		// Convert Item to FooterEntry
		// PhysicalLen in Item = HeaderSize + KeyLen + PhysicalSize
		const keyLen = 16
		physicalSize := int64(item.PhysicalLen) - record.HeaderSize - keyLen
		if physicalSize < 0 {
			physicalSize = 0
		}
		entries[i] = record.FooterEntry{
			Key:          item.Key,
			Pos:          int64(item.Offset),
			LogicalSize:  int64(item.PhysicalLen),
			PhysicalSize: physicalSize,
			SeqID:        0,
			Flags:        0, // Flags are set separately below
			KeyLen:       keyLen,
		}
		// Note: index.Item and record.FooterEntry use different flag bit positions
		// for the deleted marker, so we must use the setter method.
		if item.IsDeleted() {
			entries[i].SetDeleted()
		}
		entries[i].SetCompression(item.Compression())
	}

	footer := record.SegmentFooter{
		SegmentID:   int64(segID),
		CTime:       0,
		MinSeqID:    0,
		MaxSeqID:    0,
		RecordCount: int64(len(entries)),
		Entries:     entries,
	}

	physicalSize := record.SegmentFooterAlignedSize(len(entries))
	buf := make([]byte, physicalSize)
	data := record.AppendFooterBlock(buf, footer)

	require.NoError(t, os.MkdirAll(filepath.Dir(metaPath), 0o755))
	require.NoError(t, os.WriteFile(metaPath, data, 0o644))
}

func makeTestBlob(t *testing.T, key index.Key, value []byte, seqID uint64) []byte {
	t.Helper()
	keyBytes := make([]byte, 16)
	// Encode key as little-endian uint64s
	for i := 0; i < 8; i++ {
		keyBytes[i] = byte(key.Lo >> (i * 8))
		keyBytes[i+8] = byte(key.Hi >> (i * 8))
	}

	rec := record.NewRecord(seqID, keyBytes, value, int64(len(value)))
	buf := make([]byte, rec.EncodedSize())
	rec.EncodeTo(buf)
	return buf
}
