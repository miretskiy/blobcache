package blobcache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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

	idx, err := index.OpenIndex(tmpDir, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	segIDs.counter.Store(100)

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, nil)

	// Case 1: Gap with segment present - should fail
	// Create segment 2 (exists in gap between 1 and 3)
	require.NoError(t, idx.IngestBatch(2, []index.Item{
		{Key: index.Key{Lo: 200, Hi: 0}, SegmentID: 2, Offset: 0, PhysicalLen: 100},
	}, 0))

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

	idx, err := index.OpenIndex(tmpDir, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	segIDs.counter.Store(100)

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, nil)

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

	idx, err := index.OpenIndex(tmpDir, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, nil)

	// Ingest empty batches to create segment manifests with no items
	require.NoError(t, idx.IngestBatch(1, nil, 0))
	require.NoError(t, idx.IngestBatch(2, nil, 0))

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
	pool := NewMmapPool("test-footer", 256<<10, 2)
	defer pool.Close()

	idx, err := index.OpenIndex(tmpDir, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID() // Allocate source segment ID

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, pool)

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
	require.NoError(t, idx.IngestBatch(sourceSegID, items, 10))

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
	// Tombstones (deleted items) must be preserved during compaction for crash safety.
	// They prevent resurrection of keys that were deleted.

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)
	pool := NewMmapPool("test-footer", 256<<10, 2)
	defer pool.Close()

	idx, err := index.OpenIndex(tmpDir, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID() // Allocate source segment ID

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, pool)

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
	require.NoError(t, idx.IngestBatch(sourceSegID, items, 10))

	// Remove the deleted key from RAM (tombstones don't stay in RAM index for lookups)
	idx.Delete(keyDeleted)

	result, err := c.Compact([]uint32{sourceSegID}, false)
	require.NoError(t, err)

	require.Equal(t, 1, result.ItemsCompacted)
	require.Equal(t, 1, result.TombstonesKept)

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

func TestCompactor_TombstoneDropping(t *testing.T) {
	// When dropTombstones=true (tail segment), tombstones should be garbage collected
	// from BOTH disk (manifest) AND RAM (BlobIndex).
	//
	// This test verifies the fix for the memory leak where tombstones were removed
	// from the manifest but left as zombie entries in the RAM index.

	tmpDir := t.TempDir()
	setupTestDirs(t, tmpDir)
	pool := NewMmapPool("test-footer", 256<<10, 2)
	defer pool.Close()

	idx, err := index.OpenIndex(tmpDir, 100)
	require.NoError(t, err)
	defer idx.Close()

	archivist := NewArchivist(config{Path: tmpDir}, idx)
	defer archivist.Close()

	segIDs := &segmentIDProvider{}
	sourceSegID := segIDs.NextSegmentID()

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, pool)

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
	require.NoError(t, idx.IngestBatch(sourceSegID, items, 10))

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

	require.Equal(t, 1, result.ItemsCompacted)
	require.Equal(t, 0, result.TombstonesKept, "tombstones should not be kept")
	require.Equal(t, 1, result.TombstonesDropped, "tombstone should be dropped")

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
	pool := NewMmapPool("test-footer", 256<<10, 2)
	defer pool.Close()

	idx, err := index.OpenIndex(tmpDir, 100)
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
	require.NoError(t, idx.IngestBatch(sourceSegID, items, 10))

	c := NewCompactor(idx, archivist, segIDs, tmpDir, 0, sys.SyncNone, pool)

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
