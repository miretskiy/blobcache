package index

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
)

// writeTestFooter writes a SegmentFooter to a .meta file for testing.
func writeTestFooter(
		t *testing.T, path string, segID uint32, entries []record.FooterEntry, maxSeqID uint64,
) {
	t.Helper()

	footer := record.SegmentFooter{
		SegmentID:   int64(segID),
		CTime:       time.Now().Unix(),
		MinSeqID:    0,
		MaxSeqID:    maxSeqID,
		RecordCount: int64(len(entries)),
		Entries:     entries,
	}

	physicalSize := record.SegmentFooterAlignedSize(len(entries))
	buf := make([]byte, physicalSize)
	data := record.AppendFooterBlock(buf, footer)

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

// footerEntryFromItem creates a FooterEntry from an Item for testing.
// PhysicalLen in Item = HeaderSize + KeyLen + PhysicalSize (the payload size in FooterEntry).
func footerEntryFromItem(item Item) record.FooterEntry {
	const keyLen = 16 // Assume 16-byte key for testing
	return record.FooterEntry{
		Key:          item.Key,
		Pos:          int64(item.Offset),
		LogicalSize:  int64(item.PhysicalLen),
		PhysicalSize: int64(item.PhysicalLen) - record.HeaderSize - keyLen,
		SeqID:        0,
		Flags:        uint64(item.Flags),
		KeyLen:       keyLen,
	}
}

func TestPersistence(t *testing.T) {
	tmp := t.TempDir()

	// Create segments directory structure
	segDir := filepath.Join(tmp, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmp, 1)
	require.NoError(t, err)
	defer p.close()

	t.Run("WriteAndRead", func(t *testing.T) {
		var segID uint32 = 100
		items := make([]Item, 10)
		entries := make([]record.FooterEntry, 10)
		for i := range items {
			items[i] = Item{Key: Key{Lo: uint64(i)}, SegmentID: segID, PhysicalLen: 100}
			entries[i] = footerEntryFromItem(items[i])
		}

		// Write footer to .meta file
		writeTestFooter(t, p.metaPath(segID), segID, entries, 12345)

		manifest, err := p.readMetaFile(segID)
		require.NoError(t, err)
		require.Equal(t, segID, manifest.SegmentID)
		require.Equal(t, uint64(12345), manifest.MaxSeqID)
		require.Len(t, manifest.Items, 10)
	})

	t.Run("PrefixIsolation", func(t *testing.T) {
		// Write two different segments
		writeTestFooter(t, p.metaPath(200), 200,
			[]record.FooterEntry{footerEntryFromItem(Item{Key: Key{Lo: 200}, SegmentID: 200})}, 0)
		writeTestFooter(t, p.metaPath(300), 300,
			[]record.FooterEntry{footerEntryFromItem(Item{Key: Key{Lo: 300}, SegmentID: 300})}, 0)

		manifest, err := p.readMetaFile(200)
		require.NoError(t, err)
		require.Equal(t, uint32(200), manifest.SegmentID)
		require.Len(t, manifest.Items, 1)
	})

	t.Run("Tombstones", func(t *testing.T) {
		// Write base manifest
		var segID uint32 = 400
		items := []Item{
			{Key: Key{Lo: 1}, SegmentID: segID, PhysicalLen: 100},
			{Key: Key{Lo: 2}, SegmentID: segID, PhysicalLen: 100},
			{Key: Key{Lo: 3}, SegmentID: segID, PhysicalLen: 100},
		}
		entries := make([]record.FooterEntry, len(items))
		for i, item := range items {
			entries[i] = footerEntryFromItem(item)
		}
		writeTestFooter(t, p.metaPath(segID), segID, entries, 0)

		// Write tombstone for key 2
		require.NoError(t, p.tombstone(segID, Key{Lo: 2}, nil))
		require.NoError(t, p.flushMetaFile(segID))

		// Read back and verify tombstone is applied
		manifest, err := p.readMetaFile(segID)
		require.NoError(t, err)
		require.Len(t, manifest.Items, 3)

		for _, item := range manifest.Items {
			if item.Key.Lo == 2 {
				require.True(t, item.IsDeleted(), "Key 2 should be marked deleted")
			} else {
				require.False(t, item.IsDeleted(), "Key %d should NOT be deleted", item.Key.Lo)
			}
		}
	})

	t.Run("TombstoneBatch", func(t *testing.T) {
		var segID uint32 = 500
		items := []Item{
			{Key: Key{Lo: 10}, SegmentID: segID, PhysicalLen: 100},
			{Key: Key{Lo: 20}, SegmentID: segID, PhysicalLen: 100},
			{Key: Key{Lo: 30}, SegmentID: segID, PhysicalLen: 100},
		}
		entries := make([]record.FooterEntry, len(items))
		for i, item := range items {
			entries[i] = footerEntryFromItem(item)
		}
		writeTestFooter(t, p.metaPath(segID), segID, entries, 0)

		// Batch tombstone keys 10 and 30
		require.NoError(t, p.tombstoneBatch([]Item{
			{Key: Key{Lo: 10}, SegmentID: segID},
			{Key: Key{Lo: 30}, SegmentID: segID},
		}))
		require.NoError(t, p.flushMetaFile(segID))

		manifest, err := p.readMetaFile(segID)
		require.NoError(t, err)

		deleted := 0
		for _, item := range manifest.Items {
			if item.IsDeleted() {
				deleted++
			}
		}
		require.Equal(t, 2, deleted, "Should have 2 deleted items")
	})

	t.Run("DropSegment", func(t *testing.T) {
		var segID uint32 = 600
		writeTestFooter(t, p.metaPath(segID), segID,
			[]record.FooterEntry{footerEntryFromItem(Item{Key: Key{Lo: 600}, SegmentID: segID})}, 0)

		// Verify it exists
		manifest, err := p.readMetaFile(segID)
		require.NoError(t, err)
		require.Len(t, manifest.Items, 1)

		// Drop it
		require.NoError(t, p.dropSegment(segID))

		// Verify it's gone
		manifest, err = p.readMetaFile(segID)
		require.NoError(t, err)
		require.Empty(t, manifest.Items)
	})
}

// TestHasOlderShadow tests the tombstone dissolution query logic.
// This is correctness-critical: false negatives could cause Leapfrog Hazard (data resurrection).
func TestHasOlderShadow(t *testing.T) {
	tmp := t.TempDir()

	p, err := newPersistence(tmp, 1)
	require.NoError(t, err)
	defer p.close()

	// Create test keys
	keyA := Key{Lo: 0x1111111111111111, Hi: 0xAAAAAAAAAAAAAAAA}
	keyB := Key{Lo: 0x2222222222222222, Hi: 0xBBBBBBBBBBBBBBBB}
	keyC := Key{Lo: 0x3333333333333333, Hi: 0xCCCCCCCCCCCCCCCC}

	// Helper to create and register a segment with specific keys
	registerSeg := func(segID uint32, keys ...Key) {
		filter := bloom.New(1000, 0.03)
		for _, k := range keys {
			filter.AddHash(k)
		}
		filter.Freeze()
		p.registerSegment(SegmentMetadata{
			ID:             segID,
			LiveItemCount:  int32(len(keys)),
			TombstoneCount: 0,
			LiveBytes:      int64(len(keys) * 1000),
			SegmentKeys:    filter,
		})
	}

	t.Run("NoSegments", func(t *testing.T) {
		// With no segments registered, hasOlderShadow should always return false
		require.False(t, p.hasOlderShadow(keyA, 5))
		require.False(t, p.hasOlderShadow(keyA, 0))
	})

	// Register segments: 3 has keyA, 5 has keyB, 7 has keyC
	registerSeg(3, keyA)
	registerSeg(5, keyB)
	registerSeg(7, keyC)

	t.Run("KeyOnlyInOlderSegment", func(t *testing.T) {
		// keyA is in segment 3
		// When floor=5, segment 3 is older (3 < 5), so should return true
		require.True(t, p.hasOlderShadow(keyA, 5))

		// When floor=3, no segment is older (no segment < 3), so should return false
		require.False(t, p.hasOlderShadow(keyA, 3))

		// When floor=10, segment 3 is older, so should return true
		require.True(t, p.hasOlderShadow(keyA, 10))
	})

	t.Run("KeyOnlyInNewerSegment", func(t *testing.T) {
		// keyC is in segment 7
		// When floor=5, segments 3 is older (3 < 5) but doesn't contain keyC
		require.False(t, p.hasOlderShadow(keyC, 5))

		// When floor=7, no segment < 7 contains keyC
		require.False(t, p.hasOlderShadow(keyC, 7))
	})

	t.Run("KeyNotInAnySegment", func(t *testing.T) {
		unknownKey := Key{Lo: 0xDEADBEEF, Hi: 0xCAFEBABE}
		// Unknown key should not match any segment
		require.False(t, p.hasOlderShadow(unknownKey, 10))
		require.False(t, p.hasOlderShadow(unknownKey, 3))
	})

	t.Run("KeyInMultipleSegments", func(t *testing.T) {
		// Add keyA to segment 5 as well (key exists in both 3 and 5)
		registerSeg(5, keyA, keyB) // Overwrite segment 5 filter

		// When floor=5, segment 3 has keyA (3 < 5), so should return true
		require.True(t, p.hasOlderShadow(keyA, 5))

		// When floor=3, no segment < 3, so should return false
		require.False(t, p.hasOlderShadow(keyA, 3))
	})

	t.Run("UnregisterSegment", func(t *testing.T) {
		// Unregister segment 3
		p.unregisterSegment(3)

		// Now keyA only exists in segment 5
		// When floor=5, no segment < 5 has keyA anymore
		require.False(t, p.hasOlderShadow(keyA, 5))

		// Re-register segment 3 for other tests
		registerSeg(3, keyA)
	})

	t.Run("UnregisterMultipleSegments", func(t *testing.T) {
		// Unregister segments 3 and 5
		p.unregisterSegments([]uint32{3, 5})

		// Now only segment 7 remains
		require.False(t, p.hasOlderShadow(keyA, 10))
		require.False(t, p.hasOlderShadow(keyB, 10))
		require.True(t, p.hasOlderShadow(keyC, 10)) // keyC in segment 7, 7 < 10

		// Re-register for cleanup
		registerSeg(3, keyA)
		registerSeg(5, keyB)
	})
}

// TestSegmentRegistry tests the segment registry lifecycle.
func TestSegmentRegistry(t *testing.T) {
	tmp := t.TempDir()

	p, err := newPersistence(tmp, 1)
	require.NoError(t, err)
	defer p.close()

	createMeta := func(segID uint32, keys ...Key) SegmentMetadata {
		filter := bloom.New(1000, 0.03)
		for _, k := range keys {
			filter.AddHash(k)
		}
		filter.Freeze()
		return SegmentMetadata{
			ID:             segID,
			LiveItemCount:  int32(len(keys)),
			TombstoneCount: 0,
			LiveBytes:      int64(len(keys) * 1000),
			SegmentKeys:    filter,
		}
	}

	t.Run("RegisterInOrder", func(t *testing.T) {
		p.registerSegment(createMeta(1, Key{Lo: 1}))
		p.registerSegment(createMeta(2, Key{Lo: 2}))
		p.registerSegment(createMeta(3, Key{Lo: 3}))

		p.segments.RLock()
		require.Len(t, p.segments.sorted, 3)
		require.Len(t, p.segments.byID, 3)
		require.Equal(t, uint32(1), p.segments.sorted[0].ID)
		require.Equal(t, uint32(2), p.segments.sorted[1].ID)
		require.Equal(t, uint32(3), p.segments.sorted[2].ID)
		p.segments.RUnlock()

		// Cleanup
		p.unregisterSegments([]uint32{1, 2, 3})
	})

	t.Run("RegisterOutOfOrder", func(t *testing.T) {
		// Register in reverse order
		p.registerSegment(createMeta(30, Key{Lo: 30}))
		p.registerSegment(createMeta(10, Key{Lo: 10}))
		p.registerSegment(createMeta(20, Key{Lo: 20}))

		p.segments.RLock()
		require.Len(t, p.segments.sorted, 3)
		require.Len(t, p.segments.byID, 3)
		// Should be sorted
		require.Equal(t, uint32(10), p.segments.sorted[0].ID)
		require.Equal(t, uint32(20), p.segments.sorted[1].ID)
		require.Equal(t, uint32(30), p.segments.sorted[2].ID)
		p.segments.RUnlock()

		// Cleanup
		p.unregisterSegments([]uint32{10, 20, 30})
	})

	t.Run("IdempotentRegistration", func(t *testing.T) {
		meta1 := createMeta(5, Key{Lo: 1})
		meta2 := createMeta(5, Key{Lo: 2})

		p.registerSegment(meta1)
		p.registerSegment(meta2) // Should update, not duplicate

		p.segments.RLock()
		require.Len(t, p.segments.sorted, 1)
		require.Len(t, p.segments.byID, 1)
		require.Equal(t, uint32(5), p.segments.sorted[0].ID)
		// Should be the second filter (updated)
		require.True(t, p.segments.sorted[0].SegmentKeys.Test(Key{Lo: 2}))
		p.segments.RUnlock()

		// Cleanup
		p.unregisterSegment(5)
	})

	t.Run("UnregisterNonexistent", func(t *testing.T) {
		// Should not panic when unregistering non-existent segment
		p.unregisterSegment(999)
		p.unregisterSegments([]uint32{998, 999})
	})
}

func TestDurableIndex(t *testing.T) {
	tmp := t.TempDir()

	// Create segments directory structure
	segDir := filepath.Join(tmp, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// Create index
	idx, err := OpenIndex(tmp, 1, 1000)
	require.NoError(t, err)

	var segID uint32 = 1
	items := []Item{
		{Key: Key{Lo: 100}, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: Key{Lo: 200}, SegmentID: segID, Offset: 100, PhysicalLen: 200},
		{Key: Key{Lo: 300}, SegmentID: segID, Offset: 300, PhysicalLen: 150},
	}

	// Write footer (simulating what WriteFooter does)
	entries := make([]record.FooterEntry, len(items))
	for i, item := range items {
		entries[i] = footerEntryFromItem(item)
	}
	writeTestFooter(t, idx.segments.metaPath(segID), segID, entries, 0)

	// Create a stub .seg file (required for .meta to not be treated as orphan)
	segPath := filepath.Join(segDir, fmt.Sprintf("%d.seg", segID))
	require.NoError(t, os.WriteFile(segPath, []byte{}, 0o644))

	// Ingest into RAM
	idx.AddSegment(0, items)

	// Verify in-memory data
	item, ok := idx.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
	require.Equal(t, segID, item.SegmentID)

	require.Equal(t, 3, idx.NumItems())

	// Close and reopen
	require.NoError(t, idx.Close())

	idx2, err := OpenIndex(tmp, 1, 1000)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify data survived (loaded from .meta file)
	require.Equal(t, 3, idx2.NumItems())
	item, ok = idx2.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
}
