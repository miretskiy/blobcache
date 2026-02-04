package index

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
)

// writeTestFooterTS writes a SegmentFooter to a .meta file for tombstone testing.
func writeTestFooterTS(t *testing.T, path string, segID uint32, items []Item) {
	t.Helper()

	entries := make([]record.FooterEntry, len(items))
	for i, item := range items {
		entries[i] = record.FooterEntry{
			Key:          item.Key,
			Pos:          int64(item.Offset),
			LogicalSize:  int64(item.PhysicalLen),
			PhysicalSize: int64(item.PhysicalLen) - record.HeaderSize,
			SeqID:        0,
			Flags:        uint64(item.Flags),
			KeyLen:       16,
		}
	}

	footer := record.SegmentFooter{
		SegmentID:   int64(segID),
		CTime:       time.Now().Unix(),
		MinSeqID:    0,
		MaxSeqID:    0,
		RecordCount: int64(len(entries)),
		Entries:     entries,
	}

	physicalSize := record.SegmentFooterAlignedSize(len(entries))
	buf := make([]byte, physicalSize)
	data := record.AppendFooterBlock(buf, footer)

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

// TestTombstone_WriteAndRead validates tombstone persistence and loading.
func TestTombstone_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	segDir := filepath.Join(tmpDir, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmpDir, 1)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(10)
	key1 := Key{Lo: 1, Hi: 100}
	key2 := Key{Lo: 2, Hi: 200}
	key3 := Key{Lo: 3, Hi: 300}

	// Write the base manifest (footer)
	items := []Item{
		{Key: key1, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: key2, SegmentID: segID, Offset: 100, PhysicalLen: 200},
		{Key: key3, SegmentID: segID, Offset: 300, PhysicalLen: 300},
	}
	writeTestFooterTS(t, p.metaPath(segID), segID, items)

	// Write tombstones
	require.NoError(t, p.tombstone(segID, key1, []byte("user-key-1")))
	require.NoError(t, p.tombstone(segID, key2, nil)) // Eviction (no user key)
	require.NoError(t, p.flushMetaFile(segID))

	// Read back and verify tombstones were applied
	manifest, err := p.readMetaFile(segID)
	require.NoError(t, err)

	require.Len(t, manifest.Items, 3)
	require.True(t, manifest.Items[0].IsDeleted(), "key1 should be deleted")
	require.True(t, manifest.Items[1].IsDeleted(), "key2 should be deleted")
	require.False(t, manifest.Items[2].IsDeleted(), "key3 should be live")
}

// TestTombstone_ScanSegmentMerge validates that scanSegment merges tombstones.
func TestTombstone_ScanSegmentMerge(t *testing.T) {
	tmpDir := t.TempDir()
	segDir := filepath.Join(tmpDir, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmpDir, 1)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(5)

	// Write regular items
	items := []Item{
		{Key: Key{Lo: 1, Hi: 10}, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: Key{Lo: 2, Hi: 20}, SegmentID: segID, Offset: 100, PhysicalLen: 200},
		{Key: Key{Lo: 3, Hi: 30}, SegmentID: segID, Offset: 300, PhysicalLen: 300},
	}
	writeTestFooterTS(t, p.metaPath(segID), segID, items)

	// Write tombstones for keys 1 and 3
	require.NoError(t, p.tombstone(segID, items[0].Key, []byte("key-1")))
	require.NoError(t, p.tombstone(segID, items[2].Key, nil)) // Eviction style
	require.NoError(t, p.flushMetaFile(segID))

	// Scan segment - should merge tombstones
	var scannedItems []Item
	err = p.scanSegment(segID, func(m DurableBatch) bool {
		scannedItems = append(scannedItems, m.Items...)
		return true
	})
	require.NoError(t, err)

	// Verify all 3 items present
	require.Len(t, scannedItems, 3)

	// Verify items 0 and 2 are marked deleted (tombstones applied)
	require.True(t, scannedItems[0].IsDeleted(), "item 0 should be deleted (tombstone)")
	require.False(t, scannedItems[1].IsDeleted(), "item 1 should be live (no tombstone)")
	require.True(t, scannedItems[2].IsDeleted(), "item 2 should be deleted (tombstone)")

	// Verify keys match
	require.Equal(t, items[0].Key, scannedItems[0].Key)
	require.Equal(t, items[1].Key, scannedItems[1].Key)
	require.Equal(t, items[2].Key, scannedItems[2].Key)
}

// TestTombstone_NamespaceIsolation verifies regular and tombstone keys don't collide.
func TestTombstone_NamespaceIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	segDir := filepath.Join(tmpDir, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmpDir, 1)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(1)
	keyHash := Key{Lo: 999, Hi: 888}

	// Write regular item
	items := []Item{
		{Key: keyHash, SegmentID: segID, Offset: 0, PhysicalLen: 100},
	}
	writeTestFooterTS(t, p.metaPath(segID), segID, items)

	// Write tombstone with same hash
	require.NoError(t, p.tombstone(segID, keyHash, []byte("user-key")))
	require.NoError(t, p.flushMetaFile(segID))

	// Scan regular data - should see item marked as deleted
	var regularItems []Item
	err = p.scanSegment(segID, func(m DurableBatch) bool {
		regularItems = append(regularItems, m.Items...)
		return true
	})
	require.NoError(t, err)

	// Should see item marked as deleted (tombstone merged)
	require.Len(t, regularItems, 1)
	require.True(t, regularItems[0].IsDeleted(), "tombstone should have been merged")
}

// TestTombstone_MultipleSegments validates tombstone isolation across segments.
func TestTombstone_MultipleSegments(t *testing.T) {
	tmpDir := t.TempDir()
	segDir := filepath.Join(tmpDir, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmpDir, 1)
	require.NoError(t, err)
	defer p.close()

	// Write items to different segments
	seg10Items := []Item{
		{Key: Key{Lo: 1, Hi: 10}, SegmentID: 10, Offset: 0, PhysicalLen: 100},
	}
	seg20Items := []Item{
		{Key: Key{Lo: 2, Hi: 20}, SegmentID: 20, Offset: 0, PhysicalLen: 100},
	}
	writeTestFooterTS(t, p.metaPath(10), 10, seg10Items)
	writeTestFooterTS(t, p.metaPath(20), 20, seg20Items)

	// Write tombstones to both segments
	require.NoError(t, p.tombstone(10, seg10Items[0].Key, []byte("key-10")))
	require.NoError(t, p.tombstone(20, seg20Items[0].Key, []byte("key-20")))
	require.NoError(t, p.flushMetaFile(10))
	require.NoError(t, p.flushMetaFile(20))

	// Scan segment 10 - should only see segment 10 tombstone
	var items10 []Item
	err = p.scanSegment(10, func(m DurableBatch) bool {
		items10 = append(items10, m.Items...)
		return true
	})
	require.NoError(t, err)
	require.Len(t, items10, 1)
	require.True(t, items10[0].IsDeleted())

	// Scan segment 20 - should only see segment 20 tombstone
	var items20 []Item
	err = p.scanSegment(20, func(m DurableBatch) bool {
		items20 = append(items20, m.Items...)
		return true
	})
	require.NoError(t, err)
	require.Len(t, items20, 1)
	require.True(t, items20[0].IsDeleted())
}

// TestTombstone_Batch validates batched tombstone writes.
func TestTombstone_Batch(t *testing.T) {
	tmpDir := t.TempDir()
	segDir := filepath.Join(tmpDir, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmpDir, 1)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(1)
	numItems := 100

	// Write base items
	items := make([]Item, numItems)
	for i := range items {
		items[i] = Item{Key: Key{Lo: uint64(i)}, SegmentID: segID, Offset: uint32(i * 100), PhysicalLen: 100}
	}
	writeTestFooterTS(t, p.metaPath(segID), segID, items)

	// Batch tombstone half of them
	toDelete := make([]Item, numItems/2)
	for i := range toDelete {
		toDelete[i] = items[i*2] // Delete even indices
	}
	require.NoError(t, p.tombstoneBatch(toDelete))
	require.NoError(t, p.flushMetaFile(segID))

	// Read back and verify
	manifest, err := p.readMetaFile(segID)
	require.NoError(t, err)
	require.Len(t, manifest.Items, numItems)

	deleted := 0
	for _, item := range manifest.Items {
		if item.IsDeleted() {
			deleted++
		}
	}
	require.Equal(t, numItems/2, deleted, "Should have deleted half the items")
}

// TestTombstone_CompactTombstones validates tombstone compaction.
func TestTombstone_CompactTombstones(t *testing.T) {
	tmpDir := t.TempDir()
	segDir := filepath.Join(tmpDir, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	p, err := newPersistence(tmpDir, 1)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(1)

	// Write base items
	items := []Item{
		{Key: Key{Lo: 1}, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: Key{Lo: 2}, SegmentID: segID, Offset: 100, PhysicalLen: 100},
		{Key: Key{Lo: 3}, SegmentID: segID, Offset: 200, PhysicalLen: 100},
	}
	writeTestFooterTS(t, p.metaPath(segID), segID, items)

	// Write tombstones
	require.NoError(t, p.tombstone(segID, Key{Lo: 1}, nil))
	require.NoError(t, p.tombstone(segID, Key{Lo: 3}, nil))
	require.NoError(t, p.flushMetaFile(segID))

	// Compact tombstones
	callbackCount := 0
	err = p.compactTombstones(segID, func(tr TombstoneRecord) {
		callbackCount++
	})
	require.NoError(t, err)
	require.Equal(t, 2, callbackCount, "Should invoke callback for each tombstone")

	// Read back - tombstones should be baked into items, no more tombstone batches
	manifest, err := p.readMetaFile(segID)
	require.NoError(t, err)
	require.Len(t, manifest.Items, 3)

	// Items 1 and 3 should be marked deleted
	require.True(t, manifest.Items[0].IsDeleted())
	require.False(t, manifest.Items[1].IsDeleted())
	require.True(t, manifest.Items[2].IsDeleted())
}
