package index

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
)

// TestSegmentMetadata_Alignment verifies SegmentMetadata is properly aligned for xmap.
func TestSegmentMetadata_Alignment(t *testing.T) {
	err := xmap.VerifyAlignment[uint32, SegmentMetadata]()
	require.NoError(t, err, "SegmentMetadata must be properly aligned for xmap usage")
}

// writeTestFooter writes a SegmentFooter to a .meta file for testing.
func writeTestFooter(t *testing.T, path string, segID uint32, entries []record.FooterEntry, maxSeqID uint64) {
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
	idx.IngestBatch(items)

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
