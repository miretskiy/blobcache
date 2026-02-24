package index

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// testReadSSTViaMeta creates a ReadSSTFunc that reads the .meta footer block
// (without tombstones) and returns it as if it were an .sst file. This allows
// tests that create .meta files to work with the new .sst + .del flow.
func testReadSSTViaMeta() ReadSSTFunc {
	return func(sstPath string, segmentID uint32) (DurableBatch, error) {
		// Convert .sst path to .meta path.
		metaPath := strings.TrimSuffix(sstPath, ".sst") + ".meta"

		f, err := os.Open(metaPath)
		if err != nil {
			return DurableBatch{}, err
		}
		defer f.Close()

		stat, err := f.Stat()
		if err != nil {
			return DurableBatch{}, err
		}
		if stat.Size() < record.TailSize {
			return DurableBatch{}, fmt.Errorf("meta file too small: %d", stat.Size())
		}

		footerBlockSize, err := findFooterBlockSize(metaPath)
		if err != nil {
			return DurableBatch{}, err
		}

		footer, _, err := record.ReadFooterBlock(f, footerBlockSize, int64(segmentID))
		if err != nil {
			return DurableBatch{}, err
		}

		items := make([]Item, len(footer.Entries))
		for i := range footer.Entries {
			items[i] = footerEntryToItem(uint32(footer.SegmentID), &footer.Entries[i])
		}

		return DurableBatch{
			SegmentID: uint32(footer.SegmentID),
			CTime:     footer.CTime,
			MaxSeqID:  footer.MaxSeqID,
			Items:     items,
			Entries:   footer.Entries,
		}, nil
	}
}

func TestPersistence(t *testing.T) {
	tmp := t.TempDir()

	// Create segments directory structure
	segDir := filepath.Join(tmp, "segments", "0000")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	readSST := testReadSSTViaMeta()
	p, err := newPersistence(tmp, 1, readSST)
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

		// Read via readMetaFile (legacy path)
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
		// Write base manifest (acts as .sst via mock ReadSSTFunc)
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

		// Write tombstone for key 2 (goes to .del file)
		require.NoError(t, p.tombstone(segID, Key{Lo: 2}, nil))
		require.NoError(t, p.flushTombstoneFile(segID))

		// Read back via readSegmentIndex (.sst mock + .del merge)
		manifest, err := p.readSegmentIndex(segID)
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
		require.NoError(t, p.flushTombstoneFile(segID))

		// Read back via readSegmentIndex (.sst mock + .del merge)
		manifest, err := p.readSegmentIndex(segID)
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

		// Write a tombstone to create a .del file
		require.NoError(t, p.tombstone(segID, Key{Lo: 600}, nil))
		require.NoError(t, p.flushTombstoneFile(segID))

		// Verify .del file exists
		_, err := os.Stat(p.delPath(segID))
		require.NoError(t, err)

		// Drop it (closes and deletes .del file)
		require.NoError(t, p.dropSegment(segID))

		// Verify .del file is gone
		_, err = os.Stat(p.delPath(segID))
		require.True(t, os.IsNotExist(err), ".del file should be deleted after drop")
	})
}

// TestHasOlderShadow tests the tombstone dissolution query logic.
// HasOlderShadow uses direct RAM index lookup (not Bloom filters) to determine
// if a key has a live version in any segment older than floorID.
//
// This is correctness-critical: false negatives could cause data resurrection
// after a crash (benign in cache mode, impossible in CAS mode).
func TestHasOlderShadow(t *testing.T) {
	tmp := t.TempDir()

	idx, err := OpenIndex(tmp, 0, 1000, nil)
	require.NoError(t, err)
	defer idx.Close()

	keyA := Key{Lo: 0x1111111111111111, Hi: 0xAAAAAAAAAAAAAAAA}
	keyB := Key{Lo: 0x2222222222222222, Hi: 0xBBBBBBBBBBBBBBBB}
	keyC := Key{Lo: 0x3333333333333333, Hi: 0xCCCCCCCCCCCCCCCC}
	unknownKey := Key{Lo: 0xDEADBEEF, Hi: 0xCAFEBABE}

	t.Run("EmptyIndex", func(t *testing.T) {
		// No keys in RAM → always false
		require.False(t, idx.HasOlderShadow(keyA, 5))
		require.False(t, idx.HasOlderShadow(keyA, 0))
	})

	// Put keys into RAM index at specific segment locations
	idx.Put(Item{Key: keyA, SegmentID: 3, Offset: 0, PhysicalLen: 100})
	idx.Put(Item{Key: keyB, SegmentID: 5, Offset: 0, PhysicalLen: 100})
	idx.Put(Item{Key: keyC, SegmentID: 7, Offset: 0, PhysicalLen: 100})

	t.Run("KeyInOlderSegment", func(t *testing.T) {
		// keyA is in segment 3
		// floor=5: segment 3 < 5 → true
		require.True(t, idx.HasOlderShadow(keyA, 5))
		// floor=10: segment 3 < 10 → true
		require.True(t, idx.HasOlderShadow(keyA, 10))
	})

	t.Run("KeyInSameOrNewerSegment", func(t *testing.T) {
		// keyA is in segment 3
		// floor=3: segment 3 is NOT < 3 → false (key is in compaction range)
		require.False(t, idx.HasOlderShadow(keyA, 3))
		// floor=2: segment 3 is NOT < 2 → false
		require.False(t, idx.HasOlderShadow(keyA, 2))
	})

	t.Run("KeyNotInRAM", func(t *testing.T) {
		// Unknown key not in RAM → false (no known version)
		require.False(t, idx.HasOlderShadow(unknownKey, 10))
		require.False(t, idx.HasOlderShadow(unknownKey, 0))
	})

	t.Run("KeyEvictedFromRAM", func(t *testing.T) {
		// Simulate eviction: remove keyA from RAM
		idx.Delete(keyA)

		// Key not in RAM → false (dissolve tombstone)
		require.False(t, idx.HasOlderShadow(keyA, 10))
		require.False(t, idx.HasOlderShadow(keyA, 5))

		// Restore for other tests
		idx.Put(Item{Key: keyA, SegmentID: 3, Offset: 0, PhysicalLen: 100})
	})

	t.Run("KeyOverwrittenToNewerSegment", func(t *testing.T) {
		// keyA overwritten from segment 3 to segment 8
		idx.Put(Item{Key: keyA, SegmentID: 8, Offset: 0, PhysicalLen: 100})

		// floor=5: segment 8 is NOT < 5 → false (newer write supersedes)
		require.False(t, idx.HasOlderShadow(keyA, 5))
		// floor=10: segment 8 < 10 → true
		require.True(t, idx.HasOlderShadow(keyA, 10))
	})

	t.Run("DeletedKeyStillInRAM", func(t *testing.T) {
		// Mark keyB as deleted (still in RAM with deleted flag)
		idx.MarkDeleted(keyB)

		// keyB still in RAM at segment 5
		// floor=10: segment 5 < 10 → true (must preserve tombstone)
		require.True(t, idx.HasOlderShadow(keyB, 10))
		// floor=5: segment 5 is NOT < 5 → false
		require.False(t, idx.HasOlderShadow(keyB, 5))
	})
}

// TestSegmentRegistry tests the segment registry lifecycle.
func TestSegmentRegistry(t *testing.T) {
	tmp := t.TempDir()

	p, err := newPersistence(tmp, 1, nil)
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

	readSST := testReadSSTViaMeta()

	// Create index
	idx, err := OpenIndex(tmp, 1, 1000, readSST)
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

	idx2, err := OpenIndex(tmp, 1, 1000, readSST)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify data survived (loaded from .meta file via migration)
	require.Equal(t, 3, idx2.NumItems())
	item, ok = idx2.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
}
