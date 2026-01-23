package index

import (
	"testing"

	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
)

// TestSegmentMetadata_Alignment verifies SegmentMetadata is properly aligned for xmap.
func TestSegmentMetadata_Alignment(t *testing.T) {
	err := xmap.VerifyAlignment[uint32, SegmentMetadata]()
	require.NoError(t, err, "SegmentMetadata must be properly aligned for xmap usage")
}

func TestPersistence(t *testing.T) {
	tmp := t.TempDir()
	p, err := newPersistence(tmp)
	require.NoError(t, err)
	defer p.close()

	t.Run("BatchSplitting", func(t *testing.T) {
		var segID uint32 = 100
		// Calculate how many items fit in default chunk size
		itemsPerChunk := (maxChunkSize - uint64(ManifestHeaderSize)) / ItemSize
		count := int(itemsPerChunk) + 5
		items := make([]Item, count)
		for i := 0; i < count; i++ {
			items[i] = Item{Key: Key{Lo: uint64(i)}, SegmentID: segID, PhysicalLen: 100}
		}

		err := p.writeBatch(segID, items, 0)
		require.NoError(t, err)

		chunks := 0
		err = p.scanSegment(segID, func(m DurableBatch) bool {
			chunks++
			require.Equal(t, segID, m.SegmentID)
			return true
		})

		require.NoError(t, err)
		require.Equal(t, 2, chunks, "Should have split into two chunks")
	})

	t.Run("PrefixIsolation", func(t *testing.T) {
		err := p.writeBatch(200, []Item{{Key: Key{Lo: 200}, SegmentID: 200}}, 0)
		require.NoError(t, err)
		err = p.writeBatch(300, []Item{{Key: Key{Lo: 300}, SegmentID: 300}}, 0)
		require.NoError(t, err)

		seen300 := false
		err = p.scanSegment(200, func(m DurableBatch) bool {
			if m.SegmentID == 300 {
				seen300 = true
			}
			return true
		})

		require.NoError(t, err)
		require.False(t, seen300, "Scan for segment 200 should not see segment 300")
	})

	t.Run("ScanAllOrdering", func(t *testing.T) {
		var ids []uint32
		err := p.scanAll(func(m DurableBatch) bool {
			// Deduplicate split segments for order checking
			if len(ids) == 0 || ids[len(ids)-1] != m.SegmentID {
				ids = append(ids, m.SegmentID)
			}
			return true
		})

		require.NoError(t, err)
		require.Equal(t, []uint32{100, 200, 300}, ids, "Global scan should be in SegmentID order")
	})

	t.Run("Delete", func(t *testing.T) {
		var keyToDelete []byte
		err := p.scanSegment(200, func(m DurableBatch) bool {
			keyToDelete = m.IndexKey
			return false
		})
		require.NoError(t, err)
		require.NotNil(t, keyToDelete)

		// Delete key directly via transaction
		txn := p.db.Transaction()
		defer txn.Discard()
		err = txn.Delete(keyToDelete)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		count := 0
		err = p.scanSegment(200, func(m DurableBatch) bool {
			count++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, 0, count, "Segment 200 should be completely gone")
	})
}

func TestDurableIndex(t *testing.T) {
	tmp := t.TempDir()

	// Create and populate index
	idx, err := OpenIndex(tmp, 1000)
	require.NoError(t, err)

	var segID uint32 = 1
	items := []Item{
		{Key: Key{Lo: 100}, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: Key{Lo: 200}, SegmentID: segID, Offset: 100, PhysicalLen: 200},
		{Key: Key{Lo: 300}, SegmentID: segID, Offset: 300, PhysicalLen: 150},
	}

	err = idx.IngestBatch(segID, items, 0)
	require.NoError(t, err)

	// Verify in-memory data
	item, ok := idx.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
	require.Equal(t, segID, item.SegmentID)

	require.Equal(t, 3, idx.NumItems())

	// Close and reopen
	require.NoError(t, idx.Close())

	idx2, err := OpenIndex(tmp, 1000)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify data survived
	require.Equal(t, 3, idx2.NumItems())
	item, ok = idx2.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
}
