package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

		err := p.writeBatch(segID, items)
		require.NoError(t, err)

		chunks := 0
		err = p.scanSegment(segID, func(m SegmentManifest) bool {
			chunks++
			require.Equal(t, segID, m.SegmentID)
			return true
		})

		require.NoError(t, err)
		require.Equal(t, 2, chunks, "Should have split into two chunks")
	})

	t.Run("PrefixIsolation", func(t *testing.T) {
		err := p.writeBatch(200, []Item{{Key: Key{Lo: 200}, SegmentID: 200}})
		require.NoError(t, err)
		err = p.writeBatch(300, []Item{{Key: Key{Lo: 300}, SegmentID: 300}})
		require.NoError(t, err)

		seen300 := false
		err = p.scanSegment(200, func(m SegmentManifest) bool {
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
		err := p.scanAll(func(m SegmentManifest) bool {
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
		err := p.scanSegment(200, func(m SegmentManifest) bool {
			keyToDelete = m.IndexKey
			return false
		})
		require.NoError(t, err)
		require.NotNil(t, keyToDelete)

		err = p.delete(keyToDelete)
		require.NoError(t, err)

		count := 0
		err = p.scanSegment(200, func(m SegmentManifest) bool {
			count++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, 0, count, "Segment 200 should be completely gone")
	})
}

func TestDeleteRecordsFromSegment_Collapse(t *testing.T) {
	path, err := os.MkdirTemp("", "blobcache-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	// Override global limit for this test: header(12) + 10 items * 32 bytes = 332 bytes
	defer testingSetMaxChunkSize(uint64(ManifestHeaderSize) + 10*ItemSize)()

	p, err := newPersistence(path)
	require.NoError(t, err)
	defer p.close()

	var segID uint32 = 777

	// 25 blobs + limit of 10 = 3 chunks ([10], [10], [5])
	totalBlobs := 25
	items := make([]Item, totalBlobs)
	for i := 0; i < totalBlobs; i++ {
		items[i] = Item{Key: Key{Lo: uint64(i + 1)}, SegmentID: segID}
	}

	err = p.writeBatch(segID, items)
	require.NoError(t, err)

	// Verify initial state
	count := 0
	p.scanSegment(segID, func(m SegmentManifest) bool {
		count++
		return true
	})
	assert.Equal(t, 3, count, "Initial write failed to create 3 chunks")

	// Delete 20 blobs, leaving 5
	toDelete := make(map[Key]struct{})
	for i := 1; i <= 20; i++ {
		toDelete[Key{Lo: uint64(i)}] = struct{}{}
	}

	err = p.DeleteRecordsFromSegment(segID, toDelete)
	require.NoError(t, err)

	// Verify collapse to 1 chunk
	finalCount := 0
	totalLive := 0
	err = p.scanSegment(segID, func(m SegmentManifest) bool {
		finalCount++
		totalLive += len(m.Items)
		return true
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, finalCount, "Should have collapsed from 3 chunks to 1")
	assert.Equal(t, 5, totalLive, "Should only have 5 items remaining in persistence")
}

func TestDurableIndex(t *testing.T) {
	tmp := t.TempDir()

	// Create and populate index
	idx, err := Open(tmp, 1000)
	require.NoError(t, err)

	var segID uint32 = 1
	items := []Item{
		{Key: Key{Lo: 100}, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: Key{Lo: 200}, SegmentID: segID, Offset: 100, PhysicalLen: 200},
		{Key: Key{Lo: 300}, SegmentID: segID, Offset: 300, PhysicalLen: 150},
	}

	err = idx.IngestBatch(segID, items)
	require.NoError(t, err)

	// Verify in-memory lookup
	item, ok := idx.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
	require.Equal(t, segID, item.SegmentID)

	require.Equal(t, 3, idx.Len())

	// Close and reopen
	require.NoError(t, idx.Close())

	idx2, err := Open(tmp, 1000)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify data survived
	require.Equal(t, 3, idx2.Len())
	item, ok = idx2.Get(Key{Lo: 200})
	require.True(t, ok)
	require.Equal(t, uint32(200), item.PhysicalLen)
}
