package index

import (
	"os"
	"testing"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistence(t *testing.T) {
	tmp := t.TempDir()
	p, err := newPersistence(tmp)
	require.NoError(t, err)
	defer p.close()

	t.Run("BatchSplitting", func(t *testing.T) {
		segID := int64(100)
		// Calculate how many entries fit in default chunk size
		entriesPerChunk := (maxChunkSize - record.SegmentFooterHeaderSize) / record.FooterEntrySize
		count := int(entriesPerChunk) + 5
		batch := make([]record.FooterEntry, count)
		for i := 0; i < count; i++ {
			batch[i] = record.FooterEntry{Hash: uint64(i), LogicalSize: 100}
		}

		err := p.writeBatch(segID, batch)
		require.NoError(t, err)

		chunks := 0
		err = p.scanSegment(segID, func(seg record.SegmentFooter) bool {
			chunks++
			require.Equal(t, segID, seg.SegmentID)
			return true
		})

		require.NoError(t, err)
		require.Equal(t, 2, chunks, "Should have split into two chunks")
	})

	t.Run("PrefixIsolation", func(t *testing.T) {
		err := p.writeBatch(200, []record.FooterEntry{{Hash: 200}})
		require.NoError(t, err)
		err = p.writeBatch(300, []record.FooterEntry{{Hash: 300}})
		require.NoError(t, err)

		seen300 := false
		err = p.scanSegment(200, func(seg record.SegmentFooter) bool {
			if seg.SegmentID == 300 {
				seen300 = true
			}
			return true
		})

		require.NoError(t, err)
		require.False(t, seen300, "Scan for segment 200 should not see segment 300")
	})

	t.Run("ScanAllOrdering", func(t *testing.T) {
		var ids []int64
		err := p.scanAll(func(seg record.SegmentFooter) bool {
			// Deduplicate split segments for order checking
			if len(ids) == 0 || ids[len(ids)-1] != seg.SegmentID {
				ids = append(ids, seg.SegmentID)
			}
			return true
		})

		require.NoError(t, err)
		require.Equal(t, []int64{100, 200, 300}, ids, "Global scan should be in SegmentID order")
	})

	t.Run("Delete", func(t *testing.T) {
		var keyToDelete []byte
		err := p.scanSegment(200, func(seg record.SegmentFooter) bool {
			keyToDelete = seg.IndexKey
			return false
		})
		require.NoError(t, err)
		require.NotNil(t, keyToDelete)

		err = p.delete(keyToDelete)
		require.NoError(t, err)

		count := 0
		err = p.scanSegment(200, func(seg record.SegmentFooter) bool {
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

	// Override global limit for this test: header(16) + 10 entries * 48 bytes = 496 bytes
	defer testingSetMaxChunkSize(16 + 10*record.FooterEntrySize)()

	p, err := newPersistence(path)
	require.NoError(t, err)
	defer p.close()

	segID := int64(777)

	// 25 blobs + limit of 10 = 3 chunks ([10], [10], [5])
	totalBlobs := 25
	batch := make([]record.FooterEntry, totalBlobs)
	for i := 0; i < totalBlobs; i++ {
		batch[i] = record.FooterEntry{Hash: uint64(i + 1)}
	}

	err = p.writeBatch(segID, batch)
	require.NoError(t, err)

	// Verify initial state
	count := 0
	p.scanSegment(segID, func(seg record.SegmentFooter) bool {
		count++
		return true
	})
	assert.Equal(t, 3, count, "Initial write failed to create 3 chunks")

	// Delete 20 blobs, leaving 5
	toDelete := make(map[uint64]struct{})
	for i := 1; i <= 20; i++ {
		toDelete[uint64(i)] = struct{}{}
	}

	err = p.DeleteRecordsFromSegment(segID, toDelete)
	require.NoError(t, err)

	// Verify collapse to 1 chunk
	finalCount := 0
	totalLive := 0
	err = p.scanSegment(segID, func(seg record.SegmentFooter) bool {
		finalCount++
		totalLive += len(seg.Entries)
		return true
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, finalCount, "Should have collapsed from 3 chunks to 1")
	assert.Equal(t, 5, totalLive, "Should only have 5 entries remaining in persistence")
}

func TestDurableIndex(t *testing.T) {
	tmp := t.TempDir()

	// Create and populate index
	idx, err := Open(tmp, 1000)
	require.NoError(t, err)

	segID := int64(1)
	batch := []record.FooterEntry{
		{Hash: 100, Pos: 0, PhysicalSize: 100},
		{Hash: 200, Pos: 100, PhysicalSize: 200},
		{Hash: 300, Pos: 300, PhysicalSize: 150},
	}

	err = idx.IngestBatch(segID, batch)
	require.NoError(t, err)

	// Verify in-memory lookup
	item, ok := idx.DeprecatedGetByHash(200)
	require.True(t, ok)
	require.Equal(t, int64(200), item.PhysicalSize)
	require.Equal(t, segID, item.SegmentID)

	require.Equal(t, 3, idx.Len())

	// Close and reopen
	require.NoError(t, idx.Close())

	idx2, err := Open(tmp, 1000)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify data survived
	require.Equal(t, 3, idx2.Len())
	item, ok = idx2.DeprecatedGetByHash(200)
	require.True(t, ok)
	require.Equal(t, int64(200), item.PhysicalSize)
}
