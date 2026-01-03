package index

import (
	"os"
	"testing"

	"github.com/miretskiy/blobcache/metadata"
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
		count := maxBlobsPerSegment + 5
		batch := make([]metadata.BlobRecord, count)
		for i := 0; i < count; i++ {
			batch[i] = metadata.BlobRecord{Hash: uint64(i), Size: 100}
		}

		err := p.writeBatch(segID, batch)
		require.NoError(t, err)

		chunks := 0
		err = p.scanSegment(segID, func(seg metadata.SegmentRecord) bool {
			chunks++
			require.Equal(t, segID, seg.SegmentID)
			return true
		})

		require.NoError(t, err)
		require.Equal(t, 2, chunks, "Should have split into two chunks")
	})

	t.Run("PrefixIsolation", func(t *testing.T) {
		err := p.writeBatch(200, []metadata.BlobRecord{{Hash: 200}})
		require.NoError(t, err)
		err = p.writeBatch(300, []metadata.BlobRecord{{Hash: 300}})
		require.NoError(t, err)

		seen300 := false
		err = p.scanSegment(200, func(seg metadata.SegmentRecord) bool {
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
		err := p.scanAll(func(seg metadata.SegmentRecord) bool {
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
		err := p.scanSegment(200, func(seg metadata.SegmentRecord) bool {
			keyToDelete = seg.IndexKey
			return false
		})
		require.NoError(t, err)
		require.NotNil(t, keyToDelete)

		err = p.delete(keyToDelete)
		require.NoError(t, err)

		count := 0
		err = p.scanSegment(200, func(seg metadata.SegmentRecord) bool {
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

	// Override global limit for this test
	defer testingSetMaxBlobsPerSegment(10)()

	p, err := newPersistence(path)
	require.NoError(t, err)
	defer p.close()

	segID := int64(777)

	// 25 blobs + limit of 10 = 3 chunks ([10], [10], [5])
	totalBlobs := 25
	batch := make([]metadata.BlobRecord, totalBlobs)
	for i := 0; i < totalBlobs; i++ {
		batch[i] = metadata.BlobRecord{Hash: uint64(i + 1)}
	}

	err = p.writeBatch(segID, batch)
	require.NoError(t, err)

	// Verify initial state
	count := 0
	p.scanSegment(segID, func(seg metadata.SegmentRecord) bool {
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
	err = p.scanSegment(segID, func(seg metadata.SegmentRecord) bool {
		finalCount++
		totalLive += len(seg.Records)
		return true
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, finalCount, "Should have collapsed from 3 chunks to 1")
	assert.Equal(t, 5, totalLive, "Should only have 5 records remaining in persistence")
}
