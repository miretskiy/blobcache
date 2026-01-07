package index

import (
	"os"
	"sync"
	"testing"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a new index with automatic cleanup
func setupIndex(t *testing.T) (*Index, func()) {
	tmpDir, err := os.MkdirTemp("", "blobcache-index-test-*")
	require.NoError(t, err)

	idx, err := NewIndex(tmpDir)
	require.NoError(t, err)

	return idx, func() {
		idx.Close()
		_ = os.RemoveAll(tmpDir)
	}
}

func TestIndex_BasicIO(t *testing.T) {
	idx, cleanup := setupIndex(t)
	defer cleanup()

	hash := uint64(12345)
	batch := []metadata.BlobRecord{
		{Hash: hash, LogicalSize: 1024, Pos: 5000},
	}

	// Test Ingest
	err := idx.IngestBatch(1, batch)
	require.NoError(t, err)

	// Test Get (should mark as visited internally)
	entry, ok := idx.Get(hash)
	require.True(t, ok)
	require.Equal(t, int64(1024), entry.LogicalSize)
	require.Equal(t, int64(1), entry.SegmentID)

	// Test DeleteBlob
	require.NoError(t, idx.DeleteBlobs(entry))
	_, ok = idx.Get(hash)
	require.False(t, ok, "Blob should be gone from RAM")
}

func TestIndex_PersistenceRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "recovery-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// 1. Write data and close
	idx, err := NewIndex(tmpDir)
	require.NoError(t, err)

	hash := uint64(888)
	err = idx.IngestBatch(42, []metadata.BlobRecord{{Hash: hash, LogicalSize: 99}})
	require.NoError(t, err)
	idx.Close()

	// 2. Re-open and verify recovery
	idx2, err := NewIndex(tmpDir)
	require.NoError(t, err)
	defer idx2.Close()

	entry, ok := idx2.Get(hash)
	require.True(t, ok, "Data should be recovered from Bitcask")
	require.Equal(t, int64(42), entry.SegmentID)
}

func TestIndex_SegmentDeletion(t *testing.T) {
	idx, cleanup := setupIndex(t)
	defer cleanup()

	// Fill two segments
	require.NoError(t, idx.IngestBatch(1, []metadata.BlobRecord{{Hash: 10, LogicalSize: 100}}))
	require.NoError(t, idx.IngestBatch(2, []metadata.BlobRecord{{Hash: 20, LogicalSize: 100}}))

	// Delete segment 1
	err := idx.DeleteSegment(1)
	require.NoError(t, err)

	// Verify isolation
	_, ok := idx.Get(10)
	require.False(t, ok, "Segment 1 records should be deleted")

	_, ok = idx.Get(20)
	require.True(t, ok, "Segment 2 records should remain")
}

func TestIndex_EvictionOrchestration(t *testing.T) {
	idx, cleanup := setupIndex(t)
	defer cleanup()

	// Insert two blobs
	idx.IngestBatch(1, []metadata.BlobRecord{
		{Hash: 1, LogicalSize: 10},
		{Hash: 2, LogicalSize: 10},
	})

	// Mark 1 as visited (Hot)
	_, _ = idx.Get(1)

	// First eviction should pick 2 (the cold one)
	entry, err := idx.Evict()
	require.NoError(t, err)
	require.Equal(t, uint64(2), entry.Hash)

	// Verify 2 is removed from index
	_, ok := idx.Get(2)
	require.False(t, ok)
}

func TestIndex_Concurrency(t *testing.T) {
	idx, cleanup := setupIndex(t)
	defer cleanup()

	const workers = 8
	const itemsPerWorker = 500
	var wg sync.WaitGroup

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			batch := make([]metadata.BlobRecord, itemsPerWorker)
			for i := 0; i < itemsPerWorker; i++ {
				batch[i] = metadata.BlobRecord{
					Hash:        uint64(id*itemsPerWorker + i),
					LogicalSize: 100,
				}
			}
			idx.IngestBatch(int64(id), batch)
		}(w)
	}
	wg.Wait()

	count := 0
	idx.ForEachBlob(func(e Entry) bool {
		count++
		return true
	})
	require.Equal(t, workers*itemsPerWorker, count)
}

func TestIndex_Stats(t *testing.T) {
	idx, cleanup := setupIndex(t)
	defer cleanup()

	// 1. Initial State
	stats := idx.Stats()
	require.Equal(t, 0, stats.BlobCount)
	require.Equal(t, 0, stats.SegmentCount)

	// 2. After Ingestion
	// Segment 1: 5 blobs
	// Segment 2: 5 blobs
	for s := int64(1); s <= 2; s++ {
		batch := make([]metadata.BlobRecord, 5)
		for i := 0; i < 5; i++ {
			batch[i] = metadata.BlobRecord{Hash: uint64(s*10 + int64(i))}
		}
		require.NoError(t, idx.IngestBatch(s, batch))
	}

	stats = idx.Stats()
	require.Equal(t, 10, stats.BlobCount)
	require.Equal(t, 2, stats.SegmentCount)

	// 3. After Manual Deletion
	require.NoError(t, idx.DeleteBlobs(makeEntry(0, 10, 0, 0))) // removeNode one from segment 1
	stats = idx.Stats()
	require.Equal(t, 9, stats.BlobCount)
	require.Equal(t, 2, stats.SegmentCount, "Segment count shouldn't change on single blob delete")

	// 4. After Eviction
	_, err := idx.Evict()
	require.NoError(t, err)
	stats = idx.Stats()
	require.Equal(t, 8, stats.BlobCount)

	// 5. After Full Segment Deletion
	// Note: We already deleted blob 10 from segment 1,
	// but the segment record still exists on disk until DeleteSegment is called.
	err = idx.DeleteSegment(2)
	require.NoError(t, err)

	stats = idx.Stats()
	require.Equal(t, 3, stats.BlobCount, "Should only have the 4 remaining items from segment 1 (minus the 1 we evicted)")
	require.Equal(t, 1, stats.SegmentCount, "Segment 2 should be gone from disk")
}

func TestIndex_Stats_EmptyBatch(t *testing.T) {
	idx, cleanup := setupIndex(t)
	defer cleanup()

	// Attempting to ingest an empty batch
	err := idx.IngestBatch(1, []metadata.BlobRecord{})
	require.NoError(t, err)

	stats := idx.Stats()
	require.Equal(t, 0, stats.BlobCount, "Empty batch should not increment counts")
}

func makeEntry(segID int64, hash uint64, pos, size int64) Entry {
	return Entry{
		BlobRecord: metadata.BlobRecord{
			Hash:        hash,
			Pos:         pos,
			LogicalSize: size,
		},
		SegmentID: segID,
	}
}

func TestIndex_DeleteBlobs_Batch(t *testing.T) {
	// 1. Setup
	path, err := os.MkdirTemp("", "blobcache-idx-batch-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	// Override global limit for this test
	defer testingSetMaxBlobsPerSegment(10)()

	idx, err := NewIndex(path)
	require.NoError(t, err)
	defer idx.Close()

	seg1, seg2 := int64(101), int64(202)

	// 2. Prepare Bitcask state
	// Simulate existing segments on disk
	blobs1 := []metadata.BlobRecord{{Hash: 1}, {Hash: 2}, {Hash: 3}}
	blobs2 := []metadata.BlobRecord{{Hash: 10}, {Hash: 11}}

	require.NoError(t, idx.segments.writeBatch(seg1, blobs1))
	require.NoError(t, idx.segments.writeBatch(seg2, blobs2))

	// 3. Populate RAM Index
	idx.blobs.Store(1, &node{entry: makeEntry(seg1, 1, 0, 1024)})
	idx.blobs.Store(2, &node{entry: makeEntry(seg1, 2, 1024, 1024)})
	idx.blobs.Store(3, &node{entry: makeEntry(seg1, 3, 2048, 1024)})
	idx.blobs.Store(10, &node{entry: makeEntry(seg2, 10, 0, 512)})
	idx.blobs.Store(11, &node{entry: makeEntry(seg2, 11, 512, 512)})

	// 4. Perform Batch Deletion
	// We delete 1 and 11. This should happen synchronously.
	victims := []Entry{
		makeEntry(seg1, 1, 0, 1024),
		makeEntry(seg2, 11, 512, 512),
	}

	err = idx.DeleteBlobs(victims...)
	require.NoError(t, err)

	// 5. Verify RAM Path (Fast Path)
	_, ok1 := idx.blobs.Load(1)
	assert.False(t, ok1, "Blob 1 should be gone from RAM immediately")
	_, ok11 := idx.blobs.Load(11)
	assert.False(t, ok11, "Blob 11 should be gone from RAM immediately")

	_, ok2 := idx.blobs.Load(2)
	assert.True(t, ok2, "Blob 2 should still be in RAM")

	// 6. Verify Persistence Path (Durable Path)
	// Check Seg 1
	foundHashes1 := make(map[uint64]bool)
	err = idx.segments.scanSegment(seg1, func(seg metadata.SegmentRecord) bool {
		for _, rec := range seg.Records {
			foundHashes1[rec.Hash] = true
		}
		return true
	})
	assert.NoError(t, err)
	assert.NotContains(t, foundHashes1, uint64(1), "Hash 1 should be purged from Bitcask")
	assert.Equal(t, 2, len(foundHashes1), "Seg 1 should have 2 records left")

	// Check Seg 2
	foundHashes2 := make(map[uint64]bool)
	err = idx.segments.scanSegment(seg2, func(seg metadata.SegmentRecord) bool {
		for _, rec := range seg.Records {
			foundHashes2[rec.Hash] = true
		}
		return true
	})
	assert.NoError(t, err)
	assert.NotContains(t, foundHashes2, uint64(11), "Hash 11 should be purged from Bitcask")
	assert.Contains(t, foundHashes2, uint64(10), "Hash 10 should remain")
}
