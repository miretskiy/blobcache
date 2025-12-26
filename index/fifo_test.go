package index

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFIFOQueue_InsertionOrder tests that queue maintains insertion order
func TestFIFOQueue_InsertionOrder(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Insert values in order
	keys := []Key{10, 20, 30, 40, 50}
	for _, key := range keys {
		val := &Value{Size: int64(key) * 100, SegmentID: int64(key)}
		require.NoError(t, idx.PutBlob(key, val))
	}

	// Verify oldest is key 10
	oldest := idx.OldestBlob()
	require.NotNil(t, oldest)
	require.Equal(t, int64(1000), oldest.Size) // key 10 * 100

	// Verify order: oldest→newest
	var collected []int64
	idx.ForEachBlobSorted(SortByInsertionOrder, nil, func(kv KeyValue) bool {
		collected = append(collected, kv.Val.Size)
		return true
	})

	expected := []int64{1000, 2000, 3000, 4000, 5000}
	require.Equal(t, expected, collected)
}

// TestFIFOQueue_ReverseOrder tests iteration in reverse (newest→oldest)
func TestFIFOQueue_ReverseOrder(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Insert values
	for i := 1; i <= 5; i++ {
		val := &Value{Size: int64(i * 100)}
		require.NoError(t, idx.PutBlob(Key(i), val))
	}

	// Iterate newest→oldest
	var collected []int64
	idx.ForEachBlobSorted(SortByInsertionOrderDesc, nil, func(kv KeyValue) bool {
		collected = append(collected, kv.Val.Size)
		return true
	})

	expected := []int64{500, 400, 300, 200, 100}
	require.Equal(t, expected, collected)
}

// TestFIFOQueue_Delete tests that deletion maintains queue integrity
func TestFIFOQueue_Delete(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Insert 5 values
	for i := 1; i <= 5; i++ {
		val := &Value{Size: int64(i * 100)}
		require.NoError(t, idx.PutBlob(Key(i), val))
	}

	// Delete middle entry (key 3)
	deleted := idx.DeleteBlob(Key(3))
	require.NotNil(t, deleted)

	// Verify queue order (should skip 3)
	var collected []int64
	idx.ForEachBlobSorted(SortByInsertionOrder, nil, func(kv KeyValue) bool {
		collected = append(collected, kv.Val.Size)
		return true
	})

	expected := []int64{100, 200, 400, 500}
	require.Equal(t, expected, collected)
}

// TestFIFOQueue_DeleteOldest tests deleting tail
func TestFIFOQueue_DeleteOldest(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Insert values
	for i := 1; i <= 3; i++ {
		val := &Value{Size: int64(i * 100)}
		require.NoError(t, idx.PutBlob(Key(i), val))
	}

	// Delete oldest (key 1)
	deleted := idx.DeleteBlob(Key(1))
	require.NotNil(t, deleted)

	// Verify new oldest is key 2
	oldest := idx.OldestBlob()
	require.NotNil(t, oldest)
	require.Equal(t, int64(200), oldest.Size)
}

// TestFIFOQueue_DeleteNewest tests deleting head
func TestFIFOQueue_DeleteNewest(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Insert values
	for i := 1; i <= 3; i++ {
		val := &Value{Size: int64(i * 100)}
		require.NoError(t, idx.PutBlob(Key(i), val))
	}

	// Delete newest (key 3)
	deleted := idx.DeleteBlob(Key(3))
	require.NotNil(t, deleted)

	// Verify queue still has 1,2 in order
	var collected []int64
	idx.ForEachBlobSorted(SortByInsertionOrder, nil, func(kv KeyValue) bool {
		collected = append(collected, kv.Val.Size)
		return true
	})

	expected := []int64{100, 200}
	require.Equal(t, expected, collected)
}

// TestFIFOQueue_Empty tests empty queue behavior
func TestFIFOQueue_Empty(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Empty queue
	oldest := idx.OldestBlob()
	require.Nil(t, oldest)

	// ForEachBlobSorted should not call fn
	called := false
	idx.ForEachBlobSorted(SortByInsertionOrder, nil, func(kv KeyValue) bool {
		called = true
		return true
	})
	require.False(t, called)
}

// TestFIFOQueue_SingleEntry tests queue with one entry
func TestFIFOQueue_SingleEntry(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	val := &Value{Size: 100}
	require.NoError(t, idx.PutBlob(Key(1), val))

	// Oldest should be the only entry
	oldest := idx.OldestBlob()
	require.NotNil(t, oldest)
	require.Equal(t, int64(100), oldest.Size)

	// Delete it
	deleted := idx.DeleteBlob(Key(1))
	require.NotNil(t, deleted)

	// Queue should be empty now
	oldest = idx.OldestBlob()
	require.Nil(t, oldest)
}

// TestFIFOQueue_LoadFromDisk tests that queue is correctly initialized from disk
func TestFIFOQueue_LoadFromDisk(t *testing.T) {
	tmpDir := t.TempDir()

	// Create index and insert values
	idx1, err := NewIndex(tmpDir)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		val := &Value{Size: int64(i * 100), SegmentID: int64(i)}
		require.NoError(t, idx1.PutBlob(Key(i), val))
	}

	// Close and reopen
	require.NoError(t, idx1.Close())

	idx2, err := NewIndex(tmpDir)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify queue is correctly initialized from disk (oldest to newest)
	var collected []int64
	idx2.ForEachBlobSorted(SortByInsertionOrder, nil, func(kv KeyValue) bool {
		collected = append(collected, kv.Val.Size)
		return true
	})

	expected := []int64{100, 200, 300, 400, 500}
	require.Equal(t, expected, collected)

	// Verify oldest
	oldest := idx2.OldestBlob()
	require.NotNil(t, oldest)
	require.Equal(t, int64(100), oldest.Size)
}

// TestBitcaskIterationOrder verifies that bitcask.ForEach iterates in key order
// This is critical for FIFO queue initialization without explicit sorting
func TestBitcaskIterationOrder(t *testing.T) {
	tmpDir := t.TempDir()
	idx, err := NewIndex(tmpDir)
	require.NoError(t, err)
	defer idx.Close()

	// Insert segments with different keys (segment IDs will be sequential timestamps)
	// Insert in a specific order
	for i := 1; i <= 10; i++ {
		val := &Value{Size: int64(i * 100), SegmentID: int64(i)}
		require.NoError(t, idx.PutBlob(Key(i*1000), val))
		time.Sleep(time.Millisecond) // Ensure different timestamps
	}

	// Close and reopen to force bitcask persistence and reload
	require.NoError(t, idx.Close())

	idx2, err := NewIndex(tmpDir)
	require.NoError(t, err)
	defer idx2.Close()

	// Verify FIFO order matches insertion order (bitcask should iterate in key order)
	var collected []int64
	idx2.ForEachBlobSorted(SortByInsertionOrder, nil, func(kv KeyValue) bool {
		collected = append(collected, kv.Val.Size)
		return true
	})

	expected := []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	require.Equal(t, expected, collected, "FIFO queue order should match insertion order (bitcask iteration must be in key order)")
}
