package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func newIndex(t *testing.T) (*Index, func()) {
	tmpDir, err := os.MkdirTemp("", "segments-test-*")
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatal(err)
	}
	idx, err := NewIndex(tmpDir)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatal(err)
	}
	return idx, func() {
		idx.Close()
		_ = os.RemoveAll(tmpDir)
	}
}

func TestSkipmapIndex_PutGet(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	var key Key = 123

	// Put a record
	val := &Value{Size: 1024}
	err := idx.PutBlob(key, val)
	require.NoError(t, err)

	// Get it back
	entry, err := idx.Get(key)
	require.NoError(t, err)

	// Verify
	require.EqualValues(t, 1024, entry.Size)
}

func TestSkipmapIndex_GetNotFound(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	_, err := idx.Get(Key(123))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSkipmapIndex_Delete(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	key := Key(123)
	// Insert
	val := &Value{Size: 500}
	require.NoError(t, idx.PutBlob(key, val))

	// Delete
	deleted := idx.DeleteBlob(key)
	require.NotNil(t, deleted)

	// Verify gone
	_, err := idx.Get(key)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSkipmapIndex_PutBatch(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Batch insert
	key1 := Key(123)
	key2 := Key(321)

	records := []KeyValue{
		{Key: key1, Val: &Value{SegmentID: 100, Pos: 0, Size: 100}},
		{Key: key2, Val: &Value{SegmentID: 100, Pos: 100, Size: 200}},
	}

	err := idx.PutBatch(records)
	require.NoError(t, err)

	// Verify entries exist
	entry, err := idx.Get(key1)
	require.NoError(t, err)
	require.EqualValues(t, 100, entry.Size)
	require.Equal(t, int64(100), entry.SegmentID)
	require.Equal(t, int64(0), entry.Pos)

	entry, err = idx.Get(key2)
	require.NoError(t, err)
	require.EqualValues(t, 200, entry.Size)
	require.EqualValues(t, 100, entry.Pos)
}

func TestSkipmapIndex_Scan(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Put records
	for i := 0; i < 10; i++ {
		val := &Value{Size: int64(100 * (i + 1))}
		err := idx.PutBlob(Key(i), val)
		require.NoError(t, err)
	}

	// Range all records
	var scanned []KeyValue
	idx.ForEachBlob(func(kv KeyValue) bool {
		scanned = append(scanned, kv)
		return true
	})
	require.Equal(t, 10, len(scanned))
	for _, rec := range scanned {
		require.EqualValues(t, 0, rec.Val.Size%100)
	}
}
