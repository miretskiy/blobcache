package index

import (
	"os"
	"testing"
	"time"

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
	now := time.Now()

	// Put a record
	err := idx.PutBlob(key, Value{Size: 1024, CTime: now})
	require.NoError(t, err)

	// Get it back
	var entry Value
	err = idx.Get(key, &entry)
	require.NoError(t, err)

	// Verify
	require.EqualValues(t, 1024, entry.Size)
	require.Equal(t, now, entry.CTime)
}

func TestSkipmapIndex_GetNotFound(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	var entry Value
	err := idx.Get(Key(123), &entry)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSkipmapIndex_Delete(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	key := Key(123)
	// Insert
	now := time.Now()
	require.NoError(t, idx.PutBlob(key, Value{Size: 500, CTime: now}))

	// Delete
	idx.DeleteBlob(key)

	// Verify gone
	var entry Value
	err := idx.Get(key, &entry)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSkipmapIndex_PutBatch(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Batch insert
	now := time.Now()
	key1 := Key(123)
	key2 := Key(321)

	records := []KeyValue{
		{Key: key1, Val: Value{SegmentID: 100, Pos: 0, Size: 100, CTime: now}},
		{Key: key2, Val: Value{SegmentID: 100, Pos: 100, Size: 200, CTime: now}},
	}

	err := idx.PutBatch(records)
	require.NoError(t, err)

	// Verify entries exist
	var entry Value
	err = idx.Get(key1, &entry)
	require.NoError(t, err)
	require.EqualValues(t, 100, entry.Size)
	require.Equal(t, int64(100), entry.SegmentID)
	require.Equal(t, int64(0), entry.Pos)

	err = idx.Get(key2, &entry)
	require.NoError(t, err)
	require.EqualValues(t, 200, entry.Size)
	require.EqualValues(t, 100, entry.Pos)
}

func TestSkipmapIndex_Scan(t *testing.T) {
	idx, cleanup := newIndex(t)
	defer cleanup()

	// Put records
	now := time.Now()
	for i := 0; i < 10; i++ {
		err := idx.PutBlob(
			Key(i),
			Value{
				Size:  int64(100 * (i + 1)),
				CTime: now.Add(time.Duration(i) * time.Hour),
			})
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
