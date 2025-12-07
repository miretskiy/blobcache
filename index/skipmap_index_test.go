package index

import (
	"context"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/stretchr/testify/require"
)

func TestSkipmapIndex_PutGet(t *testing.T) {
	idx := NewSkipmapIndex()
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256
	key := base.NewKey([]byte("test-key"), numShards)
	now := time.Now().UnixNano()

	// Put an entry
	err := idx.Put(ctx, key, 1024, now)
	require.NoError(t, err)

	// Get it back
	var entry Entry
	err = idx.Get(ctx, key, &entry)
	require.NoError(t, err)

	// Verify
	require.Equal(t, 1024, entry.Size)
	require.Equal(t, now, entry.CTime)
}

func TestSkipmapIndex_GetNotFound(t *testing.T) {
	idx := NewSkipmapIndex()
	defer idx.Close()

	const numShards = 256
	key := base.NewKey([]byte("nonexistent"), numShards)
	var entry Entry
	err := idx.Get(context.Background(), key, &entry)

	require.ErrorIs(t, err, ErrNotFound)
}

func TestSkipmapIndex_Delete(t *testing.T) {
	idx := NewSkipmapIndex()
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256
	key := base.NewKey([]byte("delete-me"), numShards)

	// Insert
	now := time.Now().UnixNano()
	idx.Put(ctx, key, 500, now)

	// Delete
	err := idx.Delete(ctx, key)
	require.NoError(t, err)

	// Verify gone
	var entry Entry
	err = idx.Get(ctx, key, &entry)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSkipmapIndex_PutBatch(t *testing.T) {
	idx := NewSkipmapIndex()
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256

	// Batch insert
	now := time.Now().UnixNano()
	key1 := base.NewKey([]byte("key1"), numShards)
	key2 := base.NewKey([]byte("key2"), numShards)

	records := []Record{
		{Key: key1, SegmentID: 100, Pos: 0, Size: 100, CTime: now},
		{Key: key2, SegmentID: 100, Pos: 100, Size: 200, CTime: now + 1000},
	}

	err := idx.PutBatch(ctx, records)
	require.NoError(t, err)

	// Verify entries exist
	var entry Entry
	err = idx.Get(ctx, key1, &entry)
	require.NoError(t, err)
	require.Equal(t, 100, entry.Size)
	require.Equal(t, int64(100), entry.SegmentID)
	require.Equal(t, int64(0), entry.Pos)

	err = idx.Get(ctx, key2, &entry)
	require.NoError(t, err)
	require.Equal(t, 200, entry.Size)
	require.Equal(t, int64(100), entry.Pos)
}

func TestSkipmapIndex_GetOldestEntries(t *testing.T) {
	idx := NewSkipmapIndex()
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256

	// Put entries with different ctimes
	now := time.Now().UnixNano()
	idx.Put(ctx, base.NewKey([]byte("key1"), numShards), 100, now)
	idx.Put(ctx, base.NewKey([]byte("key2"), numShards), 200, now+1000)
	idx.Put(ctx, base.NewKey([]byte("key3"), numShards), 300, now+2000)

	// Get oldest 2
	it := idx.GetOldestEntries(ctx, 2)
	defer it.Close()

	require.True(t, it.Next())
	entry1, err := it.Entry()
	require.NoError(t, err)
	require.Equal(t, 100, entry1.Size)

	require.True(t, it.Next())
	entry2, err := it.Entry()
	require.NoError(t, err)
	require.Equal(t, 200, entry2.Size)

	require.False(t, it.Next())
}
