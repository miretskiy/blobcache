package index

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/stretchr/testify/require"
)

func TestBitcaskIndex_PutGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	idx, err := NewBitcaskIndex(tmpDir)
	require.NoError(t, err)
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256
	key := base.NewKey([]byte("test-key"), numShards)
	now := time.Now().UnixNano()

	// Put a record
	err = idx.Put(ctx, key, &Record{Size: 1024, CTime: now})
	require.NoError(t, err)

	// Get it back
	var entry Record
	err = idx.Get(ctx, key, &entry)
	require.NoError(t, err)

	// Verify
	require.Equal(t, 1024, entry.Size)
	require.Equal(t, now, entry.CTime)
}

func TestBitcaskIndex_GetNotFound(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bitcask-test-*")
	defer os.RemoveAll(tmpDir)

	idx, err := NewBitcaskIndex(tmpDir)
	require.NoError(t, err)
	defer idx.Close()

	const numShards = 256
	key := base.NewKey([]byte("nonexistent"), numShards)
	var entry Record
	err = idx.Get(context.Background(), key, &entry)

	require.ErrorIs(t, err, ErrNotFound)
}

func TestBitcaskIndex_Delete(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bitcask-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := NewBitcaskIndex(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256
	key := base.NewKey([]byte("delete-me"), numShards)

	// Insert
	now := time.Now().UnixNano()
	idx.Put(ctx, key, &Record{Size: 500, CTime: now})

	// Delete
	err := idx.Delete(ctx, key)
	require.NoError(t, err)

	// Verify gone
	var entry Record
	err = idx.Get(ctx, key, &entry)
	require.ErrorIs(t, err, ErrNotFound)

	// Delete again - Bitcask writes tombstone, doesn't error on non-existent
	err = idx.Delete(ctx, key)
	require.NoError(t, err)
}

func TestBitcaskIndex_TotalSizeOnDisk(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bitcask-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := NewBitcaskIndex(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256

	// Empty database
	total, err := idx.TotalSizeOnDisk(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), total)

	// Put records
	now := time.Now().UnixNano()
	idx.Put(ctx, base.NewKey([]byte("key1"), numShards), &Record{Size: 100, CTime: now})
	idx.Put(ctx, base.NewKey([]byte("key2"), numShards), &Record{Size: 200, CTime: now})
	idx.Put(ctx, base.NewKey([]byte("key3"), numShards), &Record{Size: 300, CTime: now})

	total, err = idx.TotalSizeOnDisk(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(600), total)

	// Delete one
	idx.Delete(ctx, base.NewKey([]byte("key2"), numShards))

	total, _ = idx.TotalSizeOnDisk(ctx)
	require.Equal(t, int64(400), total)
}

func TestBitcaskIndex_GetOldestRecords(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bitcask-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := NewBitcaskIndex(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256

	// Put records with different ctimes
	now := time.Now().UnixNano()
	idx.Put(ctx, base.NewKey([]byte("key1"), numShards), &Record{Size: 100, CTime: now})
	idx.Put(ctx, base.NewKey([]byte("key2"), numShards), &Record{Size: 200, CTime: now + 1000})
	idx.Put(ctx, base.NewKey([]byte("key3"), numShards), &Record{Size: 300, CTime: now + 2000})

	// Get oldest 2 records
	it := idx.GetOldestRecords(ctx, 2)
	defer it.Close()

	require.True(t, it.Next())
	rec1, err := it.Record()
	require.NoError(t, err)
	require.Equal(t, 100, rec1.Size) // Oldest (now)

	require.True(t, it.Next())
	rec2, err := it.Record()
	require.NoError(t, err)
	require.Equal(t, 200, rec2.Size) // Second oldest (now+1000)

	require.False(t, it.Next()) // Should only return 2
}

func TestBitcaskIndex_PutBatch(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bitcask-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := NewBitcaskIndex(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256

	// Batch insert multiple records
	now := time.Now().UnixNano()
	key1 := base.NewKey([]byte("key1"), numShards)
	key2 := base.NewKey([]byte("key2"), numShards)

	records := []Record{
		{Key: key1.Raw(), Size: 100, CTime: now},
		{Key: key2.Raw(), Size: 200, CTime: now + 1000},
	}

	err := idx.PutBatch(ctx, records)
	require.NoError(t, err)

	// Verify entries exist
	var entry Record
	err = idx.Get(ctx, key1, &entry)
	require.NoError(t, err)
	require.Equal(t, 100, entry.Size)

	err = idx.Get(ctx, key2, &entry)
	require.NoError(t, err)
	require.Equal(t, 200, entry.Size)
}
