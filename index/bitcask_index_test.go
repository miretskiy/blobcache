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

func TestBitcaskIndex_Scan(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bitcask-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := NewBitcaskIndex(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	const numShards = 256

	// Put records
	now := time.Now().UnixNano()
	idx.Put(ctx, base.NewKey([]byte("key1"), numShards), &Record{Size: 100, CTime: now})
	idx.Put(ctx, base.NewKey([]byte("key2"), numShards), &Record{Size: 200, CTime: now + 1000})
	idx.Put(ctx, base.NewKey([]byte("key3"), numShards), &Record{Size: 300, CTime: now + 2000})

	// Scan all records
	var scanned []Record
	err := idx.Scan(ctx, func(rec Record) error {
		scanned = append(scanned, rec)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(scanned))

	// Verify all records present
	sizes := make(map[int]bool)
	for _, rec := range scanned {
		sizes[rec.Size] = true
	}
	require.True(t, sizes[100])
	require.True(t, sizes[200])
	require.True(t, sizes[300])
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
