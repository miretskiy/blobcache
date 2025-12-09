package index

import (
	"context"
	"os"
	"testing"
	"time"

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
	key := []byte("test-key")
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

	key := []byte("nonexistent")
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

	key := []byte("delete-me")

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

	// Put records
	now := time.Now().UnixNano()
	idx.Put(ctx, []byte("key1"), &Record{Size: 100, CTime: now})
	idx.Put(ctx, []byte("key2"), &Record{Size: 200, CTime: now + 1000})
	idx.Put(ctx, []byte("key3"), &Record{Size: 300, CTime: now + 2000})

	// Range all records
	var scanned []Record
	err := idx.Range(ctx, func(rec Record) error {
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

	// Batch insert multiple records
	now := time.Now().UnixNano()
	key1 := []byte("key1")
	key2 := []byte("key2")

	records := []Record{
		{Key: key1, Size: 100, CTime: now},
		{Key: key2, Size: 200, CTime: now + 1000},
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
