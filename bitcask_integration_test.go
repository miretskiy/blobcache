package blobcache

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache_WithBitcaskIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcache-bitcask-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with Bitcask index
	cache, err := New(tmpDir, WithBitcaskIndex())
	require.NoError(t, err)
	defer cache.Close()

	// Write some data
	key := []byte("test-key")
	value := []byte("test-value-with-bitcask-index")

	cache.Put(key, value)
	cache.Drain()

	// Read it back
	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_BitcaskIndexEviction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcask-bitcask-evict-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Small cache to trigger eviction
	cache, err := New(tmpDir,
		WithBitcaskIndex(),
		WithMaxSize(5*1024*1024)) // 5MB limit
	require.NoError(t, err)
	defer cache.Close()

	value := make([]byte, 1024*1024) // 1MB

	// Write 6 entries (6MB > 5MB limit)
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
	}

	cache.Drain()

	// Trigger eviction
	cache.runEviction(context.Background())

	// Verify some keys were evicted (oldest ones)
	cache.memTable.TestingClearMemtable()
	_, found := cache.Get([]byte("key-0"))
	require.False(t, found, "key-0 should have been evicted")
}
