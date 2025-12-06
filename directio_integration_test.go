package blobcache

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache_DirectIOWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcache-directio-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with DirectIO writes enabled
	cache, err := New(tmpDir, WithDirectIOWrites())
	require.NoError(t, err)
	defer cache.Close()

	// Write some data
	key := []byte("test-key")
	value := []byte("test-value-with-directio")

	cache.Put(key, value)
	cache.Drain()

	// Read it back
	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_DirectIOWithLargeValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcache-directio-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir, WithDirectIOWrites())
	require.NoError(t, err)
	defer cache.Close()

	// Write 1MB value (already block-aligned size)
	key := []byte("large-key")
	value := make([]byte, 1024*1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	cache.Put(key, value)
	cache.Drain()

	// Read it back and verify
	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_DirectIOWithUnalignedValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcache-directio-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir, WithDirectIOWrites())
	require.NoError(t, err)
	defer cache.Close()

	// Write unaligned size (5000 bytes) - requires padding and truncation
	key := []byte("unaligned-key")
	value := make([]byte, 5000)
	for i := range value {
		value[i] = byte(i % 256)
	}

	cache.Put(key, value)
	cache.Drain()

	// Read it back and verify size and content
	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, 5000, len(retrieved), "Size should be exact, not padded")
	require.Equal(t, value, retrieved)
}
