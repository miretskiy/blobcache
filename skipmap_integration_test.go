package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache_SkipmapIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "skipmap-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with skipmap index and segments
	cache, err := New(tmpDir,
		WithSkipmapIndex(),
		WithSegmentSize(10*1024*1024))
	require.NoError(t, err)
	defer cache.Close()

	// Write and read
	key := []byte("test-key")
	value := []byte("test-value-with-skipmap-index")

	cache.Put(key, value)
	cache.Drain()

	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_SkipmapIndexMultipleKeys(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "skipmap-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithSkipmapIndex(),
		WithSegmentSize(5*1024*1024))
	require.NoError(t, err)
	defer cache.Close()

	// Write multiple keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, 10000)
		for j := range value {
			value[j] = byte((i + j) % 256)
		}
		cache.Put(key, value)
	}

	cache.Drain()

	// Verify all keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value, found := readAll(t, cache, key)
		require.True(t, found, "key-%d should be found", i)
		require.Equal(t, 10000, len(value))

		// Verify data
		for j := 0; j < len(value); j++ {
			require.Equal(t, byte((i+j)%256), value[j])
		}
	}
}
