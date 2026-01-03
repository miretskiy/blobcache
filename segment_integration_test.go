package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache_SegmentMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-integration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with segments and Bitcask index
	cache, err := New(tmpDir, WithSegmentSize(16<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write multiple keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, 1024*1024) // 1MB each
		for j := range value {
			value[j] = byte((i + j) % 256)
		}
		cache.Put(key, value)
	}
	cache.Drain()

	// Read them back and verify
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value, found := readAll(t, cache, key)
		require.True(t, found, "key-%d should be found", i)

		// 1. Check size once
		require.Equal(t, 1024*1024, len(value), "key-%d size mismatch", i)

		// 2. Build the expected buffer once per key
		expected := make([]byte, 1024*1024)
		for j := range expected {
			expected[j] = byte((i + j) % 256)
		}

		// 3. ONE assertion for the whole 1MB slice (Super fast)
		require.Equal(t, expected, value, "key-%d data mismatch", i)
	}
}

func TestCache_SegmentModeWithDirectIO(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-directio-integration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with segments, DirectIO, and Bitcask
	cache, err := New(tmpDir, WithSegmentSize(5<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write keys with various sizes (test alignment handling)
	sizes := []int{1024, 5000, 1024 * 1024, 999, 4096}
	for i, size := range sizes {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, size)
		for j := range value {
			value[j] = byte(j % 256)
		}
		cache.Put(key, value)
	}

	cache.Drain()

	// Verify all keys
	for i, size := range sizes {
		key := []byte(fmt.Sprintf("key-%d", i))
		value, found := readAll(t, cache, key)
		require.True(t, found, "key-%d should be found", i)
		require.Equal(t, size, len(value), "key-%d size mismatch", i)

		// Verify data
		for j := 0; j < len(value); j++ {
			require.Equal(t, byte(j%256), value[j], "key-%d byte %d mismatch", i, j)
		}
	}
}
