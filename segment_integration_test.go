package blobcache

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
	"github.com/stretchr/testify/require"
)

func TestCache_SegmentMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-integration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with segments and Bitcask index
	cache, err := New(tmpDir,
		WithSegmentSize(10*1024*1024), // 10MB segments
		WithBitcaskIndex())            // Required for segments
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
		value, found := cache.Get(key)
		if !found {
			t.Logf("DEBUG: key-%d not found, checking why...", i)
			// Try to see if it's in index
			k := base.NewKey(key, cache.cfg.Shards)
			var entry index.Entry
			if err := cache.index.Get(context.Background(), k, &entry); err != nil {
				t.Logf("  Index lookup failed: %v", err)
			} else {
				t.Logf("  Found in index: segmentID=%d, pos=%d, size=%d", entry.SegmentID, entry.Pos, entry.Size)
			}
		}
		require.True(t, found, "key-%d should be found", i)
		require.Equal(t, 1024*1024, len(value), "key-%d size mismatch", i)

		// Verify data integrity
		for j := 0; j < len(value); j++ {
			expected := byte((i + j) % 256)
			require.Equal(t, expected, value[j], "key-%d byte %d mismatch", i, j)
		}
	}
}

func TestCache_SegmentModeWithDirectIO(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment-directio-integration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create cache with segments, DirectIO, and Bitcask
	cache, err := New(tmpDir,
		WithSegmentSize(5*1024*1024), // 5MB segments
		WithDirectIOWrites(),
		WithBitcaskIndex())
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
		value, found := cache.Get(key)
		require.True(t, found, "key-%d should be found", i)
		require.Equal(t, size, len(value), "key-%d size mismatch", i)

		// Verify data
		for j := 0; j < len(value); j++ {
			require.Equal(t, byte(j%256), value[j], "key-%d byte %d mismatch", i, j)
		}
	}
}
