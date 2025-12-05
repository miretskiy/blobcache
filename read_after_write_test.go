package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCache_ReadAfterWrite verifies that values are immediately readable
// after Put, even before flushing to disk (read from memtable)
func TestCache_ReadAfterWrite(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Write multiple keys rapidly
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-data", i))

		cache.Put(key, value)

		// Immediately read back (should come from memtable, NOT disk)
		result, found := cache.Get(key)
		require.True(t, found, "key-%d should be readable immediately after Put", i)
		require.Equal(t, value, result, "value mismatch for key-%d", i)
	}

	// Verify all keys are still readable (from memtable)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expected := []byte(fmt.Sprintf("value-%d-data", i))

		result, found := cache.Get(key)
		require.True(t, found, "key-%d should be readable from memtable", i)
		require.Equal(t, expected, result, "value mismatch for key-%d", i)
	}

	// Now drain and verify keys are readable from disk
	cache.Drain()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expected := []byte(fmt.Sprintf("value-%d-data", i))

		result, found := cache.Get(key)
		require.True(t, found, "key-%d should be readable from disk after Drain", i)
		require.Equal(t, expected, result, "value mismatch for key-%d after Drain", i)
	}
}

// TestCache_UpdateInMemtable verifies that updating a key in memtable
// returns the latest value (skipmap overwrites)
func TestCache_UpdateInMemtable(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("test-key")

	// Write initial value
	cache.Put(key, []byte("value-v1"))

	// Read back
	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, []byte("value-v1"), result)

	// Update value (should overwrite in active skipmap)
	cache.Put(key, []byte("value-v2"))

	// Read back updated value
	result, found = cache.Get(key)
	require.True(t, found)
	require.Equal(t, []byte("value-v2"), result, "should get updated value from memtable")
}
