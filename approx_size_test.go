package blobcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestApproxSize_UpdatedAfterEviction verifies approxSize is decremented after eviction
func TestApproxSize_UpdatedAfterEviction(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithMaxSize(5<<10), // 5KB limit
	)
	require.NoError(t, err)
	defer cache.Close()

	// Start eviction worker
	_, err = cache.Start()
	require.NoError(t, err)

	// Add 6KB using Put (goes through memtable, creates real segments)
	initialSize := cache.approxSize.Load()
	require.Equal(t, int64(0), initialSize, "initial size should be 0")

	for i := 0; i < 6; i++ {
		data := make([]byte, 1024)
		cache.Put([]byte{byte(i)}, data)
	}
	cache.Drain() // Wait for flush to disk

	// Eviction runs async in background worker
	// Wait for approxSize to decrease (eviction updates it)
	require.Eventually(t, func() bool {
		return cache.approxSize.Load() <= int64(5<<10)
	}, 2*time.Second, 10*time.Millisecond, "approxSize should decrease after eviction")
}

// TestApproxSize_NoRepeatEviction verifies second PutBatch doesn't re-trigger eviction
func TestApproxSize_NoRepeatEviction(t *testing.T) {
	tmpDir := t.TempDir()

	evictionCount := 0
	cache, err := New(tmpDir,
		WithMaxSize(5<<10), // 5KB limit
		WithTestingInjectEvictError(func() error {
			evictionCount++
			return nil
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	_, err = cache.Start()
	require.NoError(t, err)

	// Add 6KB (triggers eviction, should evict to ~5KB)
	for i := 0; i < 6; i++ {
		data := make([]byte, 1024)
		cache.Put([]byte{byte(i)}, data)
	}
	cache.Drain()

	// Wait for first eviction to complete
	require.Eventually(t, func() bool {
		return evictionCount > 0
	}, time.Second, 10*time.Millisecond, "eviction should run")

	initialEvictions := evictionCount

	// Verify we're under limit now
	require.Eventually(t, func() bool {
		return cache.approxSize.Load() <= int64(5<<10)
	}, time.Second, 10*time.Millisecond, "should be under limit after eviction")

	// Add small write (should NOT trigger eviction since we're under limit)
	data := make([]byte, 100)
	cache.Put([]byte{100}, data)
	cache.Drain()

	// Give time for any incorrect eviction to trigger
	time.Sleep(100 * time.Millisecond)

	finalEvictions := evictionCount
	// Should not trigger another eviction (we're under limit)
	require.Equal(t, initialEvictions, finalEvictions, "should not re-trigger eviction when under limit")
}
