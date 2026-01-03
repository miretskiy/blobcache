package blobcache

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/miretskiy/blobcache/index"
	"github.com/stretchr/testify/require"
)

// Helper to read all bytes from Get()
func readAll(t *testing.T, cache *Cache, key []byte) ([]byte, bool) {
	reader, found := cache.Get(key)
	if !found {
		return nil, false
	}

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	if closer, ok := reader.(io.Closer); ok {
		closer.Close()
	}

	return data, true
}

func TestCache_PutGet_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Standard Put
	cache.Put(key, value)

	// Flush memtable to disk to test the full IO path (Index + Storage)
	cache.Drain()

	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_SelfHealing_OnCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("healing-key")
	value := []byte("precious-data")
	h := cache.KeyHasher(key)

	cache.Put(key, value)
	cache.Drain()

	// 1. Manually corrupt the storage by deleting the segment file
	entry, ok := cache.index.Get(h)
	require.True(t, ok)

	// Use shard-aware path helper
	segmentPath := getSegmentPath(tmpDir, cache.Shards, entry.SegmentID)
	err = os.Remove(segmentPath)
	require.NoError(t, err)

	// 2. Attempt Get.
	// The Index has the entry, but Storage will return a failure.
	// This triggers self-healing (DeleteBlobs) inside Cache.Get.
	_, found := cache.Get(key)
	require.False(t, found, "Get should return false after storage failure")

	// 3. Verify Self-Healing: The entry should have been purged from the index
	_, inIndex := cache.index.Get(h)
	require.False(t, inIndex, "Index should have been purged via self-healing")
}

func TestCache_BloomGhostTracking(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// 1. Manually inject a key into the Bloom filter that isn't in the index
	key := []byte("ghost-key")
	h := cache.KeyHasher(key)
	cache.bloom.Load().AddHash(h)

	// 2. Perform Get. Bloom says YES, Index says NO.
	_, found := cache.Get(key)
	require.False(t, found)

	// 3. Verify ghost hit was tracked
	require.Equal(t, uint64(1), cache.bloom.ghosts.Load(), "Ghost hit should be recorded")
	require.Equal(t, uint64(1), cache.bloom.hits.Load(), "Hit should also be recorded")
}

func TestCache_Eviction_Headroom(t *testing.T) {
	tmpDir := t.TempDir()
	// Small cache: 10KB. 5% hysteresis means target is 9.5KB.
	cache, err := New(tmpDir, WithMaxSize(10*1024), WithLargeWriteThreshold(0))
	require.NoError(t, err)
	defer cache.Close()

	// Put 12 blobs of 1KB each
	for i := 0; i < 12; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, make([]byte, 1024))
	}
	cache.Drain()

	// Trigger eviction manually for the test
	err = cache.runEvictionSieve(10 * 1024)
	require.NoError(t, err)

	// Verify we are under the 95% threshold (9.5KB)
	// That means we should have at most 9 blobs left.
	count := 0
	cache.index.ForEachBlob(func(e index.Entry) bool {
		count++
		return true
	})
	require.LessOrEqual(t, count, 9, "Should have evicted down to hysteresis target")

	// Check that deletions counter was updated
	require.Greater(t, cache.bloom.deletions.Load(), int64(0))
}

func TestCache_HolePunching_Physical(t *testing.T) {
	// Only run on systems likely to support hole punching (Linux)
	// On macOS, PunchHole is often a no-op or returns EOPNOTSUPP
	if runtime.GOOS == "darwin" {
		t.Skip("Skipping physical reclamation test on Darwin: fcntl F_PUNCHHOLE behavior differs")
	}

	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Write a large blob (> 4KB block size)
	val := make([]byte, 8192)
	key := []byte("big-blob")
	cache.Put(key, val)
	cache.Drain()

	// Get entry info
	h := cache.KeyHasher(key)
	entry, ok := cache.index.Get(h)
	require.True(t, ok)

	segmentPath := getSegmentPath(tmpDir, cache.Shards, entry.SegmentID)
	fiBefore, err := os.Stat(segmentPath)
	require.NoError(t, err)

	// 1. Mark as deleted in Index (Durable + RAM)
	err = cache.index.DeleteBlobs(entry)
	require.NoError(t, err)

	// 2. Physically reclaim space via Storage
	err = cache.storage.HolePunchBlob(entry.SegmentID, entry.Pos, entry.Size)
	require.NoError(t, err)

	fiAfter, err := os.Stat(segmentPath)
	require.NoError(t, err)

	// Logical size should remain constant (FALLOC_FL_KEEP_SIZE)
	require.Equal(t, fiBefore.Size(), fiAfter.Size(), "Logical size must stay constant")
}

func TestCache_Restart_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Phase 1: Write and Close
	cache1, err := New(tmpDir)
	require.NoError(t, err)
	cache1.Put([]byte("k1"), []byte("v1"))
	cache1.Drain()
	cache1.Close()

	// Phase 2: Open and Verify
	cache2, err := New(tmpDir)
	require.NoError(t, err)
	defer cache2.Close()

	val, found := readAll(t, cache2, []byte("k1"))
	require.True(t, found)
	require.Equal(t, []byte("v1"), val)
}

// Benchmarks

func BenchmarkCache_Get_WithBloom(b *testing.B) {
	tmpDir := b.TempDir()
	cache, _ := New(tmpDir)
	defer cache.Close()

	key := []byte("bench-key")
	cache.Put(key, make([]byte, 1024))
	cache.Drain()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cache.Get(key)
	}
}
