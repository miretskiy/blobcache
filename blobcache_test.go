package blobcache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	// Close if it's a Closer
	if closer, ok := reader.(io.Closer); ok {
		closer.Close()
	}

	return data, true
}

func TestNew_CreatesDirectories(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcache-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Verify key directories created
	require.DirExists(t, filepath.Join(tmpDir, "db"))

	// Verify marker
	require.FileExists(t, filepath.Join(tmpDir, ".initialized"))
}

func TestNew_Idempotent(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache1, err := New(tmpDir)
	require.NoError(t, err, "First New should succeed")
	cache1.Close()

	cache2, err := New(tmpDir)
	require.NoError(t, err, "Second New should succeed (idempotent)")
	cache2.Close()
}

func TestCache_StartClose(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	closer, err := cache.Start()
	require.NoError(t, err)

	// Close gracefully
	closer()

	// Close again should be safe
	cache.Close()
}

func TestCache_CloseWithoutStart(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	// Close without Start should be safe
	cache.Close()
}

func TestCache_CustomOptions(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithShards(128),
		WithBloomFPRate(0.001),
		// IncludeChecksums disabled by default
	)
	require.NoError(t, err)
	defer cache.Close()

	require.Equal(t, 128, cache.Shards)
	require.Equal(t, 0.001, cache.BloomFPRate)
	require.Nil(t, cache.Resilience.ChecksumHasher, "checksums should be disabled by default")
}

func TestCache_PutGet(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Put
	cache.Put(key, value)

	// Drain memtable to ensure write completes
	cache.Drain()

	// Get
	retrieved, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_GetNotFound(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Get non-existent key
	_, found := cache.Get([]byte("nonexistent"))
	require.False(t, found)
}

// Benchmarks

func BenchmarkCache_Put(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	value := make([]byte, 1024*1024) // 1MB

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
	}
}

func BenchmarkCache_GetHit(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()
	value := make([]byte, 1024*1024) // 1MB

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%1000))
		cache.Get(key)
	}
}

func BenchmarkCache_GetMiss(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()
	value := make([]byte, 1024*1024) // 1MB

	// Pre-populate with different keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("existing-%d", i))
		cache.Put(key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("missing-%d", i))
		cache.Get(key)
	}
}

func TestCache_Eviction(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	// Create cache without size limit initially (to test eviction manually)
	cache, err := New(tmpDir, WithTestingFlushOnPut())
	require.NoError(t, err)
	defer cache.Close()

	value := make([]byte, 1024)

	// Fill cache with 6 entries (6MB)
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
	}

	// Drain to ensure all writes complete
	cache.Drain()

	// Check size before eviction
	var totalSize int64
	cache.index.ForEachBlob(func(kv index.KeyValue) bool {
		totalSize += kv.Val.Size
		return true
	})
	require.Equal(t, int64(6<<10), totalSize)

	// Run eviction
	err = cache.runEviction(5 << 10)
	require.NoError(t, err)

	// Check size after eviction (should be under limit with hysteresis)
	totalSize = 0
	cache.index.ForEachBlob(func(kv index.KeyValue) bool {
		totalSize += kv.Val.Size
		return true
	})
	// With 10% hysteresis, should evict to ~4.5MB (5MB - 10% of 5MB)
	require.Less(t, totalSize, int64(6<<10))

	// Clear memtable so evicted keys are truly not found
	cache.memTable.TestingClearMemtable()
}
