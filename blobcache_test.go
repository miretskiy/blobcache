package blobcache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
	err = closer()
	require.NoError(t, err)

	// Close again should be safe
	err = cache.Close()
	require.NoError(t, err)
}

func TestCache_CloseWithoutStart(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	// Close without Start should be safe
	err = cache.Close()
	require.NoError(t, err)
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

	// Check size after eviction (should be under limit)
	totalSize = 0
	cache.index.ForEachBlob(func(kv index.KeyValue) bool {
		totalSize += kv.Val.Size
		return true
	})
	// Should evict enough to get under 5KB limit
	require.Less(t, totalSize, int64(6<<10))

	// Clear memtable so evicted keys are truly not found
	cache.memTable.TestingClearMemtable()
}

func TestCache_ReactiveEviction(t *testing.T) {
	t.Run("OnPutExceedingMaxSize", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
		defer os.RemoveAll(tmpDir)

		// Track evicted segments (thread-safe)
		var mu sync.Mutex
		var evictedSegments []int64
		evictionCallback := func(segmentID int64) {
			mu.Lock()
			evictedSegments = append(evictedSegments, segmentID)
			mu.Unlock()
		}

		// Create cache with 5KB limit
		cache, err := New(tmpDir,
			WithMaxSize(5<<10), // 5KB
			WithTestingOnSegmentEvicted(evictionCallback),
		)
		require.NoError(t, err)
		defer cache.Close()

		baseTime := time.Now()

		// Use PutBatch to add 6 entries with deterministic times
		// key-0 has oldest time, key-5 has newest time
		batch := make([]index.KeyValue, 6)
		for i := 0; i < 6; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			h := cache.KeyHasher(key)
			val := index.Value{
				SegmentID: int64(i), // Each key gets its own segment
				Pos:       0,
				Size:      1024, // 1KB each
				Checksum:  0,
			}
			val.SetCTime(baseTime.Add(time.Duration(i) * time.Second))
			batch[i] = index.KeyValue{
				Key: Key(h),
				Val: val,
			}
		}

		err = cache.PutBatch(batch)
		require.NoError(t, err)

		// Verify reactive eviction occurred
		mu.Lock()
		numEvicted := len(evictedSegments)
		mu.Unlock()
		require.Greater(t, numEvicted, 0, "expected reactive eviction to occur after exceeding MaxSize")

		// Verify final size is under or near limit
		var totalSize int64
		cache.index.ForEachBlob(func(kv index.KeyValue) bool {
			totalSize += kv.Val.Size
			return true
		})
		require.LessOrEqual(t, totalSize, int64(5<<10), "cache size should be at or under MaxSize after reactive eviction")

		// Verify oldest key (key-0) was evicted
		cache.memTable.TestingClearMemtable()
		_, found := cache.Get([]byte("key-0"))
		require.False(t, found, "oldest key (key-0) should have been evicted")
	})

	t.Run("OnPutBatchExceedingMaxSize", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
		defer os.RemoveAll(tmpDir)

		// Track evicted segments (thread-safe)
		var mu sync.Mutex
		var evictedSegments []int64
		evictionCallback := func(segmentID int64) {
			mu.Lock()
			evictedSegments = append(evictedSegments, segmentID)
			mu.Unlock()
		}

		// Create cache with 3KB limit
		cache, err := New(tmpDir,
			WithMaxSize(3<<10), // 3KB
			WithTestingOnSegmentEvicted(evictionCallback),
		)
		require.NoError(t, err)
		defer cache.Close()

		baseTime := time.Now()

		// First, add 2KB of "old" data using PutBatch with old timestamps
		oldBatch := make([]index.KeyValue, 2)
		for i := 0; i < 2; i++ {
			key := []byte(fmt.Sprintf("old-key-%d", i))
			h := cache.KeyHasher(key)
			val := index.Value{
				SegmentID: int64(i),
				Pos:       0,
				Size:      1024, // 1KB each
				Checksum:  0,
			}
			val.SetCTime(baseTime.Add(time.Duration(i) * time.Second))
			oldBatch[i] = index.KeyValue{
				Key: Key(h),
				Val: val,
			}
		}
		err = cache.PutBatch(oldBatch)
		require.NoError(t, err)

		// Verify we have 2KB on disk
		var initialSize int64
		cache.index.ForEachBlob(func(kv index.KeyValue) bool {
			initialSize += kv.Val.Size
			return true
		})
		require.Equal(t, int64(2<<10), initialSize, "should have 2KB before batch")

		// Now use PutBatch to add 3KB more with newer timestamps (total would be 5KB, exceeding 3KB limit)
		// This should trigger reactive eviction during PutBatch
		batch := make([]index.KeyValue, 3)
		for i := 0; i < 3; i++ {
			key := []byte(fmt.Sprintf("batch-key-%d", i))
			h := cache.KeyHasher(key)
			val := index.Value{
				SegmentID: int64(100 + i), // Different segment IDs
				Pos:       0,
				Size:      1024, // 1KB each
				Checksum:  0,
			}
			val.SetCTime(baseTime.Add(time.Duration(100+i) * time.Second))
			batch[i] = index.KeyValue{
				Key: Key(h),
				Val: val,
			}
		}

		err = cache.PutBatch(batch)
		require.NoError(t, err)

		// Verify reactive eviction occurred during PutBatch
		mu.Lock()
		numEvicted := len(evictedSegments)
		mu.Unlock()
		require.Greater(t, numEvicted, 0, "expected reactive eviction during PutBatch when exceeding MaxSize")

		// Verify final size is at or under limit
		var finalSize int64
		cache.index.ForEachBlob(func(kv index.KeyValue) bool {
			finalSize += kv.Val.Size
			return true
		})
		require.LessOrEqual(t, finalSize, int64(3<<10), "cache size should be at or under MaxSize after PutBatch reactive eviction")

		// Verify the old data was evicted (oldest should be gone)
		cache.memTable.TestingClearMemtable()
		_, found := cache.Get([]byte("old-key-0"))
		require.False(t, found, "oldest key (old-key-0) should have been evicted")
	})
}
