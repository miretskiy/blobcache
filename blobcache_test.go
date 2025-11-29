package blobcache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNew_CreatesDirectories(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "blobcache-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Verify key directories created
	require.DirExists(t, filepath.Join(tmpDir, "db"))
	require.DirExists(t, filepath.Join(tmpDir, "blobs"))
	require.DirExists(t, filepath.Join(tmpDir, "blobs", "shard-000"))
	require.DirExists(t, filepath.Join(tmpDir, "blobs", "shard-255"))

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

func TestNew_ShardMismatch(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache1, err := New(tmpDir, WithShards(256))
	require.NoError(t, err)
	cache1.Close()

	cache2, err := New(tmpDir, WithShards(512))
	require.NoError(t, err, "Should warn but not fail")
	cache2.Close()
}

func TestCache_StartClose(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	closer, err := cache.Start()
	require.NoError(t, err)

	// Workers running
	time.Sleep(100 * time.Millisecond)

	// Close gracefully
	err = closer()
	require.NoError(t, err)

	// Close again should be safe
	err = cache.Close()
	require.NoError(t, err, "Second Close should be safe")
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

func TestCache_BloomPersistence(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	// Create and populate bloom
	cache1, err := New(tmpDir)
	require.NoError(t, err)

	bloom1 := cache1.bloom.Load()
	bloom1.Add([]byte("persisted-key"))
	cache1.Close()

	// Reopen and verify
	cache2, err := New(tmpDir)
	require.NoError(t, err)
	defer cache2.Close()

	bloom2 := cache2.bloom.Load()
	require.True(t, bloom2.Test([]byte("persisted-key")), "Bloom should persist across restarts")
}

func TestCache_CustomOptions(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithShards(128),
		WithBloomFPRate(0.001),
		WithChecksums(false),
	)
	require.NoError(t, err)
	defer cache.Close()

	require.Equal(t, 128, cache.cfg.Shards)
	require.Equal(t, 0.001, cache.cfg.BloomFPRate)
	require.False(t, cache.cfg.Checksums)
}

func TestCache_PutGet(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	// Put
	err = cache.Put(ctx, key, value)
	require.NoError(t, err)

	// Get
	retrieved, err := cache.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, retrieved)
}

func TestCache_GetNotFound(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	// Get non-existent key
	_, err = cache.Get(ctx, []byte("nonexistent"))
	require.ErrorIs(t, err, ErrNotFound)
}

// Benchmarks

func BenchmarkCache_Put(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	ctx := context.Background()
	value := make([]byte, 1024*1024) // 1MB

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(ctx, key, value)
	}
}

func BenchmarkCache_GetHit(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	ctx := context.Background()
	value := make([]byte, 1024*1024) // 1MB

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(ctx, key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%1000))
		cache.Get(ctx, key)
	}
}

func BenchmarkCache_GetMiss(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	ctx := context.Background()
	value := make([]byte, 1024*1024) // 1MB

	// Pre-populate with different keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("existing-%d", i))
		cache.Put(ctx, key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("missing-%d", i))
		cache.Get(ctx, key)
	}
}

func BenchmarkCache_Mixed(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	ctx := context.Background()
	value := make([]byte, 1024*1024) // 1MB

	// Pre-populate 1000 keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(ctx, key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		op := i % 100

		if op < 10 {
			// 10% writes
			key := []byte(fmt.Sprintf("key-%d", 1000+i))
			cache.Put(ctx, key, value)
		} else if op < 55 {
			// 45% read hits
			key := []byte(fmt.Sprintf("key-%d", i%1000))
			cache.Get(ctx, key)
		} else {
			// 45% read misses
			key := []byte(fmt.Sprintf("missing-%d", i))
			cache.Get(ctx, key)
		}
	}
}
