package blobcache

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/bloom"
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

func BenchmarkCache_Mixed(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()
	value := make([]byte, 1024*1024) // 1MB

	// Pre-populate 1000 keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(value)))
	for i := 0; i < b.N; i++ {
		op := i % 100

		if op < 10 {
			// 10% writes
			key := []byte(fmt.Sprintf("key-%d", 1000+i))
			cache.Put(key, value)
		} else if op < 55 {
			// 45% read hits
			key := []byte(fmt.Sprintf("key-%d", i%1000))
			cache.Get(key)
		} else {
			// 45% read misses
			key := []byte(fmt.Sprintf("missing-%d", i))
			cache.Get(key)
		}
	}
}

//
// Tests for eviction and bloom refresh
//

func TestCache_Eviction(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	// Create cache with 5MB limit and 10% hysteresis
	cache, err := New(tmpDir,
		WithMaxSize(5*1024*1024),
		WithEvictionHysteresis(0.1),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	value := make([]byte, 1024*1024) // 1MB

	// Fill cache with 6 entries (6MB > 5MB limit)
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
		time.Sleep(10 * time.Millisecond) // Ensure different mtimes
	}

	// Drain to ensure all writes complete
	cache.Drain()

	// Check size before eviction
	totalSize, err := cache.index.TotalSizeOnDisk(ctx)
	require.NoError(t, err)
	require.Greater(t, totalSize, int64(5*1024*1024))

	// Run eviction
	err = cache.runEviction(ctx)
	require.NoError(t, err)

	// Check size after eviction (should be under limit with hysteresis)
	totalSize, err = cache.index.TotalSizeOnDisk(ctx)
	require.NoError(t, err)
	// With 10% hysteresis, should evict to ~4.5MB (5MB - 10% of 5MB)
	require.Less(t, totalSize, int64(4600000))

	// Clear memtable so evicted keys are truly not found
	cache.memTable.TestingClearMemtable()

	// Verify oldest keys were evicted
	_, found := cache.Get([]byte("key-0"))
	require.False(t, found, "key-0 should have been evicted")
}

func TestCache_BloomRefresh(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	value := []byte("test-value")

	// Put some keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cache.Put(key, value)
	}

	// Drain memtable to ensure keys are flushed to index
	cache.Drain()

	// Clear bloom filter
	emptyBloom := bloom.New(1000, 0.01)
	cache.bloom.Store(emptyBloom)

	// Verify bloom is empty
	require.False(t, cache.bloom.Load().Test([]byte("key-0")))

	// Rebuild bloom
	err = cache.rebuildBloom(ctx)
	require.NoError(t, err)

	// Verify bloom now contains keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		require.True(t, cache.bloom.Load().Test(key))
	}
}

//
// Benchmarks for eviction and bloom refresh
//

// bulkPopulateIndex uses DuckDB's generate_series to quickly insert test data
// ONLY for testing/benchmarking - bypasses normal Put() path
func bulkPopulateIndex(cache *Cache, count int) error {
	ctx := context.Background()
	// Type assert to DuckDB index (this helper only works with DuckDB)
	duckdbIndex, ok := cache.index.(*index.Index)
	if !ok {
		return fmt.Errorf("bulkPopulateIndex only works with DuckDB index")
	}
	db := duckdbIndex.TestingGetDB()

	query := fmt.Sprintf(`
		INSERT INTO entries (key, shard_id, file_id, size, ctime, mtime)
		SELECT
			CAST(CONCAT('key-', i) AS BLOB) as key,
			i %% 256 as shard_id,
			i as file_id,
			1024 as size,
			(1000000000 + i) as ctime,
			(1000000000 + i) as mtime
		FROM generate_series(0, %d) as s(i)
	`, count-1)

	_, err := db.ExecContext(ctx, query)
	return err
}

func BenchmarkCache_BloomRebuild_1M(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	// Bulk populate 1M keys using DuckDB generate_series (ONCE)
	b.Logf("Bulk populating 1M keys...")
	if err := bulkPopulateIndex(cache, 1_000_000); err != nil {
		b.Fatal(err)
	}
	b.Logf("Done populating")

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := cache.rebuildBloom(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndex_TotalSizeOnDisk_1M(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	// Bulk populate 1M keys using DuckDB generate_series (ONCE)
	b.Logf("Bulk populating 1M keys...")
	if err := bulkPopulateIndex(cache, 1_000_000); err != nil {
		b.Fatal(err)
	}
	b.Logf("Done populating")

	ctx := context.Background()
	var totalSize int64
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var err error
		totalSize, err = cache.index.TotalSizeOnDisk(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = totalSize // Prevent elision
}

func BenchmarkIndex_GetOldestEntries_1M(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-bench-*")
	defer os.RemoveAll(tmpDir)

	cache, _ := New(tmpDir)
	defer cache.Close()

	// Bulk populate 1M keys using DuckDB generate_series (ONCE)
	b.Logf("Bulk populating 1M keys...")
	if err := bulkPopulateIndex(cache, 1_000_000); err != nil {
		b.Fatal(err)
	}
	b.Logf("Done populating")

	ctx := context.Background()
	var count int
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		it := cache.index.GetOldestEntries(ctx, 1000)
		count = 0
		for it.Next() {
			_, err := it.Entry()
			if err != nil {
				b.Fatal(err)
			}
			count++
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		it.Close()
	}
	_ = count // Prevent elision
}

//
// Tests for memtable
//

func TestCache_MemTableEnabled(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	// Memtable is always enabled (required for apples-to-apples RocksDB comparison)
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	require.NotNil(t, cache.memTable, "Memtable should always be enabled")

	// Verify immediate read-after-write (from memtable, not disk)
	cache.Put([]byte("key"), []byte("value"))

	// Should read from memtable immediately (before flush)
	data, found := readAll(t, cache, []byte("key"))
	require.True(t, found)
	require.Equal(t, []byte("value"), data)
}

func TestCache_MemTableBuffered(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	require.NotNil(t, cache.memTable, "Memtable should be created")

	// Queue writes (should be async)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		cache.Put(key, value)
	}

	// Drain and close
	cache.Drain()
	cache.Close()

	// Reopen and verify all writes persisted
	cache2, err := New(tmpDir)
	require.NoError(t, err)
	defer cache2.Close()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		data, found := readAll(t, cache2, key)
		require.True(t, found)
		require.Equal(t, []byte(fmt.Sprintf("value-%d", i)), data)
	}
}

func TestCache_MemTableUnbuffered(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	cache.Put([]byte("key"), []byte("value"))

	// Drain and close
	cache.Drain()
	cache.Close()

	// Reopen and verify
	cache2, err := New(tmpDir)
	require.NoError(t, err)
	defer cache2.Close()

	data, found := readAll(t, cache2, []byte("key"))
	require.True(t, found)
	require.Equal(t, []byte("value"), data)
}
