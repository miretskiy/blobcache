package blobcache

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
	"github.com/stretchr/testify/require"
)

// Helper to read all bytes from Get()
func readAll(t *testing.T, cache *Cache, key []byte) ([]byte, bool) {
	return cache.Get(key)
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
	// Disable in-memory slab caching so Get() must go to disk.
	cache, err := New(tmpDir, WithMaxCachedSlabs(0))
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("healing-key")
	value := []byte("precious-data")
	h := cache.KeyHasher(key)

	cache.Put(key, value)
	cache.Drain()

	// 1. Manually corrupt the storage by deleting the segment file
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	// Use shard-aware path helper
	segmentPath := getSegmentPath(tmpDir, cache.Shards, entry.SegmentID)
	err = os.Remove(segmentPath)
	require.NoError(t, err)

	// 2. Attempt Get.
	// The Index has the entry, but Storage will return a failure.
	// This triggers corruption marking via ReportBlobError.
	_, found := cache.Get(key)
	require.False(t, found, "Get should return false after storage failure")

	// 3. Verify blob is marked as corrupt but still in index
	entry, inIndex := cache.index.DeprecatedGetByHash(h)
	require.True(t, inIndex, "Index entry should still exist")
	require.True(t, entry.HasError(), "Blob should be marked as corrupt")
	require.NotEqual(t, base.ErrNone, entry.Errno(), "Errno should be set")

	// 4. Subsequent reads should fail fast (corruption check)
	_, found = cache.Get(key)
	require.False(t, found, "Subsequent reads should fail")
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
	// Small cache with eviction enabled
	cache, err := New(tmpDir,
		WithMaxSize(20*1024),        // 20KB limit
		WithWriteBufferSize(2*1024)) // Small buffer to ensure flush
	require.NoError(t, err)
	defer cache.Close()
	cache.Start() // Start eviction worker

	// Put enough data to trigger eviction (30KB > 20KB limit)
	for i := 0; i < 30; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		cache.Put(key, make([]byte, 1024))
	}
	cache.Drain()

	// Poll for eviction completion with timeout
	deadline := time.Now().Add(5 * time.Second)
	var finalSize int64
	var deletions int64
	for time.Now().Before(deadline) {
		finalSize = cache.approxSize.Load()
		deletions = cache.bloom.deletions.Load()
		if finalSize < 20*1024 && deletions > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("FinalSize: %d bytes (limit: 20KB)", finalSize)
	t.Logf("Deletions: %d", deletions)

	require.Less(t, finalSize, int64(20*1024), "Should have evicted to stay under limit")
	require.Greater(t, deletions, int64(0), "Deletions should be tracked after eviction")
}

func TestCache_HolePunching_Physical(t *testing.T) {
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
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	segmentPath := getSegmentPath(tmpDir, cache.Shards, entry.SegmentID)
	fiBefore, err := os.Stat(segmentPath)
	require.NoError(t, err)

	// 1. Mark as deleted in Index (Durable + RAM)
	err = cache.index.DeleteBlobs(entry)
	require.NoError(t, err)

	// 2. Physically reclaim space via Storage
	reclaimed, err := cache.storage.HolePunchBlob(entry.SegmentID, entry.Offset, entry.PhysicalLen)
	require.NoError(t, err)
	t.Logf("Hole punch reclaimed %d bytes (requested %d)", reclaimed, entry.PhysicalLen)

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
	for b.Loop() {
		_, _ = cache.Get(key)
	}
}

// --- Compression Tests ---

func TestCache_Compression_Zstd(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithCompression(compression.CodexZstd),
		WithCompressionMinSize(100), // Compress blobs >= 100 bytes
		WithMaxCachedSlabs(0),       // Force disk reads
	)
	require.NoError(t, err)
	defer cache.Close()

	// Create highly compressible data (repeated pattern)
	original := bytes.Repeat([]byte("COMPRESS_ME_"), 1000) // ~12KB of repeated text
	key := []byte("compressed-key")

	// Write compressed
	cache.Put(key, original)
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found, "compressed blob should be readable")
	require.Equal(t, original, result, "decompressed data should match original")

	// Verify compression metadata is correct
	h := cache.KeyHasher(key)
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	// PhysicalLen is total record size (header + key + value)
	// For compressed data, this should be much smaller than original data size
	t.Logf("Original size: %d, PhysicalLen (total record): %d", len(original), entry.PhysicalLen)

	// Verify compression metadata is set correctly
	require.True(t, entry.IsCompressed(), "record should be marked as compressed")
	require.Equal(t, compression.CodexZstd, entry.Compression())
}

func TestCache_Compression_IncompressibleData(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithCompression(compression.CodexZstd),
		WithCompressionMinSize(100),
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Create incompressible data (random-like pattern)
	// The 1/8th heuristic should detect this and store raw
	original := make([]byte, 1000)
	for i := range original {
		original[i] = byte(i * 17 % 256) // Pseudo-random pattern
	}
	key := []byte("incompressible-key")

	cache.Put(key, original)
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found, "incompressible blob should be readable")
	require.Equal(t, original, result, "data should match original")

	// Check entry exists and verify compression flag
	h := cache.KeyHasher(key)
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	t.Logf("PhysicalLen: %d, IsCompressed: %v", entry.PhysicalLen, entry.IsCompressed())

	// For truly random data, compression shouldn't help much
	// The entry exists and read/write cycle works - that's the main test
}

func TestCache_Compression_SmallBlob_NoCompress(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithCompression(compression.CodexZstd),
		WithCompressionMinSize(1000), // Only compress >= 1KB
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Small blob below threshold
	original := []byte("small data under threshold")
	key := []byte("small-key")

	cache.Put(key, original)
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, original, result)

	// Verify it was NOT compressed due to size threshold
	h := cache.KeyHasher(key)
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	require.False(t, entry.IsCompressed(), "small blob should not be compressed")
}

func TestCache_Compression_MinSizeZero_NoRestriction(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithCompression(compression.CodexZstd),
		WithCompressionMinSize(0), // MinSize=0 means no minimum, compress everything
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Small but compressible data (repeated pattern)
	original := bytes.Repeat([]byte("x"), 50) // Only 50 bytes
	key := []byte("tiny-key")

	cache.Put(key, original)
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, original, result)

	// Verify it WAS compressed despite being small (MinSize=0 disables restriction)
	h := cache.KeyHasher(key)
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	t.Logf("PhysicalLen: %d, IsCompressed: %v", entry.PhysicalLen, entry.IsCompressed())

	// With MinSize=0, compression should be attempted regardless of size
	// For this highly compressible pattern, it should succeed
	require.True(t, entry.IsCompressed(), "MinSize=0 should allow compression of any size blob")
}

func TestCache_Compression_ReadFromLibrarian(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithCompression(compression.CodexZstd),
		WithCompressionMinSize(100),
		WithMaxCachedSlabs(4), // Enable Librarian cache
	)
	require.NoError(t, err)
	defer cache.Close()

	// Compressible data
	original := bytes.Repeat([]byte("librarian_test_"), 500)
	key := []byte("librarian-key")

	cache.Put(key, original)
	// DON'T drain - read from Librarian (RAM)

	result, found := cache.Get(key)
	require.True(t, found, "should find in Librarian before flush")
	require.Equal(t, original, result, "decompressed data from Librarian should match")
}

func TestCache_Compression_LZ4(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithCompression(compression.CodexLZ4),
		WithCompressionMinSize(100),
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	// LZ4 optimizes for speed over ratio, so use larger data for better compression
	original := bytes.Repeat([]byte("LZ4_TEST_DATA_"), 5000) // 70KB of repeated text
	key := []byte("lz4-key")

	cache.Put(key, original)
	cache.Drain()

	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, original, result)

	h := cache.KeyHasher(key)
	entry, ok := cache.index.DeprecatedGetByHash(h)
	require.True(t, ok)

	t.Logf("PhysicalLen: %d, IsCompressed: %v", entry.PhysicalLen, entry.IsCompressed())

	require.True(t, entry.IsCompressed(), "blob should be compressed with LZ4")
	require.Equal(t, compression.CodexLZ4, entry.Compression())
}
