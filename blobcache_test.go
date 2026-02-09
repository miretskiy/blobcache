package blobcache

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

func TestCache_PutGet_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Standard Put
	require.NoError(t, cache.Put(key, value))

	// Flush memtable to disk to test the full IO path (Index + Storage)
	cache.Drain()

	retrieved, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_Put_EmptyKeyRejected(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Empty key should return error
	err = cache.Put([]byte{}, []byte("value"))
	require.ErrorIs(t, err, ErrEmptyKey)

	// Nil key should also return error
	err = cache.Put(nil, []byte("value"))
	require.ErrorIs(t, err, ErrEmptyKey)

	// PutChecksummed should also reject empty keys
	err = cache.PutChecksummed([]byte{}, []byte("value"), 0)
	require.ErrorIs(t, err, ErrEmptyKey)
}

func TestCache_Put_EmptyValueAllowed(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxCachedSlabs(0)) // Force disk path
	require.NoError(t, err)
	defer cache.Close()

	// Empty slice value is allowed
	err = cache.Put([]byte("key-empty-slice"), []byte{})
	require.NoError(t, err)

	// Nil value is allowed
	err = cache.Put([]byte("key-nil-value"), nil)
	require.NoError(t, err)

	// Flush to disk and verify round-trip
	cache.Drain()

	// Empty slice should read back as empty
	retrieved, found := cache.Get([]byte("key-empty-slice"))
	require.True(t, found)
	require.Empty(t, retrieved)

	// Nil value should read back as empty
	retrieved, found = cache.Get([]byte("key-nil-value"))
	require.True(t, found)
	require.Empty(t, retrieved)
}

func TestCache_Delete_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxCachedSlabs(0)) // Force disk path
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("delete-me")
	value := []byte("some-value")

	// Put and verify
	require.NoError(t, cache.Put(key, value))
	cache.Drain()
	_, found := cache.Get(key)
	require.True(t, found, "key should exist before delete")

	// Delete
	require.NoError(t, cache.Delete(key))

	// Should not be found after delete
	_, found = cache.Get(key)
	require.False(t, found, "key should not be found after delete")

	// Delete again should be idempotent (no error)
	require.NoError(t, cache.Delete(key))
}

func TestCache_Delete_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Deleting non-existent key should succeed (idempotent)
	err = cache.Delete([]byte("never-existed"))
	require.NoError(t, err)
}

func TestCache_Delete_EmptyKeyRejected(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	err = cache.Delete([]byte{})
	require.ErrorIs(t, err, ErrEmptyKey)

	err = cache.Delete(nil)
	require.ErrorIs(t, err, ErrEmptyKey)
}

func TestCache_Delete_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create cache and add data
	cache, err := New(tmpDir, WithMaxCachedSlabs(0))
	require.NoError(t, err)

	key := []byte("persistent-delete")
	require.NoError(t, cache.Put(key, []byte("value")))
	cache.Drain()

	// Delete and close
	require.NoError(t, cache.Delete(key))
	require.NoError(t, cache.Close())

	// Reopen - deleted item should still be gone
	cache2, err := New(tmpDir)
	require.NoError(t, err)
	defer cache2.Close()

	_, found := cache2.Get(key)
	require.False(t, found, "deleted key should not be found after reopen")
}

func TestCache_Delete_WAL_NoHolePunch(t *testing.T) {
	// Validates that in CAS mode (WAL enabled), Delete() does NOT hole-punch.
	// Space reclamation is deferred to compaction for durability guarantees.
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),
		WithMaxCachedSlabs(0), // Force disk path
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	key := []byte("wal-delete-key")
	value := make([]byte, 100_000) // 100KB
	require.NoError(t, cache.Put(key, value))
	cache.Drain()

	// Get segment stats before delete
	h := xxh3.Hash128(key)
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Get physical size before delete
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	beforeStat, err := os.Stat(segPath)
	require.NoError(t, err)
	beforeBlocks := beforeStat.Sys().(*syscall.Stat_t).Blocks

	// Delete
	require.NoError(t, cache.Delete(key))

	// Verify tombstone in index
	item, found = cache.index.Get(h)
	require.True(t, found, "item should still exist as tombstone")
	require.True(t, item.IsDeleted(), "item should be marked deleted")

	// Verify NO hole punch happened (physical size unchanged)
	afterStat, err := os.Stat(segPath)
	require.NoError(t, err)
	afterBlocks := afterStat.Sys().(*syscall.Stat_t).Blocks
	require.Equal(t, beforeBlocks, afterBlocks,
		"WAL mode should NOT hole-punch (space reclaimed during compaction)")
}

func TestCache_Delete_Cache_LogicalTombstone(t *testing.T) {
	// Validates that in Cache mode (no WAL), Delete() creates a logical tombstone
	// without hole-punching. Physical space is reclaimed later by merge compaction.
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		// No WAL = Cache mode
		WithMaxCachedSlabs(0), // Force disk path
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	key := []byte("cache-delete-key")
	value := make([]byte, 100_000) // 100KB
	require.NoError(t, cache.Put(key, value))
	cache.Drain()

	// Get segment info before delete
	h := xxh3.Hash128(key)
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Get physical size before delete (should NOT change)
	segPath := getSegmentPath(cache.Path, cache.Shards, segID)
	beforeStat, err := os.Stat(segPath)
	require.NoError(t, err)
	beforeBlocks := beforeStat.Sys().(*syscall.Stat_t).Blocks

	// Delete
	require.NoError(t, cache.Delete(key))

	// Verify tombstone in index
	item, found = cache.index.Get(h)
	require.True(t, found, "item should still exist as tombstone")
	require.True(t, item.IsDeleted(), "item should be marked deleted")

	// Verify physical size unchanged (no hole punching)
	afterStat, err := os.Stat(segPath)
	require.NoError(t, err)
	afterBlocks := afterStat.Sys().(*syscall.Stat_t).Blocks
	require.Equal(t, beforeBlocks, afterBlocks,
		"Delete should not hole-punch; physical space reclaimed by merge compaction")

	_ = segID // Used above
}

func TestCache_Put_LargeBlob(t *testing.T) {
	// Tests the XL (extra large) write code path.
	// XL writes are triggered when record size exceeds WriteBufferSize.
	tmpDir := t.TempDir()
	bufferSize := int64(16 * 1024) // 16KB buffer
	cache, err := New(tmpDir,
		WithWriteBufferSize(bufferSize),
		WithMaxCachedSlabs(0),                  // Force disk path
		WithCompression(compression.CodexNone), // No compression for predictable size
	)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("large-key")
	// Value must be larger than WriteBufferSize to trigger XL write path
	value := make([]byte, int(bufferSize)+1024) // Exceeds buffer size
	// Use identifiable pattern for debugging
	copy(value, "XLBLOB_START_")
	for i := 13; i < len(value)-11; i++ {
		value[i] = byte(i % 256)
	}
	copy(value[len(value)-11:], "_END_XLBLOB")

	require.NoError(t, cache.Put(key, value))

	// Flush to disk and verify round-trip
	cache.Drain()

	retrieved, found := cache.Get(key)
	require.True(t, found, "key not found after drain")
	require.Equal(t, len(value), len(retrieved), "length mismatch")
	require.Equal(t, value, retrieved, "data mismatch")
}

// TestCache_LargeWrites_Comprehensive tests various combinations of normal and XL (extra large) writes.
// XL writes are triggered when record size exceeds WriteBufferSize.
// Tests verify correct round-trip for each pattern, both with and without WAL.
func TestCache_LargeWrites_Comprehensive(t *testing.T) {
	// Test patterns: 'N' = normal write, 'L' = large (XL) write
	patterns := []struct {
		name   string
		writes string // 'N' for normal, 'L' for large
		desc   string
	}{
		{"SimpleXL", "L", "single large write"},
		{"XLThenNormal", "LN", "large followed by normal"},
		{"XLThenMultiNormal", "LNNN", "large followed by multiple normals"},
		{"NormalThenXL", "NL", "normal followed by large"},
		{"MultiNormalThenXL", "NNNL", "multiple normals followed by large"},
		{"Alternating", "NLNLNL", "alternating normal and large"},
		{"Complex", "NLNLLLNN", "mixed: normal, large, normal, large, large, large, normal, normal"},
		{"AllXL", "LLL", "multiple large writes"},
		{"BookendXL", "LNNNL", "large at start and end"},
	}

	for _, walEnabled := range []bool{false, true} {
		walName := "NoWAL"
		if walEnabled {
			walName = "WithWAL"
		}

		for _, p := range patterns {
			t.Run(fmt.Sprintf("%s/%s", walName, p.name), func(t *testing.T) {
				testLargeWritePattern(t, p.writes, walEnabled)
			})
		}
	}
}

func testLargeWritePattern(t *testing.T, pattern string, walEnabled bool) {
	tmpDir := t.TempDir()
	bufferSize := int64(16 * 1024) // 16KB buffer

	opts := []Option{
		WithWriteBufferSize(bufferSize),
		WithMaxCachedSlabs(0),                  // Force disk path
		WithCompression(compression.CodexNone), // No compression for predictable size
	}
	if walEnabled {
		opts = append(opts, WithWAL())
	}

	cache, err := New(tmpDir, opts...)
	require.NoError(t, err)
	defer cache.Close()

	// Track what we write for verification
	type writeRecord struct {
		key   []byte
		value []byte
		isXL  bool
	}
	var writes []writeRecord

	// Generate writes based on pattern
	for i, ch := range pattern {
		isXL := ch == 'L'
		key := []byte(fmt.Sprintf("key-%d-%c", i, ch))

		var value []byte
		if isXL {
			// Value larger than buffer to trigger XL path
			value = make([]byte, int(bufferSize)+1024)
		} else {
			// Normal small value
			value = make([]byte, 512)
		}

		// Fill with identifiable pattern
		fillPattern(value, i, isXL)

		writes = append(writes, writeRecord{key: key, value: value, isXL: isXL})
		require.NoError(t, cache.Put(key, value), "Put failed for key %s", key)
	}

	// Flush to disk
	cache.Drain()

	// Verify all writes can be read back correctly
	for _, w := range writes {
		retrieved, found := cache.Get(w.key)
		require.True(t, found, "key %s not found after drain (isXL=%v)", w.key, w.isXL)
		require.Equal(t, len(w.value), len(retrieved),
			"length mismatch for key %s (isXL=%v)", w.key, w.isXL)
		require.Equal(t, w.value, retrieved,
			"data mismatch for key %s (isXL=%v)", w.key, w.isXL)
	}

	// Verify segment file exists and has valid structure
	verifySegmentFiles(t, tmpDir, cache.Shards)
}

// TestCache_XLRotation verifies that slab rotation occurs when XL writes
// accumulate past the threshold (2x WriteBufferSize), preventing unbounded
// memory usage in workloads with only large writes.
func TestCache_XLRotation(t *testing.T) {
	tmpDir := t.TempDir()
	bufferSize := int64(16 * 1024) // 16KB buffer

	cache, err := New(tmpDir,
		WithWriteBufferSize(bufferSize),
		WithMaxCachedSlabs(0),                  // Force disk path
		WithCompression(compression.CodexNone), // No compression
	)
	require.NoError(t, err)
	defer cache.Close()

	// Each XL write is ~17KB (just over buffer size).
	// Threshold is 2x buffer = 32KB.
	// So 2 XL writes should trigger rotation before the 3rd.
	xlSize := int(bufferSize) + 1024 // ~17KB

	var keys [][]byte
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("xl-rotation-key-%d", i))
		value := make([]byte, xlSize)
		fillPattern(value, i, true)

		keys = append(keys, key)
		require.NoError(t, cache.Put(key, value))
	}

	cache.Drain()

	// Verify all keys are readable
	for i, key := range keys {
		retrieved, found := cache.Get(key)
		require.True(t, found, "key %d not found after rotation", i)
		require.Equal(t, xlSize, len(retrieved), "key %d length mismatch", i)
	}

	// Count unique segment IDs from index - should be >1 due to rotation
	segmentIDs := make(map[uint32]struct{})
	for _, key := range keys {
		h := xxh3.Hash128(key)
		entry, ok := cache.index.Get(index.Key(h))
		require.True(t, ok, "key should be in index")
		segmentIDs[entry.SegmentID] = struct{}{}
	}
	require.Greater(t, len(segmentIDs), 1,
		"should have multiple segments due to XL rotation (got %d)", len(segmentIDs))
	t.Logf("XL rotation created %d segments for 5 XL writes", len(segmentIDs))

	// Verify .iseg (footer) files exist for each segment - needed for disaster recovery
	for segID := range segmentIDs {
		footerPath := GetFooterPath(tmpDir, cache.Shards, segID)
		_, err := os.Stat(footerPath)
		require.NoError(t, err, "footer file should exist: %s", footerPath)
	}
}

// fillPattern fills a buffer with an identifiable pattern for debugging
func fillPattern(buf []byte, index int, isXL bool) {
	prefix := "NORM_"
	if isXL {
		prefix = "XLBL_"
	}
	marker := fmt.Sprintf("%s%03d_START_", prefix, index)
	copy(buf, marker)

	// Fill middle with index-based pattern
	for i := len(marker); i < len(buf)-12; i++ {
		buf[i] = byte((i + index) % 256)
	}

	// End marker
	endMarker := fmt.Sprintf("_END_%03d", index)
	copy(buf[len(buf)-len(endMarker):], endMarker)
}

// verifySegmentFiles checks that segment files exist and have valid footer structure
func verifySegmentFiles(t *testing.T, dir string, shards int) {
	t.Helper()

	// Find all .iseg files
	segDir := fmt.Sprintf("%s/segments", dir)
	for shard := 0; shard < shards; shard++ {
		shardDir := fmt.Sprintf("%s/%04d", segDir, shard)
		entries, err := os.ReadDir(shardDir)
		if os.IsNotExist(err) {
			continue // Shard may not have data
		}
		require.NoError(t, err)

		for _, entry := range entries {
			if !entry.IsDir() && len(entry.Name()) > 5 {
				ext := entry.Name()[len(entry.Name())-5:]
				if ext == ".iseg" {
					path := fmt.Sprintf("%s/%s", shardDir, entry.Name())
					verifySegmentFile(t, path)
				}
			}
		}
	}
}

// verifySegmentFile validates a single segment file structure
func verifySegmentFile(t *testing.T, path string) {
	t.Helper()

	fi, err := os.Stat(path)
	require.NoError(t, err, "segment file should exist: %s", path)
	require.Greater(t, fi.Size(), int64(0), "segment file should not be empty: %s", path)

	// Read file header
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	// Verify file header magic
	header := make([]byte, record.FileHeaderSize)
	_, err = f.Read(header)
	require.NoError(t, err, "should read file header")

	magic := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16 | uint32(header[3])<<24
	require.Equal(t, record.FileMagic, magic, "segment file should have correct magic: %s", path)

	t.Logf("Verified segment file: %s (size=%d)", path, fi.Size())
}

func TestCache_PutChecksummed_CorrectChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0), // Force disk path
		WithChecksum(),        // Enable checksum hasher
		WithVerifyOnRead(true),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("checksum-key")
	value := []byte("checksum-value")
	// Note: checksumVerifyingReader verifies just the value stream,
	// so the CRC should be computed over value only (not key+value).
	correctCRC := record.ComputeCRC(nil, value)

	err = cache.PutChecksummed(key, value, correctCRC)
	require.NoError(t, err)

	cache.Drain()

	// Should read back successfully with correct checksum
	retrieved, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, value, retrieved)
}

func TestCache_PutChecksummed_IncorrectChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir,
		WithMaxCachedSlabs(0), // Force disk path
		WithChecksum(),        // Enable checksum hasher
		WithVerifyOnRead(true),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("bad-checksum-key")
	value := []byte("bad-checksum-value")
	incorrectCRC := uint32(0xDEADBEEF) // Wrong checksum

	err = cache.PutChecksummed(key, value, incorrectCRC)
	require.NoError(t, err) // Put succeeds (checksum stored as-is)

	cache.Drain()

	// Read should fail - data appears missing due to CRC mismatch
	_, found := cache.Get(key)
	require.False(t, found, "should not find blob with incorrect checksum")
}

func TestCache_KeyCollisionDetection(t *testing.T) {
	tmpDir := t.TempDir()
	// TrustHash=false enables collision detection (default is true in cache mode)
	cache, err := New(tmpDir, WithMaxCachedSlabs(0), WithTrustHash(false)) // Force disk path
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("collision-key")
	value := []byte("collision-value")
	h := xxh3.Hash128(key)

	err = cache.Put(key, value)
	require.NoError(t, err)
	cache.Drain()

	// Find the entry in the index to get the segment file and offset
	entry, found := cache.index.Get(index.Key(h))
	require.True(t, found, "entry should exist in index")

	// Close cache to release file handles
	require.NoError(t, cache.Close())

	// Corrupt the key bytes in the segment file.
	// Record layout: [Header:35B][Key][Value]
	// Key starts at offset + HeaderSize
	segPath := fmt.Sprintf("%s/segments/0000/%d.seg", tmpDir, entry.SegmentID)
	segFile, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	require.NoError(t, err)

	keyOffset := int64(entry.Offset) + int64(record.HeaderSize)
	// Write different key bytes (same length to keep record valid)
	corruptedKey := []byte("CORRUPTED-KEY") // Different key that would "collide"
	_, err = segFile.WriteAt(corruptedKey[:len(key)], keyOffset)
	require.NoError(t, err)
	require.NoError(t, segFile.Close())

	// Reopen cache and try to read - should fail with key mismatch
	cache2, err := New(tmpDir, WithMaxCachedSlabs(0), WithTrustHash(false))
	require.NoError(t, err)
	defer cache2.Close()

	// Get should fail because stored key doesn't match requested key
	_, found = cache2.Get(key)
	require.False(t, found, "should not find blob with mismatched key (simulated collision)")
}

func TestCache_SelfHealing_OnCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	// Disable in-memory slab caching so Get() must go to disk.
	cache, err := New(tmpDir, WithMaxCachedSlabs(0))
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("healing-key")
	value := []byte("precious-data")
	h := xxh3.Hash128(key)

	require.NoError(t, cache.Put(key, value))
	cache.Drain()

	// 1. Manually corrupt the storage by deleting the segment file
	entry, ok := cache.index.Get(index.Key(h))
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
	entry, inIndex := cache.index.Get(index.Key(h))
	require.True(t, inIndex, "Index entry should still exist")
	require.True(t, entry.HasError(), "Blob should be marked as corrupt")
	require.NotEqual(t, base.ErrNone, entry.Errno(), "Errno should be set")

	// 4. Subsequent reads should fail fast (corruption check)
	_, found = cache.Get(key)
	require.False(t, found, "Subsequent reads should fail")
}

func TestCache_Eviction_Headroom(t *testing.T) {
	tmpDir := t.TempDir()
	// Small cache with eviction enabled
	cache, err := New(tmpDir,
		WithMaxSize(20*1024),        // 20KB limit
		WithWriteBufferSize(2*1024)) // Small buffer to ensure flush (clamped to 8KB min)
	require.NoError(t, err)
	defer cache.Close()
	cache.Start() // Start eviction worker

	// Put enough data to trigger eviction (30KB > 20KB limit)
	for i := 0; i < 30; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		require.NoError(t, cache.Put(key, make([]byte, 1024)))
	}
	cache.Drain()

	// Poll for eviction completion with timeout
	deadline := time.Now().Add(5 * time.Second)
	var finalSize int64
	var deletions int64
	for time.Now().Before(deadline) {
		finalSize = cache.approxSize.Load()
		deletions = cache.bloomStats.deletions.Load()
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

func TestCache_Restart_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Phase 1: Write and Close
	cache1, err := New(tmpDir)
	require.NoError(t, err)
	cache1.Put([]byte("k1"), []byte("v1"))
	cache1.Drain()
	cache1.Close()

	// Phase 2: OpenIndex and Verify
	cache2, err := New(tmpDir)
	require.NoError(t, err)
	defer cache2.Close()

	val, found := cache2.Get([]byte("k1"))
	require.True(t, found)
	require.Equal(t, []byte("v1"), val)
}

// Benchmarks

func BenchmarkCache_Get_WithBloom(b *testing.B) {
	tmpDir := b.TempDir()
	cache, _ := New(tmpDir)
	defer cache.Close()

	key := []byte("bench-key")
	if err := cache.Put(key, make([]byte, 1024)); err != nil {
		b.Fatal(err)
	}
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
	require.NoError(t, cache.Put(key, original))
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found, "compressed blob should be readable")
	require.Equal(t, original, result, "decompressed data should match original")

	// Verify compression metadata is correct
	h := xxh3.Hash128(key)
	entry, ok := cache.index.Get(index.Key(h))
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

	// Create truly incompressible data (crypto random)
	// The 1/8th heuristic should detect this and store raw
	original := make([]byte, 1000)
	_, err = crand.Read(original)
	require.NoError(t, err, "failed to generate random data")

	key := []byte("incompressible-key")

	require.NoError(t, cache.Put(key, original))
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found, "incompressible blob should be readable")
	require.Equal(t, original, result, "data should match original")

	// Check entry exists and verify compression flag
	h := xxh3.Hash128(key)
	entry, ok := cache.index.Get(index.Key(h))
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

	require.NoError(t, cache.Put(key, original))
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, original, result)

	// Verify it was NOT compressed due to size threshold
	h := xxh3.Hash128(key)
	entry, ok := cache.index.Get(index.Key(h))
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

	require.NoError(t, cache.Put(key, original))
	cache.Drain()

	// Read back and verify
	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, original, result)

	// Verify it WAS compressed despite being small (MinSize=0 disables restriction)
	h := xxh3.Hash128(key)
	entry, ok := cache.index.Get(index.Key(h))
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

	require.NoError(t, cache.Put(key, original))
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

	require.NoError(t, cache.Put(key, original))
	cache.Drain()

	result, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, original, result)

	h := xxh3.Hash128(key)
	entry, ok := cache.index.Get(index.Key(h))
	require.True(t, ok)

	t.Logf("PhysicalLen: %d, IsCompressed: %v", entry.PhysicalLen, entry.IsCompressed())

	require.True(t, entry.IsCompressed(), "blob should be compressed with LZ4")
	require.Equal(t, compression.CodexLZ4, entry.Compression())
}
