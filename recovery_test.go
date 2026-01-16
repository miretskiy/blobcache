package blobcache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/miretskiy/blobcache/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestRecovery_CorruptIndex(t *testing.T) {
	// Create temporary cache directory
	tmpDir := t.TempDir()

	// Create cache and write some data
	cache, err := New(tmpDir,
		WithSegmentSize(0),
		WithMaxSize(100<<20),
	)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write test data
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for key, value := range testData {
		require.NoError(t, cache.Put([]byte(key), value))
	}
	cache.Drain()

	// Close cache
	if err := cache.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Corrupt the index by removing the db directory
	dbPath := filepath.Join(tmpDir, "db")
	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("failed to remove index: %v", err)
	}

	// Run recovery
	recovered, err := RecoverIndex(tmpDir,
		WithSegmentSize(0),
		WithMaxSize(100<<20),
	)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recovered.Close()

	// Verify all data is still accessible
	for key, expectedValue := range testData {
		actualValue, found := recovered.Get([]byte(key))
		if !found {
			t.Errorf("key %q not found after recovery", key)
			continue
		}

		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("value mismatch for key %q: got %q, want %q", key, actualValue, expectedValue)
		}
	}
}

func TestRecovery_CorruptSegment(t *testing.T) {
	// Create temporary cache directory
	tmpDir := t.TempDir()

	// Create cache and write some data
	cache, err := New(tmpDir,
		WithSegmentSize(0),
		WithMaxSize(100<<20),
	)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write test data - drain after each put to ensure separate segments
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for key, value := range testData {
		require.NoError(t, cache.Put([]byte(key), value))
		cache.Drain() // Force each key into a separate segment
	}

	// Close cache
	if err := cache.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Corrupt one segment file by truncating it
	segmentsDir := filepath.Join(tmpDir, "segments", "0000")
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		t.Fatalf("failed to read segments directory: %v", err)
	}

	// Find the first segment file and truncate it
	for _, entry := range entries {
		if !entry.IsDir() {
			segmentPath := filepath.Join(segmentsDir, entry.Name())

			// Truncate to make it corrupt (remove footer)
			file, err := os.OpenFile(segmentPath, os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("failed to open segment file: %v", err)
			}

			// Truncate to 10 bytes (too small for a valid footer)
			if err := file.Truncate(10); err != nil {
				file.Close()
				t.Fatalf("failed to truncate segment file: %v", err)
			}
			file.Close()

			// Only corrupt one segment
			break
		}
	}

	// Run recovery - should remove the corrupt segment
	recovered, err := RecoverIndex(tmpDir,
		WithSegmentSize(0),
		WithMaxSize(100<<20),
	)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recovered.Close()

	// The corrupt segment should have been removed
	// At least 2 keys should still be accessible (assuming they were in different segments)
	foundCount := 0
	for key := range testData {
		if _, found := recovered.Get([]byte(key)); found {
			foundCount++
		}
	}

	// We should find at least some keys (the ones in non-corrupt segments)
	if foundCount == 0 {
		t.Error("no keys found after recovery, expected at least some valid segments")
	}
}

func TestRecovery_EmptyCache(t *testing.T) {
	// Create temporary cache directory
	tmpDir := t.TempDir()

	// Create empty cache
	cache, err := New(tmpDir)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Run recovery on empty cache
	recovered, err := RecoverIndex(tmpDir)
	if err != nil {
		t.Fatalf("recovery failed on empty cache: %v", err)
	}
	defer recovered.Close()

	// Should work fine with no data
	if _, found := recovered.Get([]byte("nonexistent")); found {
		t.Error("found nonexistent key in empty recovered cache")
	}
}

func TestRecovery_InvalidSegmentID(t *testing.T) {
	// Create temporary cache directory
	tmpDir := t.TempDir()

	// Create cache and write data
	cache, err := New(tmpDir,
		WithSegmentSize(0),
	)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	require.NoError(t, cache.Put([]byte("key1"), []byte("value1")))
	cache.Drain()

	if err := cache.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Create a segment file with invalid name (should be ignored)
	segmentsDir := filepath.Join(tmpDir, "segments", "0000")
	invalidSegment := filepath.Join(segmentsDir, "invalid.seg")
	if err := os.WriteFile(invalidSegment, []byte("garbage"), 0644); err != nil {
		t.Fatalf("failed to create invalid segment: %v", err)
	}

	// Run recovery - should skip invalid segment
	recovered, err := RecoverIndex(tmpDir,
		WithSegmentSize(0),
	)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recovered.Close()

	// Original data should still be accessible
	if _, found := recovered.Get([]byte("key1")); !found {
		t.Error("key1 not found after recovery")
	}
}

// TestWAL_FileLifecycle verifies WAL files are created during Put and
// deleted after segment flush completes.
func TestWAL_FileLifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone), // Fast tests
		WithSegmentSize(0),
	)
	require.NoError(t, err)

	walDir := filepath.Join(tmpDir, "wal")

	// Initially no WAL files
	walFiles, _ := filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.Empty(t, walFiles, "expected no WAL files initially")

	// Write data - creates WAL file
	require.NoError(t, cache.Put([]byte("key1"), []byte("value1")))

	walFiles, _ = filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.Len(t, walFiles, 1, "expected 1 WAL file after Put")

	// Flush and drain - WAL file should be deleted
	cache.Drain()

	walFiles, _ = filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.Empty(t, walFiles, "expected WAL file deleted after flush")

	require.NoError(t, cache.Close())
}

// TestWAL_RecoveryAfterCrash simulates a crash before flush and verifies
// data is recovered from WAL on restart.
func TestWAL_RecoveryAfterCrash(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: write data but don't flush (simulate crash)
	cache, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone),
		WithSegmentSize(0),
	)
	require.NoError(t, err)

	testData := map[string][]byte{
		"key1": []byte("value1-wal-test"),
		"key2": []byte("value2-wal-test"),
		"key3": []byte("value3-wal-test"),
	}

	for key, value := range testData {
		require.NoError(t, cache.Put([]byte(key), value))
	}

	// Close WAL to ensure data is written but DON'T flush to segment
	// This simulates a crash where memtable wasn't flushed
	cache.wal.Close()

	// Verify WAL file exists (data not flushed to segment)
	walDir := filepath.Join(tmpDir, "wal")
	walFiles, _ := filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.NotEmpty(t, walFiles, "expected WAL file to exist")

	// Force close without proper cleanup (simulate crash)
	cache.memTable.Close()
	cache.librarian.Close()
	cache.memTable.ClosePools()
	cache.storage.Close()
	cache.index.Close()

	// Second session: recovery should restore data from WAL
	recovered, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone),
		WithSegmentSize(0),
	)
	require.NoError(t, err)
	defer recovered.Close()

	// Verify all data is recovered
	for key, expectedValue := range testData {
		actualValue, found := recovered.Get([]byte(key))
		require.True(t, found, "key %q not found after recovery", key)
		require.Equal(t, expectedValue, actualValue, "value mismatch for key %q", key)
	}

	// WAL files should be gone after recovery (replayed and flushed)
	recovered.Drain()
	walFiles, _ = filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.Empty(t, walFiles, "expected WAL files cleaned up after recovery")
}

// TestWAL_CommittedFilesCleanedUp verifies that already-committed WAL files
// are deleted during recovery without replaying.
func TestWAL_CommittedFilesCleanedUp(t *testing.T) {
	tmpDir := t.TempDir()

	// Write data and flush to segment (normal operation)
	cache, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone),
		WithSegmentSize(0),
	)
	require.NoError(t, err)

	require.NoError(t, cache.Put([]byte("key1"), []byte("value1")))
	cache.Drain() // Flush to segment, WAL file deleted

	// Create a second batch that will also be flushed
	require.NoError(t, cache.Put([]byte("key2"), []byte("value2")))
	cache.Drain()

	// Close properly
	require.NoError(t, cache.Close())

	// No WAL files should remain
	walDir := filepath.Join(tmpDir, "wal")
	walFiles, _ := filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.Empty(t, walFiles, "expected no WAL files after clean shutdown")

	// Reopen - should work with no WAL files to replay
	recovered, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone),
		WithSegmentSize(0),
	)
	require.NoError(t, err)
	defer recovered.Close()

	// Data should still be accessible from segments
	val, found := recovered.Get([]byte("key1"))
	require.True(t, found)
	require.Equal(t, []byte("value1"), val)

	val, found = recovered.Get([]byte("key2"))
	require.True(t, found)
	require.Equal(t, []byte("value2"), val)
}

// TestWAL_MultipleSlabRecovery tests recovery with multiple WAL files (rotations).
func TestWAL_MultipleSlabRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	// Use small buffer to force rotations
	cache, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone),
		WithWriteBufferSize(4096), // Small buffer to force rotations
		WithSegmentSize(0),
	)
	require.NoError(t, err)

	// Write enough data to trigger multiple slab rotations
	testData := make(map[string][]byte)
	for i := 0; i < 50; i++ {
		key := []byte(bytes.Repeat([]byte("k"), 20+i))
		value := []byte(bytes.Repeat([]byte("v"), 100))
		testData[string(key)] = value
		require.NoError(t, cache.Put(key, value))
	}

	// Close WAL without flushing (simulate crash)
	cache.wal.Close()

	walDir := filepath.Join(tmpDir, "wal")

	// Force close
	cache.memTable.Close()
	cache.librarian.Close()
	cache.memTable.ClosePools()
	cache.storage.Close()
	cache.index.Close()

	// Recover
	recovered, err := New(tmpDir,
		WithWAL(),
		WithWALSyncMode(wal.SyncNone),
		WithWriteBufferSize(4096),
		WithSegmentSize(0),
	)
	require.NoError(t, err)
	defer recovered.Close()

	// Verify all data recovered
	for key, expectedValue := range testData {
		actualValue, found := recovered.Get([]byte(key))
		require.True(t, found, "key not found after recovery")
		require.Equal(t, expectedValue, actualValue)
	}

	// WAL files should be cleaned up
	recovered.Drain()
	walFiles, _ := filepath.Glob(filepath.Join(walDir, "wal-*.log"))
	require.Empty(t, walFiles, "expected WAL files cleaned up after recovery")
}
