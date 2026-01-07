package blobcache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_CorruptIndex(t *testing.T) {
	// Create temporary cache directory
	tmpDir := t.TempDir()

	// Create cache and write some data
	cache, err := New(tmpDir,
		WithTestingFlushOnPut(),
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
		cache.Put([]byte(key), value)
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
		WithTestingFlushOnPut(),
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
		WithTestingFlushOnPut(),
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
		cache.Put([]byte(key), value)
	}
	cache.Drain()

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
		WithTestingFlushOnPut(),
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
		WithTestingFlushOnPut(),
	)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	cache.Put([]byte("key1"), []byte("value1"))
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
		WithTestingFlushOnPut(),
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
