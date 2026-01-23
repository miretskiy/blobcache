package blobcache

import (
	"bytes"
	"testing"

	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
)

// TestCompression_SizeTracking tests that LogicalSize is correctly preserved
// through the compression -> record creation -> encoding -> decoding -> decompression path.
// This test isolates the size tracking logic from the full BlobCache machinery.
func TestCompression_SizeTracking(t *testing.T) {
	original := bytes.Repeat([]byte("COMPRESS_ME_"), 1000) // ~12KB
	key := []byte("test-key")
	originalLen := len(original)

	// Phase 1: Compress (simulating maybeCompress)
	codec := compression.CodexZstd
	level := compression.CompressionDefault

	maxDst := len(original) - len(original)>>3
	handle := AcquireBuffer(0, maxDst)
	defer handle.Release()

	compressed, err := compression.Compress(codec, level, handle.Bytes(), original)
	require.NoError(t, err, "compression should succeed")
	t.Logf("Original: %d bytes -> Compressed: %d bytes", originalLen, len(compressed))

	// Phase 2: Create record (simulating putActiveCompressed)
	//   - valueBytes = compressed bytes
	//   - logicalSize = original length
	rec := record.NewRecord(1, key, compressed, int64(originalLen))
	rec.SetCompression(codec)

	t.Logf("Record: PhysicalSize=%d, LogicalSize=%d", rec.PhysicalSize, rec.LogicalSize)
	require.Equal(t, int64(len(compressed)), rec.PhysicalSize, "PhysicalSize should be compressed size")
	require.Equal(t, int64(originalLen), rec.LogicalSize, "LogicalSize should be original size")

	// Phase 3: Encode record to bytes (simulating record write to disk)
	buf := make([]byte, rec.EncodedSize())
	rec.EncodeTo(buf)

	// Phase 4: Decode header from bytes (simulating read from disk)
	hdr, err := record.DecodeHeader(buf)
	require.NoError(t, err, "header decode should succeed")

	t.Logf("Decoded Header: PhysicalSize=%d, LogicalSize=%d", hdr.PhysicalSize, hdr.LogicalSize)
	require.Equal(t, int64(len(compressed)), hdr.PhysicalSize, "Decoded PhysicalSize should match")
	require.Equal(t, int64(originalLen), hdr.LogicalSize, "Decoded LogicalSize should match original")

	// Phase 5: Allocate decompression buffer using LogicalSize (simulating archivist.go:109)
	dstBuf := make([]byte, hdr.LogicalSize)
	t.Logf("Decompression buffer size: %d (should be %d)", len(dstBuf), originalLen)

	// Phase 6: Extract compressed value from record
	keyEnd := record.HeaderSize + int(hdr.KeyLen)
	valueData := buf[keyEnd:]
	require.Equal(t, len(compressed), len(valueData), "Value data should be compressed size")

	// Phase 7: Decompress into exact-sized buffer (simulating archivist decompression)
	err = compression.Decompress(hdr.Compression(), dstBuf, valueData)
	require.NoError(t, err, "decompression with exact LogicalSize buffer should succeed")

	// Phase 8: Verify round-trip
	require.Equal(t, original, dstBuf, "decompressed data should match original")
}

// TestCompression_SizeTracking_Repeated runs the test 100 times to catch flakiness
func TestCompression_SizeTracking_Repeated(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run("", func(t *testing.T) {
			TestCompression_SizeTracking(t)
		})
	}
	t.Logf("Successfully completed 100 iterations")
}
