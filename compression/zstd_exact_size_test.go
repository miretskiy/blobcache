package compression

import (
	"bytes"
	"errors"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/stretchr/testify/require"
)

var ErrSizeMismatch = errors.New("zstd: decompressed size mismatch")

// TestZstd_ExactSizeDecompression tests that zstd decompression works
// with an exact-sized destination buffer (no +1 byte workaround).
// This test isolates zstd behavior from BlobCache's size tracking logic.
func TestZstd_ExactSizeDecompression(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "highly compressible",
			data: bytes.Repeat([]byte("COMPRESS_ME_"), 1000), // ~12KB repeated text
		},
		{
			name: "small compressible",
			data: bytes.Repeat([]byte("x"), 100), // 100 bytes
		},
		{
			name: "medium compressible",
			data: bytes.Repeat([]byte("TEST_DATA_"), 500), // ~5KB
		},
		{
			name: "large compressible",
			data: bytes.Repeat([]byte("BIG_BLOB_"), 10000), // ~90KB
		},
		{
			name: "single byte",
			data: []byte("x"),
		},
		{
			name: "empty",
			data: []byte{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.data
			originalLen := len(original)

			// Phase 1: Compress
			// Allocate destination buffer with CompressBound size
			dstBound := zstd.CompressBound(len(original))
			compressBuf := make([]byte, 0, dstBound)
			compressed, err := compressZstd(compressBuf, original, CompressionDefault)
			require.NoError(t, err, "compression should succeed")
			t.Logf("Original: %d bytes -> Compressed: %d bytes (ratio: %.2f)",
				originalLen, len(compressed), float64(len(compressed))/float64(max(1, originalLen)))

			// Phase 2: Decompress into EXACT-sized buffer (what BlobCache does)
			// This is the critical test: dst buffer is exactly originalLen bytes
			dst := make([]byte, originalLen)

			// Call decompressZstd directly (bypassing the +1 workaround)
			err = decompressZstdExact(dst, compressed)
			require.NoError(t, err, "decompression with exact-sized buffer should succeed")

			// Phase 3: Verify round-trip
			require.Equal(t, original, dst, "decompressed data should match original")
		})
	}
}

// decompressZstdExact is a test-only version that doesn't use the +1 workaround
func decompressZstdExact(dst, src []byte) error {
	if len(dst) == 0 {
		return decompressZstd(dst, src)
	}

	ctx := ctxPool.Get().(zstd.Ctx)
	defer ctxPool.Put(ctx)

	// NO +1 byte workaround - use exact-sized buffer
	n, err := ctx.DecompressInto(dst, src)
	if err != nil {
		return err
	}
	if n != len(dst) {
		return errors.New("zstd decompression: size mismatch")
	}
	return nil
}

// TestZstd_RepeatedDecompression runs the same test 100 times to catch flakiness
func TestZstd_RepeatedDecompression(t *testing.T) {
	original := bytes.Repeat([]byte("COMPRESS_ME_"), 1000)
	originalLen := len(original)

	// Compress once
	dstBound := zstd.CompressBound(len(original))
	compressBuf := make([]byte, 0, dstBound)
	compressed, err := compressZstd(compressBuf, original, CompressionDefault)
	require.NoError(t, err)

	// Decompress 100 times to catch intermittent failures
	for i := 0; i < 100; i++ {
		dst := make([]byte, originalLen)
		err := decompressZstdExact(dst, compressed)
		require.NoError(t, err, "iteration %d: decompression failed", i)
		require.Equal(t, original, dst, "iteration %d: data mismatch", i)
	}
	t.Logf("Successfully decompressed 100 times with exact-sized buffer")
}
