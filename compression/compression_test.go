package compression

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressionRoundtrip(t *testing.T) {
	codecs := []struct {
		name  string
		codec Codex
	}{
		{"zstd", CodexZstd},
		{"lz4", CodexLZ4},
		{"s2", CodexS2},
	}

	levels := []struct {
		name  string
		level Level
	}{
		{"default", CompressionDefault},
		{"speed", CompressionSpeed},
		{"best", CompressionBest},
	}

	testData := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello world")},
		{"compressible", bytes.Repeat([]byte("abcdefghij"), 1000)},
		{"binary_zeros", make([]byte, 4096)},
		{"random", randomBytes(t, 8192)},
	}

	for _, codec := range codecs {
		for _, level := range levels {
			for _, td := range testData {
				name := codec.name + "/" + level.name + "/" + td.name
				t.Run(name, func(t *testing.T) {
					// Allocate generous dst buffer for compression
					dst := make([]byte, 0, len(td.data)*2+1024)
					compressed, err := Compress(codec.codec, level.level, dst, td.data)
					require.NoError(t, err, "compression failed")
					require.NotNil(t, compressed)

					// Decompress back
					decompressed := make([]byte, len(td.data))
					err = Decompress(codec.codec, decompressed, compressed)
					require.NoError(t, err, "decompression failed")
					require.Equal(t, td.data, decompressed, "roundtrip mismatch")
				})
			}
		}
	}
}

func TestCompressionRatio(t *testing.T) {
	// Highly compressible data should compress well
	data := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 100)

	codecs := []Codex{CodexZstd, CodexLZ4, CodexS2}
	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			// Use generous buffer - compressed size can exceed original for headers
			dst := make([]byte, 0, len(data)*2)
			compressed, err := Compress(codec, CompressionDefault, dst, data)
			require.NoError(t, err)

			ratio := float64(len(compressed)) / float64(len(data))
			t.Logf("%s: %d -> %d bytes (%.1f%%)", codec, len(data), len(compressed), ratio*100)
			require.Less(t, ratio, 0.5, "expected at least 50%% compression")
		})
	}
}

func TestBufferTooSmall(t *testing.T) {
	data := bytes.Repeat([]byte("compressible data "), 100)

	codecs := []Codex{CodexZstd, CodexLZ4, CodexS2}
	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			// Provide a tiny buffer that cannot hold the output
			dst := make([]byte, 0, 10)
			_, err := Compress(codec, CompressionDefault, dst, data)
			require.ErrorIs(t, err, ErrBufferTooSmall)
			require.True(t, IsBufferTooSmall(err))
		})
	}
}

func TestDecompressSizeMismatch(t *testing.T) {
	data := bytes.Repeat([]byte("test data "), 100)

	// Test codecs that error on size mismatch
	// zstd and lz4 return errors; s2 has append semantics (doesn't check dst size)
	t.Run("zstd", func(t *testing.T) {
		dst := make([]byte, 0, len(data)*2)
		compressed, err := Compress(CodexZstd, CompressionDefault, dst, data)
		require.NoError(t, err)

		// Non-empty but too small buffer
		smallBuf := make([]byte, 10)
		err = Decompress(CodexZstd, smallBuf, compressed)
		require.Error(t, err, "zstd should error when dst is too small")
	})

	t.Run("lz4", func(t *testing.T) {
		dst := make([]byte, 0, len(data)*2)
		compressed, err := Compress(CodexLZ4, CompressionDefault, dst, data)
		require.NoError(t, err)

		// Non-empty but too small buffer
		smallBuf := make([]byte, 10)
		err = Decompress(CodexLZ4, smallBuf, compressed)
		require.Error(t, err, "lz4 should error when dst is too small")
	})

	t.Run("s2", func(t *testing.T) {
		dst := make([]byte, 0, len(data)*2)
		compressed, err := Compress(CodexS2, CompressionDefault, dst, data)
		require.NoError(t, err)

		// Non-empty but too small buffer
		smallBuf := make([]byte, 10)
		err = Decompress(CodexS2, smallBuf, compressed)
		require.ErrorIs(t, err, ErrBufferTooSmall, "s2 should error when dst is too small")
	})
}

func TestCodexString(t *testing.T) {
	tests := []struct {
		codec    Codex
		expected string
	}{
		{CodexNone, "none"},
		{CodexZstd, "zstd"},
		{CodexLZ4, "lz4"},
		{CodexS2, "s2"},
		{Codex(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.codec.String())
		})
	}
}

func TestUnsupportedCodec(t *testing.T) {
	data := []byte("test")
	dst := make([]byte, 100)

	_, err := Compress(Codex(99), CompressionDefault, dst, data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported codec")
}

func TestDecompressNoneCodec(t *testing.T) {
	// CodexNone should be a no-op for Decompress
	data := []byte("unchanged")
	dst := make([]byte, len(data))
	copy(dst, data)

	err := Decompress(CodexNone, dst, data)
	require.NoError(t, err)
}

// BenchmarkZstdCtxPool exercises the zstd context pool under parallel load.
// This tests correctness of pool usage, not compression performance.
func BenchmarkZstdCtxPool(b *testing.B) {
	data := bytes.Repeat([]byte("pool test data "), 100)
	compressed := make([]byte, len(data)*2)

	// Pre-compress for decompression tests
	preCompressed, err := Compress(CodexZstd, CompressionDefault, compressed, data)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("compress", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			dst := make([]byte, 0, len(data)*2)
			for pb.Next() {
				result, err := Compress(CodexZstd, CompressionDefault, dst, data)
				if err != nil {
					b.Fatal(err)
				}
				if len(result) == 0 {
					b.Fatal("unexpected empty result")
				}
			}
		})
	})

	b.Run("decompress", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			dst := make([]byte, len(data))
			for pb.Next() {
				if err := Decompress(CodexZstd, dst, preCompressed); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("roundtrip", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			compDst := make([]byte, 0, len(data)*2)
			decompDst := make([]byte, len(data))
			for pb.Next() {
				compressed, err := Compress(CodexZstd, CompressionDefault, compDst, data)
				if err != nil {
					b.Fatal(err)
				}
				if err := Decompress(CodexZstd, decompDst, compressed); err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(data, decompDst) {
					b.Fatal("roundtrip mismatch")
				}
			}
		})
	})
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}
