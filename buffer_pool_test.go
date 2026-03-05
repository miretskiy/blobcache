package blobcache

import (
	"testing"

	"github.com/miretskiy/dio/align"
	"github.com/stretchr/testify/require"
)

func TestAcquireAlignedBuffer_Alignment(t *testing.T) {
	sizes := []int{1, 100, 4096, 4097, 8000, 65536, 1 << 20}
	for _, size := range sizes {
		h := AcquireAlignedBuffer(size, size)
		buf := h.Bytes()
		require.Equal(t, size, len(buf), "length mismatch for size %d", size)
		require.True(t, align.IsAligned(buf), "AcquireAlignedBuffer(%d) not aligned", size)
		h.Release()
	}
}

func TestAcquireAlignedBuffer_ReuseAfterRelease(t *testing.T) {
	// Acquire, write pattern, release, re-acquire — verify alignment still holds.
	for i := 0; i < 100; i++ {
		h := AcquireAlignedBuffer(8192, 8192)
		buf := h.Bytes()
		require.True(t, align.IsAligned(buf), "iteration %d: not aligned", i)
		// Write a pattern to exercise the buffer.
		for j := range buf {
			buf[j] = byte(j)
		}
		h.Release()
	}
}

func TestAcquireAlignedBuffer_DataIntegrity(t *testing.T) {
	// Write data into aligned buffer, verify it reads back correctly.
	h := AcquireAlignedBuffer(4096, 4096)
	buf := h.Bytes()
	for i := range buf {
		buf[i] = byte(i % 251) // prime modulus to catch off-by-one
	}
	for i := range buf {
		require.Equal(t, byte(i%251), buf[i], "byte %d mismatch", i)
	}
	h.Release()
}

func TestAcquireAlignedBuffer_JumboSize(t *testing.T) {
	// Test with a size that hits the jumbo pool bucket.
	size := 3 * 1024 * 1024 // 3MB — above maxShift (2MB) but below maxPoolSize (16MB)
	h := AcquireAlignedBuffer(size, size)
	buf := h.Bytes()
	require.Equal(t, size, len(buf))
	require.True(t, align.IsAligned(buf), "jumbo aligned buffer not aligned")
	h.Release()
}
