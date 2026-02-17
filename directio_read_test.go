package blobcache

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCache_DirectIORead verifies that the Direct I/O read path correctly
// handles records at various unaligned offsets within segments.
// Records are written with deliberately awkward sizes so they land at
// non-page-aligned positions, exercising the AlignRange → shift → extract logic.
func TestCache_DirectIORead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "directio-read-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir, WithDirectIORead(true))
	require.NoError(t, err)
	defer cache.Close()

	// Awkward sizes chosen to produce unaligned record offsets within a segment:
	// - 42-byte header + key + value means each record's start depends on all
	//   previous records' sizes, creating a mix of aligned and unaligned offsets.
	// - Sizes that are NOT multiples of 4096 ensure misalignment.
	sizes := []int{
		1,       // Tiny: header dominates, next record starts mid-page
		4095,    // One byte short of a page
		4096,    // Exactly one page (but header pushes it past alignment)
		4097,    // One byte over a page
		100,     // Small
		7777,    // Odd size
		1 << 20, // 1MB — large record
		999,     // Another odd size
		12345,   // Non-power-of-two
		8191,    // 2 pages minus one byte
	}

	type entry struct {
		key   []byte
		value []byte
	}
	entries := make([]entry, len(sizes))

	for i, size := range sizes {
		key := fmt.Appendf(nil, "dio-key-%d", i)
		value := make([]byte, size)
		for j := range value {
			value[j] = byte((i*37 + j) % 256)
		}
		entries[i] = entry{key: key, value: value}
		require.NoError(t, cache.Put(key, value))
	}

	// Flush to disk so reads go through archivist (Direct I/O path).
	cache.Drain()

	// Verify every key reads back correctly.
	for i, e := range entries {
		value, found := cache.Get(e.key)
		require.True(t, found, "key %d (%q) not found", i, e.key)
		require.Equal(t, len(e.value), len(value), "key %d size mismatch", i)
		require.Equal(t, e.value, value, "key %d data mismatch", i)
	}
}

// TestCache_DirectIORead_WithCompression verifies Direct I/O reads work
// correctly with compressed records (aligned read buffer freed after decompression).
func TestCache_DirectIORead_WithCompression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "directio-read-compress-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithDirectIORead(true),
		WithCompression(1), // Zstd
		WithCompressionMinSize(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Highly compressible data at various sizes.
	sizes := []int{512, 4000, 4097, 50000, 1 << 20}
	type entry struct {
		key   []byte
		value []byte
	}
	entries := make([]entry, len(sizes))

	for i, size := range sizes {
		key := fmt.Appendf(nil, "zstd-key-%d", i)
		// Repeating pattern compresses well.
		value := make([]byte, size)
		for j := range value {
			value[j] = byte(j % 7)
		}
		entries[i] = entry{key: key, value: value}
		require.NoError(t, cache.Put(key, value))
	}

	cache.Drain()

	for i, e := range entries {
		value, found := cache.Get(e.key)
		require.True(t, found, "compressed key %d not found", i)
		require.Equal(t, e.value, value, "compressed key %d data mismatch", i)
	}
}

// TestCache_DirectIORead_ManyRecords writes enough records to span multiple
// segments, ensuring Direct I/O reads work across segment boundaries.
func TestCache_DirectIORead_ManyRecords(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "directio-read-multi-seg-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithDirectIORead(true),
		WithWriteBufferSize(1<<20), // 1MB slabs — forces frequent rotation
	)
	require.NoError(t, err)
	defer cache.Close()

	const numKeys = 50
	const valueSize = 100_000 // 100KB each → ~5MB total → several 1MB segments

	type entry struct {
		key   []byte
		value []byte
	}
	entries := make([]entry, numKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Appendf(nil, "multi-seg-%04d", i)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte((i + j*3) % 256)
		}
		entries[i] = entry{key: key, value: value}
		require.NoError(t, cache.Put(key, value))
	}

	cache.Drain()

	for i, e := range entries {
		value, found := cache.Get(e.key)
		require.True(t, found, "multi-seg key %d not found", i)
		require.Equal(t, len(e.value), len(value), "multi-seg key %d size mismatch", i)
		require.Equal(t, e.value, value, "multi-seg key %d data mismatch", i)
	}
}
