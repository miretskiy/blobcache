package blobcache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIterator_BasicIteration writes keys across multiple segments and verifies
// they are yielded in sorted order.
func TestIterator_BasicIteration(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20), WithWriteBufferSize(1<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write 100 keys — small WriteBufferSize forces multiple segments.
	const numKeys = 100
	valueSize := 8 * 1024
	value := make([]byte, valueSize)
	keys := make([][]byte, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, cache.Put(keys[i], value))
	}
	cache.Drain()

	// Iterate and verify sorted order.
	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	var collected []string
	for ok := iter.First(); ok; ok = iter.Next() {
		collected = append(collected, string(iter.Key()))
	}
	require.NoError(t, iter.Error())

	// All keys should be present and sorted.
	require.Len(t, collected, numKeys)
	for i := 1; i < len(collected); i++ {
		require.True(t, collected[i-1] < collected[i],
			"keys not in order: %q >= %q at position %d", collected[i-1], collected[i], i)
	}

	// Verify exact keys.
	for i := range numKeys {
		expected := fmt.Sprintf("key-%04d", i)
		require.Equal(t, expected, collected[i])
	}
}

// TestIterator_SeekGE verifies seeking to various positions.
func TestIterator_SeekGE(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write keys with gaps.
	for _, key := range []string{"aaa", "ccc", "eee", "ggg", "iii"} {
		require.NoError(t, cache.Put([]byte(key), []byte("value")))
	}
	cache.Drain()

	t.Run("ExactMatch", func(t *testing.T) {
		iter, err := cache.NewIterator(nil, nil)
		require.NoError(t, err)
		defer iter.Close()
		require.True(t, iter.SeekGE([]byte("ccc")))
		require.Equal(t, "ccc", string(iter.Key()))
	})

	t.Run("BetweenKeys", func(t *testing.T) {
		iter, err := cache.NewIterator(nil, nil)
		require.NoError(t, err)
		defer iter.Close()
		require.True(t, iter.SeekGE([]byte("ddd")))
		require.Equal(t, "eee", string(iter.Key()))
	})

	t.Run("BeforeFirst", func(t *testing.T) {
		iter, err := cache.NewIterator(nil, nil)
		require.NoError(t, err)
		defer iter.Close()
		require.True(t, iter.SeekGE([]byte("a")))
		require.Equal(t, "aaa", string(iter.Key()))
	})

	t.Run("AfterLast", func(t *testing.T) {
		iter, err := cache.NewIterator(nil, nil)
		require.NoError(t, err)
		defer iter.Close()
		require.False(t, iter.SeekGE([]byte("zzz")))
	})
}

// TestIterator_TombstoneFiltering verifies that deleted keys are skipped.
func TestIterator_TombstoneFiltering(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write 10 keys, then delete half.
	for i := range 10 {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%02d", i), []byte("value")))
	}
	cache.Drain()

	// Delete even-numbered keys.
	for i := range 10 {
		if i%2 == 0 {
			require.NoError(t, cache.Delete(fmt.Appendf(nil, "key-%02d", i)))
		}
	}

	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	var collected []string
	for ok := iter.First(); ok; ok = iter.Next() {
		collected = append(collected, string(iter.Key()))
	}
	require.NoError(t, iter.Error())

	// Only odd-numbered keys should remain.
	require.Len(t, collected, 5)
	for _, key := range collected {
		var idx int
		_, err := fmt.Sscanf(key, "key-%02d", &idx)
		require.NoError(t, err)
		require.True(t, idx%2 == 1, "even key %q should have been filtered", key)
	}
}

// TestIterator_Dedup verifies that when the same key is written twice,
// it appears exactly once in iteration (Pebble stores one entry per user key).
func TestIterator_Dedup(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20), WithWriteBufferSize(1<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write key "dup" in first segment.
	bigValue := make([]byte, 128*1024) // Force flush to create segment
	require.NoError(t, cache.Put([]byte("dup"), []byte("old")))
	for i := range 8 {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "filler1-%04d", i), bigValue))
	}
	cache.Drain()

	// Write key "dup" again in a new segment.
	require.NoError(t, cache.Put([]byte("dup"), []byte("new")))
	for i := range 8 {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "filler2-%04d", i), bigValue))
	}
	cache.Drain()

	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	// Count occurrences of "dup" key.
	dupCount := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if string(iter.Key()) == "dup" {
			dupCount++
		}
	}
	require.NoError(t, iter.Error())

	// Should appear exactly once (Pebble stores one key→hash mapping).
	require.Equal(t, 1, dupCount, "duplicate key should appear exactly once")
}

// TestIterator_BoundsRespected verifies lower/upper bounds.
func TestIterator_BoundsRespected(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20))
	require.NoError(t, err)
	defer cache.Close()

	for _, key := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		require.NoError(t, cache.Put([]byte(key), []byte("value")))
	}
	cache.Drain()

	// [bbb, ddd) — should include bbb, ccc but NOT ddd.
	iter, err := cache.NewIterator([]byte("bbb"), []byte("ddd"))
	require.NoError(t, err)
	defer iter.Close()

	var collected []string
	for ok := iter.First(); ok; ok = iter.Next() {
		collected = append(collected, string(iter.Key()))
	}
	require.NoError(t, iter.Error())

	require.Equal(t, []string{"bbb", "ccc"}, collected)
}

// TestIterator_ViewData verifies that View() returns correct data for
// every key via the direct-read path (bypassing bloom + index lookup).
func TestIterator_ViewData(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20), WithWriteBufferSize(1<<20))
	require.NoError(t, err)
	defer cache.Close()

	// Write keys with recognizable values — small WriteBufferSize forces
	// multiple segments so we test both within-segment and cross-segment reads.
	const numKeys = 50
	valueSize := 8 * 1024
	expected := make(map[string][]byte, numKeys)
	for i := range numKeys {
		key := fmt.Appendf(nil, "key-%04d", i)
		val := make([]byte, valueSize)
		// Stamp key name into value for later verification.
		copy(val, key)
		require.NoError(t, cache.Put(key, val))
		expected[string(key)] = val
	}
	cache.Drain()

	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		key := string(iter.Key())
		got := iter.View(func(data []byte) {
			exp, ok := expected[key]
			require.True(t, ok, "unexpected key %q", key)
			require.Equal(t, len(exp), len(data), "size mismatch for key %q", key)
			// Verify the key stamp in the value.
			require.Equal(t, key, string(data[:len(key)]),
				"data mismatch for key %q", key)
		})
		require.True(t, got, "View() returned false for live key %q", key)
		seen++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, numKeys, seen)
}

// TestIterator_ViewAfterReopen verifies View() works after closing and
// reopening the cache (cold librarian, exercises the disk path).
func TestIterator_ViewAfterReopen(t *testing.T) {
	tmpDir := t.TempDir()

	const numKeys = 20
	valueSize := 16 * 1024

	// Phase 1: populate and close.
	{
		cache, err := New(tmpDir, WithMaxSize(100<<20), WithWriteBufferSize(1<<20))
		require.NoError(t, err)
		for i := range numKeys {
			val := make([]byte, valueSize)
			val[0] = byte(i)
			require.NoError(t, cache.Put(fmt.Appendf(nil, "k-%04d", i), val))
		}
		cache.Drain()
		require.NoError(t, cache.Close())
	}

	// Phase 2: reopen and iterate — librarian is empty, all reads from disk.
	cache, err := New(tmpDir, WithMaxSize(100<<20), WithWriteBufferSize(1<<20))
	require.NoError(t, err)
	defer cache.Close()

	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	count := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		got := iter.View(func(data []byte) {
			require.Equal(t, valueSize, len(data))
		})
		require.True(t, got, "View() returned false for live key")
		count++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, numKeys, count)
}

// TestIterator_PrefetchHits verifies that the read-ahead prefetch buffer
// reduces disk reads when iterating sequentially. With contiguous records,
// a View() that reads ahead should let subsequent View() calls hit the buffer.
// Tests both buffered and directIO read paths.
func TestIterator_PrefetchHits(t *testing.T) {
	for _, directIO := range []bool{false, true} {
		name := "buffered"
		if directIO {
			name = "directio"
		}
		t.Run(name, func(t *testing.T) {
			tmpDir := t.TempDir()

			const numKeys = 200
			valueSize := 8 * 1024 // 8KB values — small enough to fit many per segment

			// Phase 1: populate and close (ensures cold librarian on reopen).
			{
				cache, err := New(tmpDir, WithMaxSize(100<<20), WithWriteBufferSize(1<<20))
				require.NoError(t, err)
				for i := range numKeys {
					val := make([]byte, valueSize)
					val[0] = byte(i)
					require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%04d", i), val))
				}
				cache.Drain()
				require.NoError(t, cache.Close())
			}

			// Phase 2: reopen and iterate with View() on every key.
			cache, err := New(tmpDir,
				WithMaxSize(100<<20),
				WithWriteBufferSize(1<<20),
				WithDirectIORead(directIO),
			)
			require.NoError(t, err)
			defer cache.Close()

			iter, err := cache.NewIterator(nil, nil)
			require.NoError(t, err)
			defer iter.Close()

			count := 0
			for ok := iter.First(); ok; ok = iter.Next() {
				got := iter.View(func(data []byte) {
					require.Equal(t, valueSize, len(data))
				})
				require.True(t, got, "View() returned false for live key")
				count++
			}
			require.NoError(t, iter.Error())
			require.Equal(t, numKeys, count)

			// Verify read-ahead stats.
			t.Logf("PrefetchHits=%d  PrefetchMisses=%d  ReadAheadBytes=%d",
				iter.Stats.PrefetchHits, iter.Stats.PrefetchMisses, iter.Stats.ReadAheadBytes)

			// With contiguous records in the same segment, most View() calls should
			// be served from the prefetch buffer. We expect at least 50% hit rate
			// (actually much higher — one miss per segment boundary + first read).
			totalViews := iter.Stats.PrefetchHits + iter.Stats.PrefetchMisses
			require.Equal(t, int64(numKeys), totalViews, "total views should match key count")
			hitRate := float64(iter.Stats.PrefetchHits) / float64(totalViews)
			t.Logf("Prefetch hit rate: %.1f%%", hitRate*100)
			require.Greater(t, hitRate, 0.5,
				"prefetch hit rate should be >50%% for sequential iteration, got %.1f%%", hitRate*100)
			require.Greater(t, iter.Stats.ReadAheadBytes, int64(0),
				"should have read ahead some bytes")
		})
	}
}

// TestIterator_Empty verifies iteration over an empty cache.
func TestIterator_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := New(tmpDir, WithMaxSize(100<<20))
	require.NoError(t, err)
	defer cache.Close()

	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	require.False(t, iter.First())
	require.NoError(t, iter.Error())
	require.False(t, iter.Valid())
}
