package blobcache

import (
	"bytes"
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

	iter, err := cache.NewIterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	t.Run("ExactMatch", func(t *testing.T) {
		require.True(t, iter.SeekGE([]byte("ccc")))
		require.Equal(t, "ccc", string(iter.Key()))
	})

	t.Run("BetweenKeys", func(t *testing.T) {
		require.True(t, iter.SeekGE([]byte("ddd")))
		require.Equal(t, "eee", string(iter.Key()))
	})

	t.Run("BeforeFirst", func(t *testing.T) {
		require.True(t, iter.SeekGE([]byte("a")))
		require.Equal(t, "aaa", string(iter.Key()))
	})

	t.Run("AfterLast", func(t *testing.T) {
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

// TestIterator_Dedup verifies that when the same key exists in multiple segments,
// only the newest version is yielded.
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

	// Should appear exactly once (dedup by merging iter).
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

// TestMergingIter_ManySegments verifies correct merge across many segments.
func TestMergingIter_ManySegments(t *testing.T) {
	// Create mock sources simulating many segments.
	numSources := 50
	keysPerSource := 10

	var sources []mergeSource
	for s := range numSources {
		var entries []mockEntry
		for k := range keysPerSource {
			key := fmt.Appendf(nil, "seg%03d-key%03d", s, k)
			entries = append(entries, mockEntry{key: key})
		}
		sources = append(sources, mergeSource{
			segID: uint32(s + 1),
			iter:  &mockIter{entries: entries, pos: -1},
		})
	}

	m := newMergingIter(sources, bytes.Compare)
	defer m.Close()

	var collected []string
	key, _, ok := m.First()
	for ok {
		collected = append(collected, string(key))
		key, _, ok = m.Next()
	}
	require.NoError(t, m.Error())

	// All keys should be present and sorted.
	require.Len(t, collected, numSources*keysPerSource)
	for i := 1; i < len(collected); i++ {
		require.True(t, collected[i-1] < collected[i],
			"keys not in order at position %d: %q >= %q", i, collected[i-1], collected[i])
	}
}

// TestMergingIter_Dedup verifies same key across segments — newest wins.
func TestMergingIter_Dedup(t *testing.T) {
	sharedKey := []byte("shared")

	sources := []mergeSource{
		{segID: 1, iter: &mockIter{entries: []mockEntry{
			{key: append([]byte(nil), sharedKey...), segID: 1},
			{key: []byte("only-in-1")},
		}, pos: -1}},
		{segID: 3, iter: &mockIter{entries: []mockEntry{
			{key: []byte("only-in-3")},
			{key: append([]byte(nil), sharedKey...), segID: 3},
		}, pos: -1}},
		{segID: 2, iter: &mockIter{entries: []mockEntry{
			{key: []byte("only-in-2")},
			{key: append([]byte(nil), sharedKey...), segID: 2},
		}, pos: -1}},
	}

	m := newMergingIter(sources, bytes.Compare)
	defer m.Close()

	var collected []string
	var seenSharedSegID uint64
	key, val, ok := m.First()
	for ok {
		collected = append(collected, string(key))
		if string(key) == "shared" {
			seenSharedSegID = val.Hash.Lo // We encode segID in the mock value
		}
		key, val, ok = m.Next()
	}
	require.NoError(t, m.Error())

	// "shared" should appear exactly once.
	sharedCount := 0
	for _, k := range collected {
		if k == "shared" {
			sharedCount++
		}
	}
	require.Equal(t, 1, sharedCount, "shared key should appear once")

	// The version from segment 3 (highest segID) should win.
	require.Equal(t, uint64(3), seenSharedSegID,
		"highest segID should win for duplicate keys")
}

// --- Mock iterator for unit testing the merging iterator ---

type mockEntry struct {
	key   []byte
	segID uint32 // stored in sstValue.Hash.Lo for dedup verification
}

type mockIter struct {
	entries []mockEntry
	pos     int
}

func (m *mockIter) First() ([]byte, []byte, bool) {
	m.pos = 0
	return m.current()
}

func (m *mockIter) Next() ([]byte, []byte, bool) {
	m.pos++
	return m.current()
}

func (m *mockIter) SeekGE(target []byte) ([]byte, []byte, bool) {
	m.pos = 0
	for m.pos < len(m.entries) {
		if bytes.Compare(m.entries[m.pos].key, target) >= 0 {
			return m.current()
		}
		m.pos++
	}
	return nil, nil, false
}

func (m *mockIter) current() ([]byte, []byte, bool) {
	if m.pos >= len(m.entries) {
		return nil, nil, false
	}
	e := &m.entries[m.pos]
	// Encode a mock sstValue with segID in Hash.Lo for dedup tests.
	val := encodeSSTValue(&sstEntry{
		Hash:    Key{Lo: uint64(e.segID)},
		KeyLen:  uint16(len(e.key)),
	})
	return e.key, val[:], true
}

func (m *mockIter) Close() error { return nil }
func (m *mockIter) Error() error { return nil }
