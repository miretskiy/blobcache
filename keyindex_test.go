package blobcache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

func makeHash(key string) Key {
	return xxh3.HashString128(key)
}

func makeEntry(key string) KeyIndexEntry {
	return KeyIndexEntry{
		UserKey: []byte(key),
		Hash:    makeHash(key),
	}
}

func TestKeyIndex_AddAndIterate(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	// Add entries for two segments.
	seg1Entries := []KeyIndexEntry{
		makeEntry("banana"),
		makeEntry("cherry"),
	}
	seg2Entries := []KeyIndexEntry{
		makeEntry("apple"),
		makeEntry("date"),
	}

	require.NoError(t, ki.AddEntries(1, seg1Entries))
	require.NoError(t, ki.AddEntries(2, seg2Entries))

	// Iterate over 0x01 namespace — should yield keys in sorted order.
	snap := ki.NewSnapshot()
	defer snap.Close()

	lower := []byte{nsKeyToHash}
	upper := []byte{nsKeyToHash + 1}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	require.NoError(t, err)

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		keys = append(keys, string(k[1:])) // strip prefix
	}
	require.NoError(t, iter.Close())

	require.Equal(t, []string{"apple", "banana", "cherry", "date"}, keys)
}

func TestKeyIndex_DeleteByHash(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	entries := []KeyIndexEntry{
		makeEntry("alpha"),
		makeEntry("beta"),
		makeEntry("gamma"),
	}
	require.NoError(t, ki.AddEntries(1, entries))

	// Delete beta by hash (simulates eviction).
	require.NoError(t, ki.DeleteByHash(makeHash("beta")))

	// Verify beta is gone from both namespaces.
	snap := ki.NewSnapshot()
	defer snap.Close()

	// Check 0x01 (key→hash).
	lower := []byte{nsKeyToHash}
	upper := []byte{nsKeyToHash + 1}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	require.NoError(t, err)

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()[1:]))
	}
	require.NoError(t, iter.Close())
	require.Equal(t, []string{"alpha", "gamma"}, keys)

	// Check 0x00 (hash→key) — beta's hash should return not-found.
	hashKey := encodeHashLookupKey(makeHash("beta"))
	_, closer, err := snap.Get(hashKey)
	require.ErrorIs(t, err, pebble.ErrNotFound)
	if closer != nil {
		closer.Close()
	}
}

func TestKeyIndex_DeleteByUserKey(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	entries := []KeyIndexEntry{
		makeEntry("foo"),
		makeEntry("bar"),
	}
	require.NoError(t, ki.AddEntries(1, entries))

	// Delete foo by user key + hash (simulates explicit Delete).
	require.NoError(t, ki.DeleteByUserKey([]byte("foo"), makeHash("foo")))

	// Verify foo is gone.
	snap := ki.NewSnapshot()
	defer snap.Close()

	lower := []byte{nsKeyToHash}
	upper := []byte{nsKeyToHash + 1}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	require.NoError(t, err)

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()[1:]))
	}
	require.NoError(t, iter.Close())
	require.Equal(t, []string{"bar"}, keys)
}

func TestKeyIndex_DrainSegment(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	// Add entries for two segments.
	seg1Entries := []KeyIndexEntry{
		makeEntry("k1"),
		makeEntry("k2"),
	}
	seg2Entries := []KeyIndexEntry{
		makeEntry("k3"),
		makeEntry("k4"),
	}

	require.NoError(t, ki.AddEntries(10, seg1Entries))
	require.NoError(t, ki.AddEntries(20, seg2Entries))

	// Drain segment 10.
	require.NoError(t, ki.DrainSegment(10))

	// Only segment 20 keys should remain.
	snap := ki.NewSnapshot()
	defer snap.Close()

	lower := []byte{nsKeyToHash}
	upper := []byte{nsKeyToHash + 1}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	require.NoError(t, err)

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()[1:]))
	}
	require.NoError(t, iter.Close())
	require.Equal(t, []string{"k3", "k4"}, keys)

	// Sentinel for seg 10 should be gone.
	has, err := ki.HasSentinel(10)
	require.NoError(t, err)
	require.False(t, has)

	// Sentinel for seg 20 should still exist.
	has, err = ki.HasSentinel(20)
	require.NoError(t, err)
	require.True(t, has)
}

func TestKeyIndex_RelocateSegment(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	entries := []KeyIndexEntry{
		makeEntry("a"),
		makeEntry("b"),
	}
	require.NoError(t, ki.AddEntries(100, entries))

	// Relocate from seg 100 to seg 200.
	hashes := []Key{makeHash("a"), makeHash("b")}
	require.NoError(t, ki.RelocateSegment(100, 200, hashes))

	// Old sentinel gone.
	has, err := ki.HasSentinel(100)
	require.NoError(t, err)
	require.False(t, has)

	// New sentinel present.
	has, err = ki.HasSentinel(200)
	require.NoError(t, err)
	require.True(t, has)

	// User keys still iterable (0x01 namespace unchanged by relocate).
	snap := ki.NewSnapshot()
	defer snap.Close()

	lower := []byte{nsKeyToHash}
	upper := []byte{nsKeyToHash + 1}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	require.NoError(t, err)

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()[1:]))
	}
	require.NoError(t, iter.Close())
	require.Equal(t, []string{"a", "b"}, keys)

	// Old segment membership gone (0x02+100+hash).
	prefix := make([]byte, 5)
	prefix[0] = nsSegMember
	binary.BigEndian.PutUint32(prefix[1:5], 100)
	prefixUpper := prefixUpperBound(prefix)
	iter2, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpper,
	})
	require.NoError(t, err)
	require.False(t, iter2.First(), "old segment membership should be empty")
	require.NoError(t, iter2.Close())

	// New segment membership present (0x02+200+hash).
	prefix2 := make([]byte, 5)
	prefix2[0] = nsSegMember
	binary.BigEndian.PutUint32(prefix2[1:5], 200)
	prefixUpper2 := prefixUpperBound(prefix2)
	iter3, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix2,
		UpperBound: prefixUpper2,
	})
	require.NoError(t, err)
	count := 0
	for iter3.First(); iter3.Valid(); iter3.Next() {
		count++
	}
	require.NoError(t, iter3.Close())
	require.Equal(t, 2, count)
}

func TestKeyIndex_Sentinel(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	// Initially no sentinel.
	has, err := ki.HasSentinel(42)
	require.NoError(t, err)
	require.False(t, has)

	// Set sentinel.
	require.NoError(t, ki.SetSentinel(42))

	// Now present.
	has, err = ki.HasSentinel(42)
	require.NoError(t, err)
	require.True(t, has)
}

func TestKeyIndex_DeleteNonExistent(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	// Deleting a non-existent hash should not error.
	require.NoError(t, ki.DeleteByHash(makeHash("phantom")))

	// Deleting a non-existent user key should not error.
	require.NoError(t, ki.DeleteByUserKey([]byte("ghost"), makeHash("ghost")))

	// Draining a non-existent segment should not error.
	require.NoError(t, ki.DrainSegment(999))
}

func TestKeyIndex_RoundTripHashEncoding(t *testing.T) {
	h := Key{Lo: 0x1234567890ABCDEF, Hi: 0xFEDCBA0987654321}
	encoded := encodeHashValue(h)
	decoded := decodeHash(encoded)
	require.Equal(t, h, decoded)
}

func TestKeyIndex_LargeSegment(t *testing.T) {
	dir := t.TempDir()
	ki, err := OpenKeyIndex(dir)
	require.NoError(t, err)
	defer ki.Close()

	// Add 1000 entries.
	entries := make([]KeyIndexEntry, 1000)
	for i := range entries {
		key := fmt.Sprintf("key-%05d", i)
		entries[i] = makeEntry(key)
	}
	require.NoError(t, ki.AddEntries(1, entries))

	// Verify ordered iteration yields all 1000 in sorted order.
	snap := ki.NewSnapshot()
	defer snap.Close()

	lower := []byte{nsKeyToHash}
	upper := []byte{nsKeyToHash + 1}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	require.NoError(t, err)

	count := 0
	var prev []byte
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()[1:] // strip prefix
		if prev != nil {
			require.True(t, bytes.Compare(prev, k) < 0, "keys not in order: %q >= %q", prev, k)
		}
		prev = append(prev[:0], k...)
		count++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, 1000, count)
}

func TestKeyIndex_PrefixUpperBound(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x02, 0x00, 0x01}, []byte{0x02, 0x00, 0x02}},
		{[]byte{0x02, 0x00, 0xFF}, []byte{0x02, 0x01, 0x00}},
		{[]byte{0xFF, 0xFF}, nil},
		{[]byte{0x03}, []byte{0x04}},
	}

	for _, tt := range tests {
		result := prefixUpperBound(tt.input)
		require.Equal(t, tt.expected, result, "prefixUpperBound(%x)", tt.input)
	}
}
