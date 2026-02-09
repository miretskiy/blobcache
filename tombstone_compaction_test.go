package blobcache

import (
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// TestTombstoneCompaction_Basic validates the tombstone compaction flow.
func TestTombstoneCompaction_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),             // CAS mode (deletes don't hole punch immediately)
		WithMaxCachedSlabs(0), // Force disk path
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write several blobs
	keys := [][]byte{
		[]byte("key-1"),
		[]byte("key-2"),
		[]byte("key-3"),
		[]byte("key-4"),
	}
	value := make([]byte, 10_000) // 10KB each

	for _, k := range keys {
		require.NoError(t, cache.Put(k, value))
	}
	cache.Drain()

	// Get segment ID
	h := xxh3.Hash128(keys[0])
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	// Delete keys 1 and 3 (CAS mode - no immediate hole punch)
	require.NoError(t, cache.Delete(keys[0]))
	require.NoError(t, cache.Delete(keys[2]))

	// Verify tombstones exist in incremental log
	// (We can't easily inspect Bitcask directly, but we can check via scanSegment)
	var itemsBeforeCompaction []index.Item
	err = cache.index.ForEachSegment(func(m index.DurableBatch) bool {
		if m.SegmentID == segID {
			itemsBeforeCompaction = append(itemsBeforeCompaction, m.Items...)
		}
		return true
	})
	require.NoError(t, err)
	require.Len(t, itemsBeforeCompaction, 4)

	// Items should be marked deleted (tombstones merged during scan)
	require.True(t, itemsBeforeCompaction[0].IsDeleted())
	require.False(t, itemsBeforeCompaction[1].IsDeleted())
	require.True(t, itemsBeforeCompaction[2].IsDeleted())
	require.False(t, itemsBeforeCompaction[3].IsDeleted())

	// Track hole punch calls
	punchedKeys := make(map[index.Key]bool)

	// Run tombstone compaction with callback
	shard := cache.index.SegmentLockShard(segID)
	shard.Lock()
	err = cache.index.CompactTombstones(segID, func(tr index.TombstoneRecord) {
		punchedKeys[tr.KeyHash] = true
		// No-op: hole punching removed; merge compaction reclaims space
	})
	shard.Unlock()
	require.NoError(t, err)

	// Verify callback was called for both tombstones
	h1 := xxh3.Hash128(keys[0])
	h3 := xxh3.Hash128(keys[2])
	require.True(t, punchedKeys[h1], "callback should be invoked for key-1 tombstone")
	require.True(t, punchedKeys[h3], "callback should be invoked for key-3 tombstone")
	require.Len(t, punchedKeys, 2, "callback should only be called for actual tombstones")

	// Verify tombstones are now collapsed into manifest
	var itemsAfterCompaction []index.Item
	err = cache.index.ForEachSegment(func(m index.DurableBatch) bool {
		if m.SegmentID == segID {
			itemsAfterCompaction = append(itemsAfterCompaction, m.Items...)
		}
		return true
	})
	require.NoError(t, err)
	require.Len(t, itemsAfterCompaction, 4, "items should still be present")

	// Items should still be marked deleted
	require.True(t, itemsAfterCompaction[0].IsDeleted())
	require.False(t, itemsAfterCompaction[1].IsDeleted())
	require.True(t, itemsAfterCompaction[2].IsDeleted())
	require.False(t, itemsAfterCompaction[3].IsDeleted())
}

// TestTombstoneCompaction_Idempotent validates that hole punching is idempotent.
func TestTombstoneCompaction_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Write and delete
	key := []byte("test-key")
	value := make([]byte, 10_000)
	require.NoError(t, cache.Put(key, value))
	cache.Drain()

	h := xxh3.Hash128(key)
	item, found := cache.index.Get(h)
	require.True(t, found)
	segID := item.SegmentID

	require.NoError(t, cache.Delete(key))

	// Run tombstone compaction twice
	shard := cache.index.SegmentLockShard(segID)

	shard.Lock()
	err = cache.index.CompactTombstones(segID, func(tr index.TombstoneRecord) {
		// First compaction - should hole punch
	})
	shard.Unlock()
	require.NoError(t, err)

	// Second compaction - should be no-op (no tombstones left)
	shard.Lock()
	callbackInvoked := false
	err = cache.index.CompactTombstones(segID, func(tr index.TombstoneRecord) {
		callbackInvoked = true
	})
	shard.Unlock()
	require.NoError(t, err)
	require.False(t, callbackInvoked, "second compaction should find no tombstones")
}
