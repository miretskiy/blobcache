package index

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mills.io/bitcask/v2"
)

// TestTombstone_KeyStructure validates the tombstone key format.
func TestTombstone_KeyStructure(t *testing.T) {
	tmpDir := t.TempDir()
	p, err := newPersistence(tmpDir)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(42)
	keyHash := Key{Lo: 0x1234567890ABCDEF, Hi: 0xFEDCBA0987654321}

	t.Run("without user key (eviction)", func(t *testing.T) {
		tombKey := p.makeTombstoneKey(segID, keyHash, nil)

		require.Equal(t, 21, len(tombKey), "tombstone without user key should be 21 bytes")
		require.Equal(t, prefixTombstone, tombKey[0], "first byte should be tombstone prefix")

		// Verify segment ID
		gotSegID := binary.BigEndian.Uint32(tombKey[1:5])
		require.Equal(t, segID, gotSegID)

		// Verify hash
		gotLo := binary.BigEndian.Uint64(tombKey[5:13])
		gotHi := binary.BigEndian.Uint64(tombKey[13:21])
		require.Equal(t, keyHash.Lo, gotLo)
		require.Equal(t, keyHash.Hi, gotHi)
	})

	t.Run("with user key (delete)", func(t *testing.T) {
		userKey := []byte("my-user-key-data")
		tombKey := p.makeTombstoneKey(segID, keyHash, userKey)

		expectedLen := 21 + len(userKey)
		require.Equal(t, expectedLen, len(tombKey))
		require.Equal(t, prefixTombstone, tombKey[0])

		// Verify user key at end
		gotUserKey := tombKey[21:]
		require.Equal(t, userKey, gotUserKey)
	})
}

// TestTombstone_WriteAndLoad validates tombstone persistence and loading.
func TestTombstone_WriteAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	p, err := newPersistence(tmpDir)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(10)
	key1 := Key{Lo: 1, Hi: 100}
	key2 := Key{Lo: 2, Hi: 200}
	key3 := Key{Lo: 3, Hi: 300}

	// Write tombstones
	require.NoError(t, p.tombstone(segID, key1, []byte("user-key-1")))
	require.NoError(t, p.tombstone(segID, key2, nil)) // Eviction (no user key)
	require.NoError(t, p.tombstone(segID, key3, []byte("user-key-3")))

	// Load tombstones via scanSegment
	var loaded map[Key]struct{}
	txn := p.db.Transaction()
	defer txn.Discard()

	tombStart := make([]byte, 5)
	tombStart[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombStart[1:5], segID)

	tombEnd := make([]byte, 5)
	tombEnd[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombEnd[1:5], segID+1)

	loaded = make(map[Key]struct{})
	err = txn.Range(tombStart, tombEnd, func(key bitcask.Key) error {
		if len(key) >= 21 {
			k := Key{
				Lo: binary.BigEndian.Uint64(key[5:13]),
				Hi: binary.BigEndian.Uint64(key[13:21]),
			}
			loaded[k] = struct{}{}
		}
		return nil
	})
	require.NoError(t, err)

	// Verify all tombstones were loaded
	require.Len(t, loaded, 3)
	require.Contains(t, loaded, key1)
	require.Contains(t, loaded, key2)
	require.Contains(t, loaded, key3)
}

// TestTombstone_ScanSegmentMerge validates that scanSegment merges tombstones.
func TestTombstone_ScanSegmentMerge(t *testing.T) {
	tmpDir := t.TempDir()
	p, err := newPersistence(tmpDir)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(5)

	// Write regular items
	items := []Item{
		{Key: Key{Lo: 1, Hi: 10}, SegmentID: segID, Offset: 0, PhysicalLen: 100},
		{Key: Key{Lo: 2, Hi: 20}, SegmentID: segID, Offset: 100, PhysicalLen: 200},
		{Key: Key{Lo: 3, Hi: 30}, SegmentID: segID, Offset: 300, PhysicalLen: 300},
	}
	require.NoError(t, p.writeBatch(segID, items, 1000))

	// Write tombstones for keys 1 and 3
	require.NoError(t, p.tombstone(segID, items[0].Key, []byte("key-1")))
	require.NoError(t, p.tombstone(segID, items[2].Key, nil)) // Eviction style

	// Scan segment - should merge tombstones
	var scannedItems []Item
	err = p.scanSegment(segID, func(m DurableBatch) bool {
		scannedItems = append(scannedItems, m.Items...)
		return true
	})
	require.NoError(t, err)

	// Verify all 3 items present
	require.Len(t, scannedItems, 3)

	// Verify items 0 and 2 are marked deleted (tombstones applied)
	require.True(t, scannedItems[0].IsDeleted(), "item 0 should be deleted (tombstone)")
	require.False(t, scannedItems[1].IsDeleted(), "item 1 should be live (no tombstone)")
	require.True(t, scannedItems[2].IsDeleted(), "item 2 should be deleted (tombstone)")

	// Verify keys match
	require.Equal(t, items[0].Key, scannedItems[0].Key)
	require.Equal(t, items[1].Key, scannedItems[1].Key)
	require.Equal(t, items[2].Key, scannedItems[2].Key)
}

// TestTombstone_NamespaceIsolation verifies regular and tombstone keys don't collide.
func TestTombstone_NamespaceIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	p, err := newPersistence(tmpDir)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(1)
	keyHash := Key{Lo: 999, Hi: 888}

	// Write regular item
	items := []Item{
		{Key: keyHash, SegmentID: segID, Offset: 0, PhysicalLen: 100},
	}
	require.NoError(t, p.writeBatch(segID, items, 100))

	// Write tombstone with same hash
	require.NoError(t, p.tombstone(segID, keyHash, []byte("user-key")))

	// Scan regular data - should see live item
	var regularItems []Item
	err = p.scanSegment(segID, func(m DurableBatch) bool {
		regularItems = append(regularItems, m.Items...)
		return true
	})
	require.NoError(t, err)

	// Should see item marked as deleted (tombstone merged)
	require.Len(t, regularItems, 1)
	require.True(t, regularItems[0].IsDeleted(), "tombstone should have been merged")
}

// TestTombstone_MultipleSegments validates tombstone isolation across segments.
func TestTombstone_MultipleSegments(t *testing.T) {
	tmpDir := t.TempDir()
	p, err := newPersistence(tmpDir)
	require.NoError(t, err)
	defer p.close()

	// Write items to different segments
	seg10Items := []Item{
		{Key: Key{Lo: 1, Hi: 10}, SegmentID: 10, Offset: 0, PhysicalLen: 100},
	}
	seg20Items := []Item{
		{Key: Key{Lo: 2, Hi: 20}, SegmentID: 20, Offset: 0, PhysicalLen: 100},
	}
	require.NoError(t, p.writeBatch(10, seg10Items, 100))
	require.NoError(t, p.writeBatch(20, seg20Items, 200))

	// Write tombstones to both segments
	require.NoError(t, p.tombstone(10, seg10Items[0].Key, []byte("key-10")))
	require.NoError(t, p.tombstone(20, seg20Items[0].Key, []byte("key-20")))

	// Scan segment 10 - should only see segment 10 tombstone
	var items10 []Item
	err = p.scanSegment(10, func(m DurableBatch) bool {
		items10 = append(items10, m.Items...)
		return true
	})
	require.NoError(t, err)
	require.Len(t, items10, 1)
	require.True(t, items10[0].IsDeleted())

	// Scan segment 20 - should only see segment 20 tombstone
	var items20 []Item
	err = p.scanSegment(20, func(m DurableBatch) bool {
		items20 = append(items20, m.Items...)
		return true
	})
	require.NoError(t, err)
	require.Len(t, items20, 1)
	require.True(t, items20[0].IsDeleted())
}

// TestTombstone_Performance benchmarks tombstone write throughput.
func TestTombstone_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	tmpDir := t.TempDir()
	p, err := newPersistence(tmpDir)
	require.NoError(t, err)
	defer p.close()

	segID := uint32(1)
	numTombstones := 1000

	// Write tombstones (simulating eviction with no user keys)
	for i := 0; i < numTombstones; i++ {
		k := Key{Lo: uint64(i), Hi: 0}
		err := p.tombstone(segID, k, nil)
		require.NoError(t, err)
	}

	// Verify all tombstones present
	count := 0
	tombStart := make([]byte, 5)
	tombStart[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombStart[1:5], segID)

	tombEnd := make([]byte, 5)
	tombEnd[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombEnd[1:5], segID+1)

	err = p.db.Range(tombStart, tombEnd, func(key bitcask.Key) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, numTombstones, count)

	t.Logf("Successfully wrote and verified %d tombstones", numTombstones)
}
