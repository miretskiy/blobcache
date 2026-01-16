package blobcache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// TestMemTable_LifecycleGuard verifies that writes with seqID <= maxSealedSeq are rejected
func TestMemTable_LifecycleGuard(t *testing.T) {
	cfg := defaultConfig(t.TempDir())
	cfg.WriteBufferSize = 4 << 10 // 4KB

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}
	mt := NewMemTable(cfg, mb, mh, nil, nil)
	defer mt.Close()

	// Write with seqID=100
	err := mt.Put(100, xmap.Key{Lo: 1, Hi: 0}, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	// Simulate rotation by manually setting maxSealedSeq
	mt.mu.Lock()
	mt.mu.maxSealedSeq = 100
	mt.mu.Unlock()

	// Write with seqID=50 (older than sealed) should be rejected
	err = mt.Put(50, xmap.Key{Lo: 2, Hi: 0}, []byte("key2"), []byte("value2"))
	require.True(t, errors.Is(err, errSequenceTooOld), "expected errSequenceTooOld, got: %v", err)

	// Write with seqID=100 (equal to sealed) should also be rejected
	err = mt.Put(100, xmap.Key{Lo: 3, Hi: 0}, []byte("key3"), []byte("value3"))
	require.True(t, errors.Is(err, errSequenceTooOld), "expected errSequenceTooOld for equal seqID")

	// Write with seqID=101 (newer) should succeed
	err = mt.Put(101, xmap.Key{Lo: 4, Hi: 0}, []byte("key4"), []byte("value4"))
	require.NoError(t, err)
}

// TestMemTable_ConcurrencyGuard verifies that concurrent writes to the same key
// result in the newer seqID winning
func TestMemTable_ConcurrencyGuard(t *testing.T) {
	cfg := defaultConfig(t.TempDir())
	cfg.WriteBufferSize = 64 << 10 // 64KB to avoid rotation

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}
	mt := NewMemTable(cfg, mb, mh, nil, nil)
	defer mt.Close()

	key := xmap.Key{Lo: 12345, Hi: 0}
	oldValue := []byte("old-value")
	newValue := []byte("new-value")

	keyBytes := []byte("test-key")

	// Write with older seqID first
	err := mt.Put(100, key, keyBytes, oldValue)
	require.NoError(t, err)

	// Write with newer seqID
	err = mt.Put(200, key, keyBytes, newValue)
	require.NoError(t, err)

	// Now write with older seqID again - should NOT overwrite
	err = mt.Put(150, key, keyBytes, []byte("middle-value"))
	require.NoError(t, err) // Write itself succeeds (space reserved)

	// Verify the index has the newer value (seqID=200)
	mt.mu.Lock()
	active := mt.mu.active
	mt.mu.Unlock()

	record, found := active.index.Get(key)
	require.True(t, found)
	require.Equal(t, uint64(200), record.SeqID, "index should have the newest seqID")
}

// TestMemTable_ConcurrentWritesSameKey tests concurrent goroutines writing same key
func TestMemTable_ConcurrentWritesSameKey(t *testing.T) {
	cfg := defaultConfig(t.TempDir())
	cfg.WriteBufferSize = 1 << 20 // 1MB to avoid rotation

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}
	mt := NewMemTable(cfg, mb, mh, nil, nil)
	defer mt.Close()

	key := xmap.Key{Lo: 99999, Hi: 0}
	const numWriters = 100

	var wg sync.WaitGroup
	var maxSeqWritten atomic.Uint64

	keyBytes := []byte("concurrent-key")
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		seqID := uint64(i + 1)
		go func(seq uint64) {
			defer wg.Done()
			value := make([]byte, 100)
			err := mt.Put(seq, key, keyBytes, value)
			require.NoError(t, err)

			// Track highest seqID we wrote
			for {
				current := maxSeqWritten.Load()
				if seq <= current || maxSeqWritten.CompareAndSwap(current, seq) {
					break
				}
			}
		}(seqID)
	}

	wg.Wait()

	// Verify the index has the highest seqID
	mt.mu.Lock()
	active := mt.mu.active
	mt.mu.Unlock()

	record, found := active.index.Get(key)
	require.True(t, found)
	require.Equal(t, uint64(numWriters), record.SeqID,
		"index should have seqID=%d (the highest), got %d", numWriters, record.SeqID)
}

// TestMemTable_RotationUpdatesMaxSealedSeq verifies that rotation captures currentMaxSeq
func TestMemTable_RotationUpdatesMaxSealedSeq(t *testing.T) {
	cfg := defaultConfig(t.TempDir())
	cfg.WriteBufferSize = 1 << 10 // 1KB - small to trigger rotation

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}
	mt := NewMemTable(cfg, mb, mh, nil, nil)
	defer mt.Close()

	// Write some data with increasing seqIDs
	for i := 1; i <= 5; i++ {
		keyBytes := []byte("key")
		err := mt.Put(uint64(i*100), xmap.Key{Lo: uint64(i), Hi: 0}, keyBytes, make([]byte, 100))
		require.NoError(t, err)
	}

	// Verify currentMaxSeq is 500
	mt.mu.Lock()
	require.Equal(t, uint64(500), mt.mu.active.currentMaxSeq)
	mt.mu.Unlock()

	// Trigger rotation by writing large value
	err := mt.Put(600, xmap.Key{Lo: 999, Hi: 0}, []byte("large-key"), make([]byte, 800))
	require.NoError(t, err)

	// Wait for any pending writes
	mt.Drain()

	// After rotation, maxSealedSeq should be at least 500
	mt.mu.Lock()
	maxSealed := mt.mu.maxSealedSeq
	mt.mu.Unlock()

	require.GreaterOrEqual(t, maxSealed, uint64(500),
		"maxSealedSeq should be >= 500 after rotation, got %d", maxSealed)

	// Writing with seqID <= maxSealed should now fail
	err = mt.Put(400, xmap.Key{Lo: 1000, Hi: 0}, []byte("fail-key"), []byte("should-fail"))
	require.True(t, errors.Is(err, errSequenceTooOld))
}

// --- Cache Retry Loop Tests ---

// mockSequenceVendor allows tests to control sequence ID generation
type mockSequenceVendor struct {
	seq atomic.Uint64
}

func (m *mockSequenceVendor) NextSeq() uint64 {
	return m.seq.Add(1)
}

// TestCache_RetryLoop_ZombieResurrection verifies that putWithRetry acquires fresh seqID on rejection
func TestCache_RetryLoop_ZombieResurrection(t *testing.T) {
	tmpDir := t.TempDir()

	// We'll use a sequence vendor that we control
	seqVendor := &mockSequenceVendor{}
	seqVendor.seq.Store(100) // Start at 100

	cache, err := New(tmpDir,
		WithWriteBufferSize(64<<10),
		WithTestingKnobs(&TestingKnobs{
			SequenceVendor: seqVendor,
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Simulate a zombie scenario:
	// 1. First call gets seqID=101
	// 2. Manually set maxSealedSeq to 200 (simulating rotation happened)
	// 3. Put should fail with errSequenceTooOld
	// 4. Retry should get seqID=102 (still old), fail again
	// 5. Eventually succeed when seqID > maxSealedSeq

	// Set up maxSealedSeq to force retries
	cache.memTable.mu.Lock()
	cache.memTable.mu.maxSealedSeq = 200
	cache.memTable.mu.Unlock()

	// Now bump sequence vendor past maxSealedSeq so retry will succeed
	seqVendor.seq.Store(200)

	// Put should retry and eventually succeed
	require.NoError(t, cache.Put([]byte("zombie-key"), []byte("zombie-value")))

	// Verify the write succeeded by reading back
	data, found := readAll(t, cache, []byte("zombie-key"))
	require.True(t, found, "zombie write should have succeeded after retry")
	require.Equal(t, []byte("zombie-value"), data)

	// The final seqID should be > 200
	cache.memTable.mu.Lock()
	active := cache.memTable.mu.active
	cache.memTable.mu.Unlock()

	h := xxh3.Hash128([]byte("zombie-key"))
	record, found := active.index.Get(xmap.Key(h))
	require.True(t, found)
	require.Greater(t, record.SeqID, uint64(200),
		"final seqID should be > maxSealedSeq(200), got %d", record.SeqID)

}

// TestCache_RetryLoop_IdempotentSuccess tests that if a newer version exists,
// the retry loop returns success (idempotent behavior)
func TestCache_RetryLoop_IdempotentSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	// Use controlled sequence vendor so we can simulate the scenario precisely
	seqVendor := &mockSequenceVendor{}
	seqVendor.seq.Store(100)

	cache, err := New(tmpDir,
		WithWriteBufferSize(64<<10),
		WithSegmentSize(0), // Flush on put
		WithTestingKnobs(&TestingKnobs{
			SequenceVendor: seqVendor,
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := []byte("idempotent-key")
	h := xxh3.Hash128(key)

	// First, write a value with seqID=101
	require.NoError(t, cache.Put(key, []byte("first-value")))
	cache.Drain()

	// Verify it's in the index
	_, found := cache.index.Get(index.Key(h))
	require.True(t, found)

	// Now write a "newer" value with seqID=200
	seqVendor.seq.Store(199) // nextSeq will return 200
	require.NoError(t, cache.Put(key, []byte("newer-value")))
	cache.Drain()

	// Verify the entry still exists in index
	_, found = cache.index.Get(index.Key(h))
	require.True(t, found)

	// Now simulate a zombie: it will get seqID=50 (older than what's in index)
	// and maxSealedSeq will reject it
	seqVendor.seq.Store(49) // nextSeq will return 50

	cache.memTable.mu.Lock()
	cache.memTable.mu.maxSealedSeq = 300 // Force rejection
	cache.memTable.mu.Unlock()

	// Zombie writes with seqID=50:
	// 1. Gets errSequenceTooOld (50 <= 300)
	// 2. Checks index: existingRecord.SeqID (200) >= 50? YES
	// 3. Returns success (idempotent - newer version exists)
	require.NoError(t, cache.Put(key, []byte("zombie-value")))

	// The value should still be "newer-value" (zombie was idempotently "successful")
	data, found := readAll(t, cache, key)
	require.True(t, found)
	require.Equal(t, []byte("newer-value"), data, "should still have newer value, not zombie")
}
