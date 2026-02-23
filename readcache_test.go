package blobcache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// makeRawRecord builds a raw on-disk record from key and value bytes.
func makeRawRecord(t *testing.T, key, value []byte) []byte {
	t.Helper()
	rec := record.Record{
		Header: record.Header{
			Magic:        record.RecordMagic,
			KeyLen:       uint16(len(key)),
			PhysicalSize: int64(len(value)),
			LogicalSize:  int64(len(value)),
		},
		Key:   key,
		Value: value,
	}
	// Compute and set HeaderCRC.
	buf := make([]byte, rec.EncodedSize())
	rec.EncodeTo(buf)
	return buf
}

func hashKey(key []byte) Key {
	return xxh3.Hash128(key)
}

// noopReporter satisfies ErrorReporter for tests.
type noopReporter struct{}

func (noopReporter) IsDegraded() bool                        { return false }
func (noopReporter) ReportError(error)                       {}
func (noopReporter) ReportBlobError(_ Key, _ base.BlobErrno) {}

// newTestReadCache creates a ReadCache for testing with the given slab size and count.
// The caller should defer rc.Close(). Do NOT close the pool separately —
// ReadCache.Close() handles it.
func newTestReadCache(slabSize int64, maxSlabs int) *ReadCache {
	return NewReadCache(slabSize, maxSlabs, 0, noopReporter{})
}

// --- ReadCache Core ---

func TestReadCache_InsertAndAcquire(t *testing.T) {
	rc := newTestReadCache(1<<20, 4) // 1MB slabs, 4 total
	defer rc.Close()

	key := []byte("test-key-1")
	value := []byte("test-value-1-data")
	rawRec := makeRawRecord(t, key, value)
	h := hashKey(key)

	// Insert into read cache.
	ok := rc.Insert(h, rawRec)
	require.True(t, ok, "Insert should succeed")

	// Acquire from read cache.
	data, storedKey, rel, found := rc.Acquire(h)
	require.True(t, found, "Acquire should find the key")
	require.Equal(t, key, storedKey)
	require.Equal(t, value, data)
	rel.Release()
}

func TestReadCache_AcquireNotFound(t *testing.T) {
	rc := newTestReadCache(1<<20, 4)
	defer rc.Close()

	h := hashKey([]byte("nonexistent"))
	_, _, _, found := rc.Acquire(h)
	require.False(t, found)
}

func TestReadCache_SlabRotation(t *testing.T) {
	// Use tiny slabs (4KB) to force rotation quickly.
	// Use enough slabs (10) so eviction doesn't kick in during the test —
	// we're testing rotation, not eviction.
	rc := newTestReadCache(4096, 10)
	defer rc.Close()

	// Insert records until at least one rotation occurs.
	var insertedKeys []Key
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := make([]byte, 200) // ~242 bytes per record with header
		rawRec := makeRawRecord(t, key, value)
		h := hashKey(key)

		if !rc.Insert(h, rawRec) {
			break
		}
		insertedKeys = append(insertedKeys, h)
	}

	// Verify sealed list has entries (rotation happened).
	sealedList := *rc.lib.view.Load()
	require.Greater(t, len(sealedList), 0, "should have at least one sealed slab")

	// All inserted keys should be findable (in sealed or active).
	for _, h := range insertedKeys {
		_, _, rel, found := rc.Acquire(h)
		require.True(t, found, "key should be found after rotation")
		rel.Release()
	}
}

func TestReadCache_Eviction_FIFO(t *testing.T) {
	// 3 slabs total: 1 active + 2 sealed. When sealed list hits 2,
	// the oldest (FIFO) slab is evicted on next rotation.
	rc := newTestReadCache(4096, 3)
	defer rc.Close()

	// Fill and seal slabs by inserting many records.
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("evict-key-%04d", i))
		value := make([]byte, 200)
		rawRec := makeRawRecord(t, key, value)
		h := hashKey(key)
		rc.Insert(h, rawRec)
	}

	// Verify sealed list doesn't exceed maxSlabs-1.
	sealedList := *rc.lib.view.Load()
	require.LessOrEqual(t, len(sealedList), 2,
		"sealed list should not exceed maxSlabs-1 (2), got %d", len(sealedList))

	// Evictions should have occurred.
	stats := rc.Stats()
	require.Greater(t, stats.Evictions, int64(0), "should have evicted at least one slab")
}

func TestReadCache_Invalidate(t *testing.T) {
	rc := newTestReadCache(1<<20, 4)
	defer rc.Close()

	key := []byte("invalidate-me")
	value := []byte("value")
	rawRec := makeRawRecord(t, key, value)
	h := hashKey(key)

	rc.Insert(h, rawRec)

	// Verify it's there.
	_, _, rel, found := rc.Acquire(h)
	require.True(t, found)
	rel.Release()

	// Invalidate.
	rc.Invalidate(h)

	// Should be gone.
	_, _, _, found = rc.Acquire(h)
	require.False(t, found, "key should be gone after Invalidate")
}

func TestReadCache_LargeRecordSkipped(t *testing.T) {
	rc := newTestReadCache(1024, 2)
	defer rc.Close()

	key := []byte("big-key")
	value := make([]byte, 2000) // Exceeds 1024-byte slab
	rawRec := makeRawRecord(t, key, value)
	h := hashKey(key)

	ok := rc.Insert(h, rawRec)
	require.False(t, ok, "record larger than slab should be rejected")
}

func TestReadCache_Close_Idempotent(t *testing.T) {
	rc := newTestReadCache(1<<20, 2)

	// Close twice — should not panic.
	rc.Close()
	rc.Close()
}

func TestReadCache_Stats(t *testing.T) {
	rc := newTestReadCache(1<<20, 4)
	defer rc.Close()

	// Initially all zeros.
	s := rc.Stats()
	require.Zero(t, s.Hits)
	require.Zero(t, s.Misses)
	require.Zero(t, s.Inserts)
	require.Zero(t, s.Evictions)
	require.Zero(t, s.Slabs)

	// Insert a record.
	key := []byte("stats-key")
	value := []byte("stats-value")
	rawRec := makeRawRecord(t, key, value)
	h := hashKey(key)
	rc.Insert(h, rawRec)

	s = rc.Stats()
	require.Equal(t, int64(1), s.Inserts)

	// Acquire does NOT count hits — hits/misses are counted by Archivist.ReadBlob.
	_, _, rel, found := rc.Acquire(h)
	require.True(t, found)
	rel.Release()

	s = rc.Stats()
	require.Equal(t, int64(0), s.Hits, "Acquire alone should not count hits")

	// Miss on unknown key.
	_, _, _, found = rc.Acquire(hashKey([]byte("no-such-key")))
	require.False(t, found)
	// Misses are only counted by Archivist.ReadBlob, not Acquire.
	require.Equal(t, int64(0), s.Misses)
}

// --- inflightGroup ---

func TestInflightGroup_SingleFlight(t *testing.T) {
	g := newInflightGroup()
	var callCount atomic.Int32

	const numGoroutines = 100
	var ready sync.WaitGroup
	ready.Add(numGoroutines)
	gate := make(chan struct{})        // Holds all goroutines until everyone is spawned.
	leaderBlock := make(chan struct{}) // Holds the leader in fn() so others queue up.

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			<-gate
			g.DoOnce(42, func() {
				callCount.Add(1)
				<-leaderBlock // Hold flight open for others to join.
			})
		}()
	}

	ready.Wait() // All goroutines spawned and waiting at the gate.
	close(gate)  // Release them all at once.

	// Give goroutines time to enter DoOnce. The leader blocks in fn();
	// all others wait on the shard's Cond.
	time.Sleep(100 * time.Millisecond)

	close(leaderBlock) // Release the leader; all waiters unblock.
	wg.Wait()

	require.Equal(t, int32(1), callCount.Load(),
		"exactly 1 goroutine should execute fn, got %d", callCount.Load())
}

func TestInflightGroup_DifferentKeysRunConcurrently(t *testing.T) {
	g := newInflightGroup()
	var callCount atomic.Int32
	barrier := make(chan struct{})

	// Two different keys should both run their fn.
	var wg sync.WaitGroup
	for _, key := range []uint64{1, 2} {
		wg.Add(1)
		go func(k uint64) {
			defer wg.Done()
			g.DoOnce(k, func() {
				callCount.Add(1)
				<-barrier // Block until released
			})
		}(key)
	}

	// Wait a bit for both goroutines to enter fn, then release.
	// Both should be running concurrently.
	close(barrier)
	wg.Wait()

	require.Equal(t, int32(2), callCount.Load(),
		"different keys should run independently")
}

func TestInflightGroup_SequentialReuse(t *testing.T) {
	g := newInflightGroup()
	var callCount atomic.Int32

	// First call.
	g.DoOnce(42, func() { callCount.Add(1) })
	// After completion, same key should trigger a new call.
	g.DoOnce(42, func() { callCount.Add(1) })

	require.Equal(t, int32(2), callCount.Load(),
		"after first flight completes, same key should re-execute")
}

// --- flightKey ---

func TestFlightKey_Encoding(t *testing.T) {
	// Same segment + same aligned offset → same key.
	k1 := flightKey(100, 0)
	k2 := flightKey(100, 0)
	require.Equal(t, k1, k2)

	// Different segment → different key.
	k3 := flightKey(200, 0)
	require.NotEqual(t, k1, k3)

	// Different chunk → different key.
	k4 := flightKey(100, prefetchChunkSize)
	require.NotEqual(t, k1, k4)

	// Offsets within same chunk → same key.
	k5 := flightKey(100, 100)
	k6 := flightKey(100, 200)
	require.Equal(t, k5, k6, "offsets within same 64KB chunk should produce same key")
}
