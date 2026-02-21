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
	pool := NewMmapPool("test-rc", slabSize, maxSlabs)
	rc := &ReadCache{
		pool:        pool,
		slabSize:    slabSize,
		maxSlabs:    maxSlabs,
		flights:     newInflightGroup(),
		evictor:     DropStrategy{},
		errReporter: noopReporter{},
	}
	empty := make([]*ReadSlab, 0)
	rc.sealed.Store(&empty)
	rc.active = rc.newActiveSlab()
	return rc
}

// --- ReadSlab / ColdScore / RecordHit ---

func TestReadSlab_ColdScore_Empty(t *testing.T) {
	rs := &ReadSlab{totalItems: 0}
	require.Equal(t, 1.0, rs.ColdScore(), "empty slab should be fully cold")
}

func TestReadSlab_ColdScore_AllVisited(t *testing.T) {
	rs := &ReadSlab{totalItems: 160}
	// With visitSampleFactor=16, we need 160/16 = 10 sampled visits to cover all items.
	rs.visitedCount.Store(10)
	require.Equal(t, 0.0, rs.ColdScore(), "fully visited slab should be fully hot")
}

func TestReadSlab_ColdScore_HalfVisited(t *testing.T) {
	rs := &ReadSlab{totalItems: 320}
	// 320 items, 10 sampled visits → estimated 160 visits → 160/320 = 0.5 visited → 0.5 cold
	rs.visitedCount.Store(10)
	require.InDelta(t, 0.5, rs.ColdScore(), 0.001)
}

func TestReadSlab_ColdScore_OverVisited(t *testing.T) {
	rs := &ReadSlab{totalItems: 100}
	// More estimated visits (200) than items (100) → clamped to 0.0
	rs.visitedCount.Store(100) // 100 * 16 = 1600 >> 100
	require.Equal(t, 0.0, rs.ColdScore())
}

func TestSelectVictim_ColdestFirst(t *testing.T) {
	slabs := []*ReadSlab{
		{totalItems: 100, createdAt: 1},
		{totalItems: 100, createdAt: 2},
		{totalItems: 100, createdAt: 3},
	}
	// slab[0] has 5 sampled visits (est. 80 → cold=0.2)
	slabs[0].visitedCount.Store(5)
	// slab[1] has 0 visits → cold=1.0 (coldest!)
	// slab[2] has 3 sampled visits (est. 48 → cold=0.52)
	slabs[2].visitedCount.Store(3)

	victimIdx := selectVictim(slabs)
	require.Equal(t, 1, victimIdx, "slab[1] is coldest (score=1.0)")
}

func TestSelectVictim_Tiebreak_OldestWins(t *testing.T) {
	slabs := []*ReadSlab{
		{totalItems: 100, createdAt: 300},
		{totalItems: 100, createdAt: 100}, // Oldest
		{totalItems: 100, createdAt: 200},
	}
	// All have cold score = 1.0 (no visits)
	victimIdx := selectVictim(slabs)
	require.Equal(t, 1, victimIdx, "on tie, oldest slab (createdAt=100) should be evicted")
}

func TestSelectVictim_Empty(t *testing.T) {
	require.Equal(t, -1, selectVictim(nil))
}

func TestDecayVisitedCounts(t *testing.T) {
	slabs := []*ReadSlab{
		{totalItems: 100},
		{totalItems: 100},
	}
	slabs[0].visitedCount.Store(20)
	slabs[1].visitedCount.Store(10)

	decayVisitedCounts(slabs)

	require.Equal(t, int32(10), slabs[0].visitedCount.Load())
	require.Equal(t, int32(5), slabs[1].visitedCount.Load())
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
	// all others queue on the flight's done channel.
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
	sealedList := *rc.sealed.Load()
	require.Greater(t, len(sealedList), 0, "should have at least one sealed slab")

	// All inserted keys should be findable (in sealed or active).
	for _, h := range insertedKeys {
		_, _, rel, found := rc.Acquire(h)
		require.True(t, found, "key should be found after rotation")
		rel.Release()
	}
}

func TestReadCache_Eviction_ColdestSlab(t *testing.T) {
	// 3 slabs total. After sealing 2 + active = 3, the next seal triggers eviction.
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
	sealedList := *rc.sealed.Load()
	require.LessOrEqual(t, len(sealedList), rc.maxSlabs-1,
		"sealed list should not exceed maxSlabs-1 (%d), got %d", rc.maxSlabs-1, len(sealedList))
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

	// Acquire should count as hit.
	_, _, rel, found := rc.Acquire(h)
	require.True(t, found)
	rel.Release()

	s = rc.Stats()
	require.Equal(t, int64(1), s.Hits)

	// Miss on unknown key.
	_, _, _, found = rc.Acquire(hashKey([]byte("no-such-key")))
	require.False(t, found)
	// Misses are only counted in FetchAndPopulate, not Acquire.
	require.Equal(t, int64(0), s.Misses)
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
