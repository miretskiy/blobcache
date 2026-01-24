package index

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
	"github.com/zhangyunhao116/skipmap"
)

// -----------------------------------------------------------------------------
// Helpers & Test Infrastructure
// -----------------------------------------------------------------------------

// Indexer is the common interface for benchmarking old vs new implementations.
type Indexer interface {
	Get(Key) (Item, bool)
	Put(Item)
}

func makeTestKey(i int) Key {
	return xxh3.Hash128(fmt.Appendf(nil, "key-%d", i))
}

func makeItem(key Key, segmentID uint32, physLen uint32) Item {
	return Item{
		Key:         key,
		SegmentID:   segmentID,
		PhysicalLen: physLen,
	}
}

// -----------------------------------------------------------------------------
// Reference Implementation (Production Baseline)
// -----------------------------------------------------------------------------
// This simulates the exact architecture being replaced:
// Generic SkipMap + sync.Pool + Linked List Pointers.

type oldNode struct {
	key     uint64
	entry   Item // Same entry type for fair comparison
	visited bool

	// The GC Killers: Pointers that must be traversed
	next *oldNode
	prev *oldNode
}

type oldIndex struct {
	blobs  *skipmap.Uint64Map[*oldNode]
	pool   sync.Pool
	listMu sync.Mutex
	head   *oldNode
}

func newOldIndex() *oldIndex {
	return &oldIndex{
		blobs: skipmap.NewUint64[*oldNode](),
		pool: sync.Pool{
			New: func() any { return &oldNode{} },
		},
	}
}

func (idx *oldIndex) Put(item Item) {
	k64 := item.Key.Lo

	if n, ok := idx.blobs.Load(k64); ok {
		n.entry = item
		n.visited = true
		return
	}

	n := idx.pool.Get().(*oldNode)
	n.key = k64
	n.entry = item
	n.visited = false

	idx.blobs.Store(k64, n)

	idx.listMu.Lock()
	n.next = idx.head
	if idx.head != nil {
		idx.head.prev = n
	}
	idx.head = n
	idx.listMu.Unlock()
}

func (idx *oldIndex) Get(key Key) (Item, bool) {
	k64 := key.Lo

	if n, ok := idx.blobs.Load(k64); ok {
		return n.entry, true
	}
	return Item{}, false
}

// -----------------------------------------------------------------------------
// Alignment Tests (Validates struct layout at test time, not runtime)
// -----------------------------------------------------------------------------

func TestIndex_Alignment(t *testing.T) {
	// This guarantees the layout is correct forever.
	// If someone messes up ShardState padding, 'go test' fails immediately.
	if err := xmap.VerifyAlignment[uint32, ShardState](); err != nil {
		t.Fatal(err)
	}
}

// -----------------------------------------------------------------------------
// Functional Correctness Tests
// -----------------------------------------------------------------------------

func TestBasicCRUD(t *testing.T) {
	idx := NewBlobIndex(1024)
	k1 := makeTestKey(1)
	item1 := makeItem(k1, 1, 50)
	item1.Offset = 100

	// Put & Get
	idx.Put(item1)
	got, found := idx.Get(k1)
	require.True(t, found, "Key should be found after Put")
	require.Equal(t, item1.SegmentID, got.SegmentID)
	require.Equal(t, item1.Offset, got.Offset)

	// Update
	item2 := makeItem(k1, 2, 60)
	item2.Offset = 200
	idx.Put(item2)
	got, _ = idx.Get(k1)
	require.Equal(t, uint32(2), got.SegmentID, "Update failed: SegmentID should change")

	// Delete
	require.True(t, idx.Delete(k1), "Delete should return true for existing key")
	_, found = idx.Get(k1)
	require.False(t, found, "Key should not be found after Delete")
}

func TestArenaReuse(t *testing.T) {
	// Verifies the "Zero-Allocation" property in steady state.
	count := 10_000
	idx := NewBlobIndex(count)

	// Fill
	for i := 0; i < count; i++ {
		k := makeTestKey(i)
		idx.Put(makeItem(k, uint32(i), 10))
	}

	getArenaSize := func() int {
		total := 0
		for i := 0; i < ShardCount; i++ {
			s := idx.ShardAt(i)
			s.RLock()
			total += len(s.Extra.nodes)
			s.RUnlock()
		}
		return total
	}

	initialSize := getArenaSize()

	// Delete All
	for i := 0; i < count; i++ {
		idx.Delete(makeTestKey(i))
	}

	// Refill
	for i := 0; i < count; i++ {
		k := makeTestKey(i)
		idx.Put(makeItem(k, uint32(i), 20))
	}

	finalSize := getArenaSize()
	t.Logf("Arena Nodes: Initial=%d, Final=%d", initialSize, finalSize)

	// Allow minor growth (<5%) due to hash distribution variance
	require.LessOrEqual(t, finalSize, initialSize+(count/20),
		"Arena grew significantly despite reuse")
}

func TestEviction_ClockLogic(t *testing.T) {
	idx := NewBlobIndex(100)

	// Find 3 keys that hash to Shard 0 for deterministic testing
	// With 256 shards, shard 0 is k.Lo & 0xFF == 0
	var keys []Key
	for i := 0; len(keys) < 3; i++ {
		k := makeTestKey(i)
		if k.Lo&0xFF == 0 {
			keys = append(keys, k)
		}
	}
	A, B, C := keys[0], keys[1], keys[2]

	// Insert A, B, C (Initially Cold)
	// After inserts: list is C(head) <-> B <-> A <-> C (circular)
	// hand stays at A (first insert)
	idx.Put(makeItem(A, 1, 100))
	idx.Put(makeItem(B, 2, 100))
	idx.Put(makeItem(C, 3, 100))

	// Access A (Mark Hot)
	idx.Get(A)

	// Evict 100 bytes
	// Hand starts at A, A is hot -> clear visited, advance to A.next (which is C in circular list)
	// C is cold -> evict C
	evicted := idx.EvictBatch(100)

	require.Len(t, evicted, 1, "Should evict exactly 1 item")
	require.Equal(t, uint32(3), evicted[0].SegmentID, "Should evict C (SegmentID 3)")

	_, hasA := idx.Get(A)
	_, hasC := idx.Get(C)
	require.True(t, hasA, "Item A (Hot) should not be evicted")
	require.False(t, hasC, "Item C (Cold) should be evicted")
}

func TestEviction_SizeBased(t *testing.T) {
	idx := NewBlobIndex(1000)
	count := 100
	var itemSize uint32 = 1024

	// Insert 100KB
	for i := 0; i < count; i++ {
		k := makeTestKey(i)
		idx.Put(makeItem(k, uint32(i), itemSize))
	}

	// Request eviction of 50KB
	target := int64(50 * 1024)
	evicted := idx.EvictBatch(target)

	var totalFreed int64
	for _, it := range evicted {
		totalFreed += int64(it.PhysicalLen)
	}

	require.GreaterOrEqual(t, totalFreed, target, "Evicted too little")
	require.LessOrEqual(t, totalFreed, target+int64(itemSize)*20, "Evicted way too much")
}

func TestLen(t *testing.T) {
	idx := NewBlobIndex(100)
	require.Equal(t, 0, idx.NumItems())

	for i := 0; i < 50; i++ {
		k := makeTestKey(i)
		idx.Put(makeItem(k, uint32(i), 100))
	}
	require.Equal(t, 50, idx.NumItems())

	idx.Delete(makeTestKey(0))
	require.Equal(t, 49, idx.NumItems())
}

func TestStats(t *testing.T) {
	idx := NewBlobIndex(100)
	for i := 0; i < 100; i++ {
		k := makeTestKey(i)
		idx.Put(makeItem(k, uint32(i), 100))
	}

	stats := idx.Stats()
	require.Equal(t, 100, stats.Items)
	require.Equal(t, ShardCount, stats.Shards)
	require.GreaterOrEqual(t, stats.ArenaNodes, 100)
	require.Greater(t, stats.MemoryEst, int64(0))
}

// -----------------------------------------------------------------------------
// Relocate Tests (Compare-And-Swap for Compaction)
// -----------------------------------------------------------------------------

func TestRelocate_Success(t *testing.T) {
	idx := NewBlobIndex(100)
	k := makeTestKey(1)

	// Insert item at segment 10, offset 100
	item := makeItem(k, 10, 500)
	item.Offset = 100
	idx.Put(item)

	// Relocate from (10, 100) to (20, 200)
	ok := idx.Relocate(k, 10, 20, 100, 200, RelocateLive)
	require.True(t, ok, "Relocate should succeed when location matches")

	// Verify the item was updated
	got, found := idx.Get(k)
	require.True(t, found)
	require.Equal(t, uint32(20), got.SegmentID)
	require.Equal(t, uint32(200), got.Offset)
	require.Equal(t, uint32(500), got.PhysicalLen, "PhysicalLen should be preserved")
}

func TestRelocate_FailSegmentMismatch(t *testing.T) {
	idx := NewBlobIndex(100)
	k := makeTestKey(1)

	// Insert item at segment 10, offset 100
	item := makeItem(k, 10, 500)
	item.Offset = 100
	idx.Put(item)

	// Try to relocate from wrong segment (15, 100) - should fail
	ok := idx.Relocate(k, 15, 20, 100, 200, RelocateLive)
	require.False(t, ok, "Relocate should fail when segment doesn't match")

	// Verify the item was NOT updated
	got, _ := idx.Get(k)
	require.Equal(t, uint32(10), got.SegmentID)
	require.Equal(t, uint32(100), got.Offset)
}

func TestRelocate_FailOffsetMismatch(t *testing.T) {
	idx := NewBlobIndex(100)
	k := makeTestKey(1)

	// Insert item at segment 10, offset 100
	item := makeItem(k, 10, 500)
	item.Offset = 100
	idx.Put(item)

	// Try to relocate from wrong offset (10, 150) - should fail
	ok := idx.Relocate(k, 10, 20, 150, 200, RelocateLive)
	require.False(t, ok, "Relocate should fail when offset doesn't match")

	// Verify the item was NOT updated
	got, _ := idx.Get(k)
	require.Equal(t, uint32(10), got.SegmentID)
	require.Equal(t, uint32(100), got.Offset)
}

func TestRelocate_FailKeyNotFound(t *testing.T) {
	idx := NewBlobIndex(100)
	k := makeTestKey(1)

	// Try to relocate non-existent key
	ok := idx.Relocate(k, 10, 20, 100, 200, RelocateLive)
	require.False(t, ok, "Relocate should fail when key doesn't exist")
}

func TestRelocate_ConcurrentRace(t *testing.T) {
	// Tests the scenario where compaction reads an item, but a concurrent write
	// updates it before compaction can relocate. Only one should win.
	idx := NewBlobIndex(100)
	k := makeTestKey(1)

	// Insert item at segment 10, offset 100
	item := makeItem(k, 10, 500)
	item.Offset = 100
	idx.Put(item)

	var wg sync.WaitGroup
	var relocateWon, putWon atomic.Bool

	// Goroutine 1: Compaction tries to relocate from (10, 100) to (20, 200)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if idx.Relocate(k, 10, 20, 100, 200, RelocateLive) {
			relocateWon.Store(true)
		}
	}()

	// Goroutine 2: Concurrent write updates to (30, 300)
	wg.Add(1)
	go func() {
		defer wg.Done()
		newItem := makeItem(k, 30, 600)
		newItem.Offset = 300
		idx.Put(newItem)
		putWon.Store(true) // Put always "wins" in terms of completing
	}()

	wg.Wait()

	// Check final state
	got, _ := idx.Get(k)

	// One of these outcomes must be true:
	// 1. Relocate ran first: item at (20, 200), then Put updated to (30, 300) => final (30, 300)
	// 2. Put ran first: item at (30, 300), then Relocate fails (location mismatch) => final (30, 300)
	// 3. Relocate ran first and Put hasn't completed: item at (20, 200)
	//
	// The key invariant: we never lose the Put's update (no "Leapfrog Hazard")
	if relocateWon.Load() {
		// Relocate succeeded, so item was at (10, 100) when relocate ran
		// Final state depends on Put timing
		t.Logf("Relocate won, final state: seg=%d, off=%d", got.SegmentID, got.Offset)
	} else {
		// Relocate failed because Put ran first and changed the location
		require.Equal(t, uint32(30), got.SegmentID, "Put should have won")
		require.Equal(t, uint32(300), got.Offset)
		t.Log("Put won - Relocate correctly detected location change")
	}
}

func TestRelocate_PreservesFlags(t *testing.T) {
	idx := NewBlobIndex(100)
	k := makeTestKey(1)

	// Insert item with errno flag set (but not deleted)
	item := makeItem(k, 10, 500)
	item.Offset = 100
	item.SetErrno(5)
	idx.Put(item)

	// Relocate should succeed (item is live, just has error flag)
	ok := idx.Relocate(k, 10, 20, 100, 200, RelocateLive)
	require.True(t, ok, "Relocate should succeed for live item with errno")

	// Verify flags are preserved and location updated
	got, _ := idx.Get(k)
	require.False(t, got.IsDeleted(), "Should not be deleted")
	require.Equal(t, item.Errno(), got.Errno(), "Errno should be preserved")
	require.Equal(t, uint32(20), got.SegmentID, "SegmentID should be updated")
	require.Equal(t, uint32(200), got.Offset, "Offset should be updated")
}

func TestRelocate_GhostGuard(t *testing.T) {
	// This test validates the "Ghost Guard" fix that prevents the
	// "Ghost Resurrection" bug where a concurrent delete during compaction
	// could be overwritten by compaction's stale live data.
	//
	// Scenario:
	// 1. Compactor reads segment, sees key K as live at (seg=10, off=100)
	// 2. User deletes K → tombstone marked in RAM
	// 3. Compactor writes K to new segment at (seg=50, off=200)
	// 4. Compactor calls Relocate(K, 10, 100, 50, 200)
	// 5. WITHOUT fix: Relocate succeeds, zombie K reappears as live
	// 6. WITH fix: Relocate fails, K stays deleted

	idx := NewBlobIndex(100)
	key := makeTestKey(999)

	// T0: Initial state - live item at old location
	item := makeItem(key, 10, 1000)
	item.Offset = 100
	idx.Put(item)

	// Verify initial state
	got, ok := idx.Get(key)
	require.True(t, ok)
	require.False(t, got.IsDeleted(), "initially should be live")
	require.Equal(t, uint32(10), got.SegmentID)
	require.Equal(t, uint32(100), got.Offset)

	// T1: Concurrent delete (marks tombstone in RAM)
	// This simulates user calling Delete() while compaction is in flight
	s := idx.Shard(key)
	s.Lock()
	i := s.Items[key]
	s.Extra.nodes[i].item.SetDeleted()
	s.Unlock()

	// T2: Verify item is now marked deleted
	got, ok = idx.Get(key)
	require.True(t, ok)
	require.True(t, got.IsDeleted(), "should be marked deleted")

	// T3: Compactor attempts relocation (has stale view of item as live)
	// This should FAIL because item is now deleted
	relocated := idx.Relocate(key, 10, 50, 100, 200, RelocateLive)
	require.False(t, relocated, "should not relocate deleted item (Ghost Guard)")

	// T4: Verify item remains deleted with OLD location
	// (compaction should skip this item entirely)
	got, ok = idx.Get(key)
	require.True(t, ok)
	require.True(t, got.IsDeleted(), "should still be deleted")
	require.Equal(t, uint32(10), got.SegmentID, "location should not change")
	require.Equal(t, uint32(100), got.Offset, "location should not change")
}

func TestRelocate_TombstoneMigration(t *testing.T) {
	// This test validates that tombstones CAN be relocated during compaction
	// when using expectDeleted=true.
	//
	// Scenario:
	// 1. Item exists at (seg=10, off=100)
	// 2. Item is deleted (tombstone)
	// 3. Compaction wants to move tombstone to new segment (seg=50, off=0)
	// 4. Relocate with expectDeleted=true should succeed

	idx := NewBlobIndex(100)
	key := makeTestKey(888)

	// Initial state - live item
	item := makeItem(key, 10, 1000)
	item.Offset = 100
	idx.Put(item)

	// Delete the item (mark tombstone)
	s := idx.Shard(key)
	s.Lock()
	i := s.Items[key]
	s.Extra.nodes[i].item.SetDeleted()
	s.Unlock()

	// Verify tombstone state
	got, ok := idx.Get(key)
	require.True(t, ok)
	require.True(t, got.IsDeleted())
	require.Equal(t, uint32(10), got.SegmentID)
	require.Equal(t, uint32(100), got.Offset)

	// Relocate with RelocateLive should FAIL (Ghost Guard)
	relocatedLive := idx.Relocate(key, 10, 50, 100, 0, RelocateLive)
	require.False(t, relocatedLive, "should not relocate tombstone as live item")

	// Relocate with RelocateTombstone should SUCCEED (tombstone migration)
	relocatedTombstone := idx.Relocate(key, 10, 50, 100, 0, RelocateTombstone)
	require.True(t, relocatedTombstone, "should relocate tombstone with RelocateTombstone")

	// Verify tombstone was moved
	got, ok = idx.Get(key)
	require.True(t, ok)
	require.True(t, got.IsDeleted(), "should still be deleted")
	require.Equal(t, uint32(50), got.SegmentID, "segment should be updated")
	require.Equal(t, uint32(0), got.Offset, "offset should be updated")
}

func TestRelocate_TombstoneMigration_RaceDetection(t *testing.T) {
	// Tests that relocating a tombstone fails if the item was un-deleted
	// (race with a new Put) during compaction.

	idx := NewBlobIndex(100)
	key := makeTestKey(777)

	// Initial state - deleted item (tombstone)
	item := makeItem(key, 10, 1000)
	item.Offset = 100
	item.SetDeleted()
	idx.Put(item)

	// Verify tombstone state
	got, ok := idx.Get(key)
	require.True(t, ok)
	require.True(t, got.IsDeleted())

	// Simulate concurrent Put that overwrites tombstone with new live item
	s := idx.Shard(key)
	s.Lock()
	i := s.Items[key]
	s.Extra.nodes[i].item.Flags = 0          // Clear deleted flag
	s.Extra.nodes[i].item.PhysicalLen = 2000 // New data
	s.Extra.nodes[i].item.SegmentID = 20     // New segment
	s.Extra.nodes[i].item.Offset = 500       // New offset
	s.Unlock()

	// Tombstone relocation should FAIL because item is now live
	relocated := idx.Relocate(key, 10, 50, 100, 0, RelocateTombstone)
	require.False(t, relocated, "should fail: item is no longer deleted")

	// Also fails because location doesn't match
	relocated = idx.Relocate(key, 20, 50, 500, 0, RelocateTombstone)
	require.False(t, relocated, "should fail: item is live, not tombstone")
}

// -----------------------------------------------------------------------------
// Concurrency & Stress Tests
// -----------------------------------------------------------------------------

func TestConcurrency_Correctness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	idx := NewBlobIndex(10_000)
	var wg sync.WaitGroup

	const (
		numWriters = 8
		numReaders = 16
		numOps     = 50_000
		keySpace   = 2_000
	)

	var opsCount atomic.Uint64

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id)))
			for opsCount.Add(1) <= numOps {
				k := makeTestKey(r.Intn(keySpace))
				idx.Put(makeItem(k, uint32(id), 100))
				if r.Intn(100) == 0 {
					runtime.Gosched()
				}
			}
		}(w)
	}

	// Readers
	for w := 0; w < numReaders; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id + 100)))
			for opsCount.Load() <= numOps {
				k := makeTestKey(r.Intn(keySpace))
				idx.Get(k)
				if r.Float32() < 0.05 {
					idx.Delete(k)
				}
			}
		}(w)
	}

	// Evictor
	wg.Add(1)
	go func() {
		defer wg.Done()
		for opsCount.Load() <= numOps {
			idx.EvictBatch(4096)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
}

// -----------------------------------------------------------------------------
// Benchmarks: GC Impact ("The Honest Benchmark")
// -----------------------------------------------------------------------------

func BenchmarkGC_Comparison(b *testing.B) {
	const numItems = 5_000_000

	b.Run("Old_SkipMap_Generics", func(b *testing.B) {
		var m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		idx := newOldIndex()

		var wg sync.WaitGroup
		workers := 16
		chunk := numItems / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(offset int) {
				defer wg.Done()
				for i := 0; i < chunk; i++ {
					k := makeTestKey(offset + i)
					idx.Put(makeItem(k, uint32(offset+i), 100))
				}
			}(w * chunk)
		}
		wg.Wait()

		var m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m2)

		b.ReportMetric(float64(m2.HeapAlloc-m1.HeapAlloc)/1024/1024, "MB_heap")
		b.ReportMetric(float64(m2.HeapObjects-m1.HeapObjects), "heap_objects")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}

		// Keep alive
		idx.Get(makeTestKey(0))
	})

	b.Run("New_Arena_Flat", func(b *testing.B) {
		var m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		idx := NewBlobIndex(numItems)

		var wg sync.WaitGroup
		workers := 16
		chunk := numItems / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(offset int) {
				defer wg.Done()
				for i := 0; i < chunk; i++ {
					k := makeTestKey(offset + i)
					idx.Put(makeItem(k, uint32(offset+i), 100))
				}
			}(w * chunk)
		}
		wg.Wait()

		var m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m2)

		b.ReportMetric(float64(m2.HeapAlloc-m1.HeapAlloc)/1024/1024, "MB_heap")
		b.ReportMetric(float64(m2.HeapObjects-m1.HeapObjects), "heap_objects")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}

		// Keep alive
		idx.Get(makeTestKey(0))
	})
}

// -----------------------------------------------------------------------------
// Benchmarks: Throughput
// -----------------------------------------------------------------------------

// benchThroughput is a shared benchmark runner for different index implementations.
func benchThroughput(b *testing.B, idx Indexer, keySpace int) {
	b.Helper()

	// Pre-populate
	for i := 0; i < keySpace; i++ {
		k := makeTestKey(i)
		idx.Put(makeItem(k, 0, 100))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := makeTestKey(r.Intn(keySpace))
			if r.Float32() < 0.90 {
				idx.Get(k)
			} else {
				idx.Put(makeItem(k, 0, 100))
			}
		}
	})
}

func BenchmarkThroughput(b *testing.B) {
	// Scenario 1: Standard Spread (1M keys)
	// Low collision, sharded locks should excel
	b.Run("Spread_1M/Old_SkipMap", func(b *testing.B) {
		benchThroughput(b, newOldIndex(), 1_000_000)
	})
	b.Run("Spread_1M/New_Arena", func(b *testing.B) {
		benchThroughput(b, NewBlobIndex(1_000_000), 1_000_000)
	})

	// Scenario 2: High Contention (1K keys)
	// Hot shard stress test, tests mutex degradation vs lock-free
	b.Run("Contention_1K/Old_SkipMap", func(b *testing.B) {
		benchThroughput(b, newOldIndex(), 1_000)
	})
	b.Run("Contention_1K/New_Arena", func(b *testing.B) {
		benchThroughput(b, NewBlobIndex(1_000), 1_000)
	})
}
