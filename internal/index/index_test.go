package index

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	Put(Key, Item)
}

func makeTestKey(i int) Key {
	var k Key
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	h := xxh3.Hash128(b)
	binary.LittleEndian.PutUint64(k[0:8], h.Lo)
	binary.LittleEndian.PutUint64(k[8:16], h.Hi)
	return k
}

func makeItem(segmentID uint32, physLen uint32) Item {
	return Item{
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

func (idx *oldIndex) Put(key Key, item Item) {
	k64 := binary.LittleEndian.Uint64(key[:8])

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
	k64 := binary.LittleEndian.Uint64(key[:8])

	if n, ok := idx.blobs.Load(k64); ok {
		return n.entry, true
	}
	return Item{}, false
}

// -----------------------------------------------------------------------------
// Functional Correctness Tests
// -----------------------------------------------------------------------------

func TestBasicCRUD(t *testing.T) {
	idx := New(1024)
	k1 := makeTestKey(1)
	item1 := makeItem(1, 50)
	item1.Offset = 100

	// Put & Get
	idx.Put(k1, item1)
	got, found := idx.Get(k1)
	require.True(t, found, "Key should be found after Put")
	require.Equal(t, item1.SegmentID, got.SegmentID)
	require.Equal(t, item1.Offset, got.Offset)

	// Update
	item2 := makeItem(2, 60)
	item2.Offset = 200
	idx.Put(k1, item2)
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
	idx := New(count)

	// Fill
	for i := 0; i < count; i++ {
		idx.Put(makeTestKey(i), makeItem(uint32(i), 10))
	}

	getArenaSize := func() int {
		total := 0
		for i := 0; i < ShardCount; i++ {
			idx.shards[i].mu.RLock()
			total += len(idx.shards[i].nodes)
			idx.shards[i].mu.RUnlock()
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
		idx.Put(makeTestKey(i), makeItem(uint32(i), 20))
	}

	finalSize := getArenaSize()
	t.Logf("Arena Nodes: Initial=%d, Final=%d", initialSize, finalSize)

	// Allow minor growth (<5%) due to hash distribution variance
	require.LessOrEqual(t, finalSize, initialSize+(count/20),
		"Arena grew significantly despite reuse")
}

func TestEviction_ClockLogic(t *testing.T) {
	idx := New(100)

	// Find 3 keys that hash to Shard 0 for deterministic testing
	var keys []Key
	for i := 0; len(keys) < 3; i++ {
		k := makeTestKey(i)
		if k[0] == 0 {
			keys = append(keys, k)
		}
	}
	A, B, C := keys[0], keys[1], keys[2]

	// Insert A, B, C (Initially Cold)
	// After inserts: list is C(head) <-> B <-> A <-> C (circular)
	// hand stays at A (first insert)
	idx.Put(A, makeItem(1, 100))
	idx.Put(B, makeItem(2, 100))
	idx.Put(C, makeItem(3, 100))

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
	idx := New(1000)
	count := 100
	var itemSize uint32 = 1024

	// Insert 100KB
	for i := 0; i < count; i++ {
		idx.Put(makeTestKey(i), makeItem(uint32(i), itemSize))
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
	idx := New(100)
	require.Equal(t, 0, idx.Len())

	for i := 0; i < 50; i++ {
		idx.Put(makeTestKey(i), makeItem(uint32(i), 100))
	}
	require.Equal(t, 50, idx.Len())

	idx.Delete(makeTestKey(0))
	require.Equal(t, 49, idx.Len())
}

func TestStats(t *testing.T) {
	idx := New(100)
	for i := 0; i < 100; i++ {
		idx.Put(makeTestKey(i), makeItem(uint32(i), 100))
	}

	stats := idx.Stats()
	require.Equal(t, 100, stats.Items)
	require.Equal(t, ShardCount, stats.Shards)
	require.GreaterOrEqual(t, stats.ArenaNodes, 100)
	require.Greater(t, stats.MemoryEst, int64(0))
}

// -----------------------------------------------------------------------------
// Concurrency & Stress Tests
// -----------------------------------------------------------------------------

func TestConcurrency_Correctness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	idx := New(10_000)
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
				idx.Put(k, makeItem(uint32(id), 100))
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
					idx.Put(k, makeItem(uint32(offset+i), 100))
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

		idx := New(numItems)

		var wg sync.WaitGroup
		workers := 16
		chunk := numItems / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(offset int) {
				defer wg.Done()
				for i := 0; i < chunk; i++ {
					k := makeTestKey(offset + i)
					idx.Put(k, makeItem(uint32(offset+i), 100))
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
		idx.Put(makeTestKey(i), makeItem(0, 100))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := makeTestKey(r.Intn(keySpace))
			if r.Float32() < 0.90 {
				idx.Get(k)
			} else {
				idx.Put(k, makeItem(0, 100))
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
		benchThroughput(b, New(1_000_000), 1_000_000)
	})

	// Scenario 2: High Contention (1K keys)
	// Hot shard stress test, tests mutex degradation vs lock-free
	b.Run("Contention_1K/Old_SkipMap", func(b *testing.B) {
		benchThroughput(b, newOldIndex(), 1_000)
	})
	b.Run("Contention_1K/New_Arena", func(b *testing.B) {
		benchThroughput(b, New(1_000), 1_000)
	})
}
