// Package index provides a GC-optimized in-memory index for blob metadata.
//
// The index uses a sharded arena-backed design where maps store uint32 indices
// into pre-allocated slices, eliminating heap pointers entirely. This makes
// the data structures "scan-free" from the GC's perspective, dramatically
// reducing pause times at scale (millions of entries).
//
// Eviction uses the SIEVE/Clock algorithm with byte-based targets and a
// hybrid strategy (random greedy for small targets, proportional fair for large).
package index

import (
	"math/rand"

	"github.com/miretskiy/blobcache/internal/record"
)

// Public constants that callers may need to reference.
const (
	// ShardCount is the number of shards for lock distribution.
	// 256 provides good parallelism with uniform distribution from XXHash3.
	ShardCount = 256
)

// Internal tuning constants.
const (
	nullIdx = 0xFFFFFFFF // Sentinel for "no link" in arena indices

	maxEvictPerLock      = 64        // Max items per lock hold (~13µs)
	overshootFactor      = 2         // Stop if freed > 2x target
	smallTargetThreshold = 64 * 1024 // Switch point for eviction strategy
)

// Key is the 128-bit hash of a blob key.
// Defined as an array (not slice) for GC optimization - no heap pointer.
type Key [16]byte

// Item holds the in-memory metadata for a blob entry.
// Embeds record.FooterEntry for direct persistence mapping.
//
// NOTE: Item.Hash is redundant with the map key (Key [16]byte), but we keep it
// because FooterEntry.Hash is required for on-disk format. When we upgrade to
// 128-bit hashes, FooterEntry.Hash will also change.
type Item struct {
	record.FooterEntry        // Hash, Pos, LogicalSize, PhysicalSize, SeqID, Flags
	SegmentID          int64  // Which segment file contains this blob
}

// BlobIndex is the main entry point for the in-memory index.
// It distributes entries across 256 shards for concurrent access.
type BlobIndex struct {
	shards [ShardCount]shard
}

// New creates a new index with the given initial capacity hint.
// The capacity is distributed across shards for pre-allocation.
func New(initialCapacity int) *BlobIndex {
	bi := &BlobIndex{}
	shardCap := initialCapacity / ShardCount
	if shardCap < 1 {
		shardCap = 1
	}

	for i := 0; i < ShardCount; i++ {
		bi.shards[i].init(shardCap)
	}
	return bi
}

// Get returns the item for the given key and marks it as visited (hot).
// Uses RLock + atomic store to minimize contention on the hot path.
func (idx *BlobIndex) Get(k Key) (Item, bool) {
	shardIdx := k[0] // XXHash3 is uniform, first byte is sufficient
	s := &idx.shards[shardIdx]

	s.mu.RLock()
	// Optimization: No defer in hot path to save ~5-10ns

	i, ok := s.items[k]
	if !ok {
		s.mu.RUnlock()
		return Item{}, false
	}

	// Hot path optimization: check before set.
	// If already visited, avoid atomic write to prevent cache line invalidation.
	if s.nodes[i].visited.Load() == 0 {
		s.nodes[i].visited.Store(1)
	}

	val := s.nodes[i].item
	s.mu.RUnlock()

	return val, true
}

// Put inserts or updates an item.
// It is "pure storage": it NEVER evicts. If capacity is needed, the arena grows.
// Eviction is driven externally by EvictBatch.
func (idx *BlobIndex) Put(k Key, val Item) {
	shardIdx := k[0]
	s := &idx.shards[shardIdx]

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update existing?
	if i, ok := s.items[k]; ok {
		s.nodes[i].item = val
		s.nodes[i].visited.Store(1) // Keep hot on update
		return
	}

	// Alloc from free list or append to arena
	newIdx := s.alloc()

	s.nodes[newIdx] = node{
		key:  k,
		item: val,
		// visited is zero-initialized (cold) by default
	}
	s.items[k] = newIdx
	s.link(newIdx)
}

// Delete removes an item explicitly.
// Returns true if the item existed and was removed.
func (idx *BlobIndex) Delete(k Key) bool {
	shardIdx := k[0]
	s := &idx.shards[shardIdx]

	s.mu.Lock()
	defer s.mu.Unlock()

	i, ok := s.items[k]
	if !ok {
		return false
	}

	delete(s.items, k)
	s.free(i)
	return true
}

// EvictBatch removes items to free up targetBytes of storage.
// Uses a hybrid strategy:
//   - Small targets (<64KB): Random Greedy - pick random start shard
//   - Large targets (≥64KB): Proportional Fair - each shard pays fair share
//
// Returns the evicted items so the caller can clean up associated resources.
func (idx *BlobIndex) EvictBatch(targetBytes int64) []Item {
	if targetBytes <= 0 {
		return nil
	}

	// Heuristic alloc: assume avg item size 4KB
	evicted := make([]Item, 0, targetBytes/4096+16)
	var freedTotal int64

	if targetBytes < smallTargetThreshold {
		// Small target: Random Greedy - pick random start, evict until target met
		startShard := rand.Intn(ShardCount)
		for i := 0; i < ShardCount && freedTotal < targetBytes; i++ {
			shardID := (startShard + i) % ShardCount
			s := &idx.shards[shardID]

			s.mu.Lock()
			freedTotal += s.evictUpTo(targetBytes-freedTotal, &evicted, maxEvictPerLock)
			s.mu.Unlock()
		}
	} else {
		// Large target: Proportional Fairness - each shard pays fair share
		quotaPerShard := targetBytes / int64(ShardCount)
		if quotaPerShard == 0 {
			quotaPerShard = 1
		}

		for i := 0; i < ShardCount; i++ {
			// Safety valve: stop if massively overshot due to large items
			if freedTotal >= targetBytes*overshootFactor {
				break
			}

			s := &idx.shards[i]
			s.mu.Lock()
			freedTotal += s.evictUpTo(quotaPerShard, &evicted, maxEvictPerLock)
			s.mu.Unlock()
		}
	}

	return evicted
}

// Len returns the total number of items across all shards.
func (idx *BlobIndex) Len() int {
	total := 0
	for i := 0; i < ShardCount; i++ {
		idx.shards[i].mu.RLock()
		total += len(idx.shards[i].items)
		idx.shards[i].mu.RUnlock()
	}
	return total
}

// Stats holds statistics about the index state.
type Stats struct {
	Items      int   // Total items in the index
	ArenaNodes int   // Total nodes allocated in arenas
	FreeNodes  int   // Nodes in free lists (available for reuse)
	Shards     int   // Number of shards
	MemoryEst  int64 // Estimated memory usage in bytes
}

// Stats returns statistics about the index state.
func (idx *BlobIndex) Stats() Stats {
	var s Stats
	s.Shards = ShardCount

	for i := 0; i < ShardCount; i++ {
		idx.shards[i].mu.RLock()
		s.Items += len(idx.shards[i].items)
		s.ArenaNodes += len(idx.shards[i].nodes)

		// Count free list nodes
		freeIdx := idx.shards[i].freeHead
		for freeIdx != nullIdx {
			s.FreeNodes++
			freeIdx = idx.shards[i].nodes[freeIdx].next
		}
		idx.shards[i].mu.RUnlock()
	}

	// Estimate memory: node size * arena nodes + map overhead
	const nodeSize = 16 + 64 + 4 + 4 + 4 // key + item + next + prev + visited
	const mapEntryOverhead = 32          // approximate per-entry map overhead
	s.MemoryEst = int64(s.ArenaNodes)*nodeSize + int64(s.Items)*mapEntryOverhead

	return s
}
