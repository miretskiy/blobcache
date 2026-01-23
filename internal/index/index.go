// Package index provides a GC-optimized in-memory index for blob metadata.
//
// The index uses xmap for sharded concurrent access, with a custom ShardState
// that holds arena-backed nodes for the SIEVE eviction algorithm.
//
// Design Philosophy: RAM stores only "coordinates" (24 bytes per item).
// Full metadata (SeqID, LogicalSize, compression) lives on disk in record.Header
// and is read on-demand during Get() operations.
package index

import (
	"math/rand"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/xmap"
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

// Key is the 128-bit XXH3 hash of a blob key.
// Alias to xmap.Key for consistency across the codebase.
type Key = xmap.Key

// Item holds the minimum coordinates needed to locate a blob on disk.
// This design (32 bytes) enables 1TB capacity with ~24GB RAM (4KB blobs).
//
// Layout (32 bytes, 8-byte aligned):
//
//	Key         Key    // 16B: 128-bit XXH3 hash - back-reference for eviction/map deletes
//	SegmentID   uint32 //  4B: Supports 4 billion segments
//	Offset      uint32 //  4B: Supports 4GB segment files
//	PhysicalLen uint32 //  4B: Supports 4GB blobs
//	Flags       uint32 //  4B: Deleted, errno, compression (alignment freebie)
//
// Full metadata (SeqID, LogicalSize) is read from disk Header on Get().
type Item struct {
	Key         Key    // 128-bit XXH3 hash - back-reference for eviction
	SegmentID   uint32 // Which segment file contains this blob
	Offset      uint32 // Byte offset within segment (to record header)
	PhysicalLen uint32 // On-disk size in bytes (header + key + value)
	Flags       uint32 // Status flags (deleted, errno, compression codec)
}

// Item flag constants (packed into 32 bits).
const (
	// Compression codec in bits 31-28 (4 bits, 16 values).
	itemFlagCompressionShift = 28
	itemFlagCompressionMask  = uint32(0xF) << itemFlagCompressionShift

	// BlobErrno in bits 27-23 (5 bits, 32 values).
	itemFlagErrnoShift = 23
	itemFlagErrnoMask  = uint32(0x1F) << itemFlagErrnoShift

	// Status flags.
	itemFlagDeleted = uint32(1) << 22 // Tombstone marker
)

// IsDeleted returns true if the blob is marked as deleted.
func (item *Item) IsDeleted() bool {
	return (item.Flags & itemFlagDeleted) != 0
}

// SetDeleted marks the blob as deleted (tombstone).
// Clears PhysicalLen and all flags except deleted, since tombstones have no data.
func (item *Item) SetDeleted() {
	item.Flags = itemFlagDeleted // Clear compression, errno; keep only deleted flag
	item.PhysicalLen = 0
}

// Errno returns the error code for this blob.
func (item *Item) Errno() base.BlobErrno {
	return base.BlobErrno((item.Flags & itemFlagErrnoMask) >> itemFlagErrnoShift)
}

// SetErrno sets the error code for this blob.
func (item *Item) SetErrno(errno base.BlobErrno) {
	item.Flags = (item.Flags &^ itemFlagErrnoMask) | (uint32(errno&0x1F) << itemFlagErrnoShift)
}

// HasError returns true if the blob has a non-zero error code.
func (item *Item) HasError() bool {
	return item.Errno() != base.ErrNone
}

// Compression returns the compression codec for this blob.
func (item *Item) Compression() compression.Codex {
	return compression.Codex((item.Flags & itemFlagCompressionMask) >> itemFlagCompressionShift)
}

// SetCompression sets the compression codec for this blob.
func (item *Item) SetCompression(c compression.Codex) {
	item.Flags = (item.Flags &^ itemFlagCompressionMask) | (uint32(c) << itemFlagCompressionShift)
}

// IsCompressed returns true if the blob is compressed.
func (item *Item) IsCompressed() bool {
	return item.Compression() != compression.CodexNone
}

// ShardState is the "Extra" payload stored in each xmap shard.
// It holds the arena and SIEVE algorithm state for eviction.
//
// Size breakdown:
//   - nodes []node:    24 bytes (slice header)
//   - freeHead uint32:  4 bytes
//   - hand uint32:      4 bytes
//   - head uint32:      4 bytes
//   - _:               60 bytes (explicit padding)
//   - Total:           96 bytes
//
// Combined with xmap.Shard base (32 bytes), total = 128 bytes (2 cache lines).
// The padding must be 60 bytes (not 56) to avoid implicit compiler padding
// that would mess up the alignment math.
type ShardState struct {
	nodes    []node   // Arena: Contiguous memory, no pointers
	freeHead uint32   // Free List: Stack head
	hand     uint32   // Sieve Cursor: Current position
	head     uint32   // Circular List Head: Newest item
	_        [60]byte // Padding to make Shard 128 bytes (2 cache lines)
}

// node is an arena-backed entry with no heap pointers.
// Uses uint32 indices instead of pointers for GC optimization.
//
// Size: 44 bytes = Item(32) + next(4) + prev(4) + visited(4)
type node struct {
	item    Item          // Blob coordinates including Key for eviction (32 bytes)
	next    uint32        // Index in 'nodes' slice (circular list)
	prev    uint32        // Index in 'nodes' slice (circular list)
	visited atomic.Uint32 // SIEVE algorithm: 0=cold, 1=hot
}

// BlobIndex is the main entry point for the in-memory index.
// It uses xmap for sharding with custom ShardState for eviction.
type BlobIndex struct {
	*xmap.Map[uint32, ShardState]
}

// NewBlobIndex creates a new index with the given initial capacity hint.
// The capacity is distributed across shards for pre-allocation.
func NewBlobIndex(initialCapacity int) *BlobIndex {
	bi := &BlobIndex{
		Map: xmap.New[uint32, ShardState](
			xmap.WithShardShift(8), // 256 shards
			xmap.WithInitialCapacity(initialCapacity),
		),
	}

	// Initialize arena for each shard
	shardCap := initialCapacity / ShardCount
	if shardCap < 1 {
		shardCap = 1
	}
	for i := range ShardCount {
		s := bi.ShardAt(i)
		s.Lock()
		s.Extra.nodes = make([]node, 0, shardCap)
		s.Extra.freeHead = nullIdx
		s.Extra.hand = nullIdx
		s.Extra.head = nullIdx
		s.Unlock()
	}

	return bi
}

// Get returns the item for the given key and marks it as visited (hot).
// Uses RLock + atomic store to minimize contention on the hot path.
func (idx *BlobIndex) Get(k Key) (Item, bool) {
	s := idx.Shard(k)

	s.RLock()
	// Optimization: No defer in hot path to save ~5-10ns

	i, ok := s.Items[k]
	if !ok {
		s.RUnlock()
		return Item{}, false
	}

	// Hot path optimization: check before set.
	// If already visited, avoid atomic write to prevent cache line invalidation.
	if s.Extra.nodes[i].visited.Load() == 0 {
		s.Extra.nodes[i].visited.Store(1)
	}

	val := s.Extra.nodes[i].item
	s.RUnlock()

	return val, true
}

// Put inserts or updates an item.
// It is "pure storage": it NEVER evicts. If capacity is needed, the arena grows.
// Eviction is driven externally by EvictBatch.
//
// The item's Key field must be set - it serves as both the map key and the
// back-reference needed for eviction.
func (idx *BlobIndex) Put(item Item) {
	s := idx.Shard(item.Key)

	s.Lock()
	defer s.Unlock()

	// Update existing?
	if i, ok := s.Items[item.Key]; ok {
		s.Extra.nodes[i].item = item
		s.Extra.nodes[i].visited.Store(1) // Keep hot on update
		return
	}

	// Alloc from free list or append to arena
	newIdx := allocNode(&s.Extra)

	s.Extra.nodes[newIdx] = node{item: item}
	s.Items[item.Key] = newIdx
	linkNode(&s.Extra, newIdx)
}

// Delete removes an item explicitly.
// Returns true if the item existed and was removed.
func (idx *BlobIndex) Delete(k Key) bool {
	s := idx.Shard(k)

	s.Lock()
	defer s.Unlock()

	i, ok := s.Items[k]
	if !ok {
		return false
	}

	delete(s.Items, k)
	freeNode(&s.Extra, i)
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
		for i := range ShardCount {
			if freedTotal >= targetBytes {
				break
			}
			shardID := (startShard + i) % ShardCount
			s := idx.ShardAt(shardID)

			s.Lock()
			freedTotal += evictUpTo(&s.Extra, s.Items, targetBytes-freedTotal, &evicted, maxEvictPerLock)
			s.Unlock()
		}
	} else {
		// Large target: Proportional Fairness - each shard pays fair share
		quotaPerShard := targetBytes / int64(ShardCount)
		if quotaPerShard == 0 {
			quotaPerShard = 1
		}

		for i := range ShardCount {
			// Safety valve: stop if massively overshot due to large items
			if freedTotal >= targetBytes*overshootFactor {
				break
			}

			s := idx.ShardAt(i)
			s.Lock()
			freedTotal += evictUpTo(&s.Extra, s.Items, quotaPerShard, &evicted, maxEvictPerLock)
			s.Unlock()
		}
	}

	return evicted
}

// NumItems returns the total number of items across all shards.
func (idx *BlobIndex) NumItems() int {
	return idx.Len()
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
	var st Stats
	st.Shards = ShardCount

	for i := range ShardCount {
		s := idx.ShardAt(i)
		s.RLock()
		st.Items += len(s.Items)
		st.ArenaNodes += len(s.Extra.nodes)

		// Count free list nodes
		freeIdx := s.Extra.freeHead
		for freeIdx != nullIdx {
			st.FreeNodes++
			freeIdx = s.Extra.nodes[freeIdx].next
		}
		s.RUnlock()
	}

	// Estimate memory: node size * arena nodes + map overhead
	// node = Item(32) + next(4) + prev(4) + visited(4) = 44 bytes
	// Note: Item.Key (16 bytes) is redundant with map key - future optimization target
	const nodeSize = 32 + 4 + 4 + 4
	const mapEntryOverhead = 32 // approximate per-entry map overhead (Key 16 + uint32 4 + bucket ~12)
	st.MemoryEst = int64(st.ArenaNodes)*nodeSize + int64(st.Items)*mapEntryOverhead

	return st
}

// --- Arena helpers (operate on ShardState) ---

// allocNode returns an index for a new node.
// Reuses from free list if available, otherwise appends to arena.
func allocNode(state *ShardState) uint32 {
	if state.freeHead != nullIdx {
		idx := state.freeHead
		state.freeHead = state.nodes[idx].next // Pop from stack
		return idx
	}
	state.nodes = append(state.nodes, node{})
	return uint32(len(state.nodes) - 1)
}

// freeNode pushes an index onto the free list after unlinking from circular list.
func freeNode(state *ShardState, idx uint32) {
	unlinkNode(state, idx)
	state.nodes[idx].next = state.freeHead // Push to free stack
	state.freeHead = idx
}

// linkNode adds idx to the circular list at head position (newest).
// Tail is always head.prev in a circular list.
func linkNode(state *ShardState, idx uint32) {
	if state.head == nullIdx {
		// First item: circular self-reference
		state.head = idx
		state.nodes[idx].next = idx
		state.nodes[idx].prev = idx
		state.hand = idx
		return
	}

	// Insert between tail (head.prev) and head
	head := state.head
	tail := state.nodes[head].prev

	state.nodes[idx].next = head
	state.nodes[idx].prev = tail
	state.nodes[head].prev = idx
	state.nodes[tail].next = idx

	state.head = idx
}

// unlinkNode removes idx from the circular list.
func unlinkNode(state *ShardState, idx uint32) {
	next := state.nodes[idx].next
	prev := state.nodes[idx].prev

	if next == idx {
		// Only one item in list (points to itself)
		state.head = nullIdx
		state.hand = nullIdx
		return
	}

	// Stitch neighbors together
	state.nodes[prev].next = next
	state.nodes[next].prev = prev

	// Update head if we removed it
	if state.head == idx {
		state.head = next
	}

	// Update hand if it was on the victim
	if state.hand == idx {
		state.hand = next
	}
}

// runSieve finds a victim using the Clock/SIEVE algorithm.
// Assumes the list is NOT empty. Returns the victim's arena index.
func runSieve(state *ShardState) uint32 {
	if state.head == nullIdx {
		return nullIdx
	}
	if state.hand == nullIdx {
		state.hand = state.head
	}

	for {
		curr := state.hand
		state.hand = state.nodes[curr].next // Advance immediately (circular - always safe)

		if state.nodes[curr].visited.Load() == 1 {
			// Give second chance: clear visited bit and continue
			state.nodes[curr].visited.Store(0)
			continue
		}

		// Found cold victim
		return curr
	}
}

// evictUpTo runs eviction inside the lock, returning bytes freed.
// Limits work to maxCount items to bound lock hold time.
func evictUpTo(
	state *ShardState, items map[Key]uint32, quotaBytes int64, dst *[]Item, maxCount int,
) int64 {
	var localFreed int64
	var count int

	for count < maxCount && localFreed < quotaBytes {
		if state.head == nullIdx {
			break
		}

		victimIdx := runSieve(state)
		item := state.nodes[victimIdx].item

		*dst = append(*dst, item)
		localFreed += int64(item.PhysicalLen)
		count++

		delete(items, state.nodes[victimIdx].item.Key)
		freeNode(state, victimIdx)
	}
	return localFreed
}

// Relocate atomically moves an item from (oldSeg, oldOff) to (newSeg, newOff).
// Returns true if the relocation succeeded, false if the current location doesn't match.
//
// This is used during compaction to safely move items to new segments:
//   - If a concurrent write updated the item to a newer segment, relocation fails (safe)
//   - If the item is still at the expected old location, relocation succeeds
//
// The compare-and-swap semantics prevent the "Leapfrog Hazard" where compaction
// could accidentally overwrite a newer write with stale data from an old segment.
func (idx *BlobIndex) Relocate(k Key, oldSeg, oldOff, newSeg, newOff uint32) bool {
	s := idx.Shard(k)

	s.Lock()
	defer s.Unlock()

	i, ok := s.Items[k]
	if !ok {
		return false
	}

	item := &s.Extra.nodes[i].item
	if item.SegmentID != oldSeg || item.Offset != oldOff {
		// Location changed since we read it - someone else updated the item
		return false
	}

	// Ghost Guard: If deleted after staleness check, skip relocation.
	// Prevents "Ghost Resurrection" where a concurrent delete could be
	// overwritten by compaction's stale data.
	if item.IsDeleted() {
		return false
	}

	// Relocation success: update to new location
	item.SegmentID = newSeg
	item.Offset = newOff
	return true
}
