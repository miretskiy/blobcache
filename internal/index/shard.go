package index

import (
	"sync"
	"sync/atomic"
)

// shard is a single partition of the index with its own lock and arena.
// The arena-backed design eliminates heap pointers for GC optimization.
type shard struct {
	mu sync.RWMutex

	items    map[Key]uint32 // Directory: Key -> Arena Index
	nodes    []node         // Arena: Contiguous memory, no pointers
	freeHead uint32         // Free List: Stack head
	hand     uint32         // Sieve Cursor: Current position
	head     uint32         // Circular List Head: Newest item
}

// node is an arena-backed entry with no heap pointers.
// Uses uint32 indices instead of pointers for GC optimization.
type node struct {
	key     Key           // Back-reference for eviction callbacks
	item    Item          // Lean blob coordinates (24 bytes)
	next    uint32        // Index in 'nodes' slice (circular list)
	prev    uint32        // Index in 'nodes' slice (circular list)
	visited atomic.Uint32 // SIEVE algorithm: 0=cold, 1=hot
}

func (s *shard) init(capHint int) {
	s.items = make(map[Key]uint32, capHint)
	s.nodes = make([]node, 0, capHint)
	s.freeHead = nullIdx
	s.hand = nullIdx
	s.head = nullIdx
}

// alloc returns an index for a new node.
// Reuses from free list if available, otherwise appends to arena.
func (s *shard) alloc() uint32 {
	if s.freeHead != nullIdx {
		idx := s.freeHead
		s.freeHead = s.nodes[idx].next // Pop from stack
		return idx
	}
	s.nodes = append(s.nodes, node{})
	return uint32(len(s.nodes) - 1)
}

// free pushes an index onto the free list after unlinking from circular list.
func (s *shard) free(idx uint32) {
	s.unlink(idx)
	s.nodes[idx].next = s.freeHead // Push to free stack
	s.freeHead = idx
}

// link adds idx to the circular list at head position (newest).
// Tail is always head.prev in a circular list.
func (s *shard) link(idx uint32) {
	if s.head == nullIdx {
		// First item: circular self-reference
		s.head = idx
		s.nodes[idx].next = idx
		s.nodes[idx].prev = idx
		s.hand = idx
		return
	}

	// Insert between tail (head.prev) and head
	head := s.head
	tail := s.nodes[head].prev

	s.nodes[idx].next = head
	s.nodes[idx].prev = tail
	s.nodes[head].prev = idx
	s.nodes[tail].next = idx

	s.head = idx
}

// unlink removes idx from the circular list.
func (s *shard) unlink(idx uint32) {
	next := s.nodes[idx].next
	prev := s.nodes[idx].prev

	if next == idx {
		// Only one item in list (points to itself)
		s.head = nullIdx
		s.hand = nullIdx
		return
	}

	// Stitch neighbors together
	s.nodes[prev].next = next
	s.nodes[next].prev = prev

	// Update head if we removed it
	if s.head == idx {
		s.head = next
	}

	// Update hand if it was on the victim
	if s.hand == idx {
		s.hand = next
	}
}

// runSieve finds a victim using the Clock/SIEVE algorithm.
// Assumes the list is NOT empty. Returns the victim's arena index.
func (s *shard) runSieve() uint32 {
	if s.head == nullIdx {
		return nullIdx
	}
	if s.hand == nullIdx {
		s.hand = s.head
	}

	for {
		curr := s.hand
		s.hand = s.nodes[curr].next // Advance immediately (circular - always safe)

		if s.nodes[curr].visited.Load() == 1 {
			// Give second chance: clear visited bit and continue
			s.nodes[curr].visited.Store(0)
			continue
		}

		// Found cold victim
		return curr
	}
}

// evictUpTo runs eviction inside the lock, returning bytes freed.
// Limits work to maxCount items to bound lock hold time.
func (s *shard) evictUpTo(quotaBytes int64, dst *[]Item, maxCount int) int64 {
	var localFreed int64
	var count int

	for count < maxCount && localFreed < quotaBytes {
		if s.head == nullIdx {
			break
		}

		victimIdx := s.runSieve()
		item := s.nodes[victimIdx].item

		*dst = append(*dst, item)
		localFreed += int64(item.PhysicalLen)
		count++

		delete(s.items, s.nodes[victimIdx].key)
		s.free(victimIdx)
	}
	return localFreed
}
