package index

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/metadata"
)

// node is internal to the eviction policy. It "owns" the Entry data.
type node struct {
	entry   Entry
	visited atomic.Bool
	next    *node
	prev    *node
}

var nodePool = sync.Pool{
	New: func() any { return &node{} },
}

// newNode is an internal helper for the policy factory
func newNode(rec metadata.BlobRecord, segID int64) *node {
	n := nodePool.Get().(*node)
	n.entry = Entry{BlobRecord: rec, SegmentID: segID}
	n.visited.Store(false)
	n.next, n.prev = nil, nil
	return n
}

type sievePolicy struct {
	sync.Mutex
	head, tail, hand *node
	count            int
}

// Add creates a node and links it. It returns the pointer (the handle).
func (p *sievePolicy) Add(entry Entry) *node {
	p.Lock()
	defer p.Unlock()

	n := newNode(entry.BlobRecord, entry.SegmentID)
	n.prev = p.head
	if p.head != nil {
		p.head.next = n
	}
	p.head = n
	if p.tail == nil {
		p.tail = n
	}
	p.count++
	return n
}

// removeNode unlinks the node from the FIFO list.
// It does NOT return the node to the pool.
func (p *sievePolicy) removeNode(n *node) {
	p.Lock()
	defer p.Unlock()
	p.unlinkLocked(n)
}

// unlinkLocked performs pointer surgery. Caller must hold p.Mutex.
func (p *sievePolicy) unlinkLocked(n *node) {
	// If hand is currently pointing to this node, move it forward
	if p.hand == n {
		p.hand = n.next
	}

	if n.prev != nil {
		n.prev.next = n.next
	} else {
		// n was the tail
		p.tail = n.next
	}

	if n.next != nil {
		n.next.prev = n.prev
	} else {
		// n was the head
		p.head = n.prev
	}

	p.count--
	n.next, n.prev = nil, nil
}

// EvictNext implements the Sieve algorithm.
// It identifies the victim, unlinks it from the list, and returns the *node.
func (p *sievePolicy) EvictNext() (*node, error) {
	p.Lock()
	defer p.Unlock()

	if p.count == 0 {
		return nil, errors.New("eviction: empty")
	}

	// Sieve logic: search for a node where visited == false
	// Limit to 2 full passes to prevent infinite loops in corrupt states
	for i := 0; i < p.count*2; i++ {
		if p.hand == nil {
			p.hand = p.tail
		}

		curr := p.hand
		p.hand = curr.next

		if curr.visited.Load() {
			curr.visited.Store(false)
			continue
		}

		// Victim found. Unlink immediately so the hand never lands here again.
		p.unlinkLocked(curr)
		return curr, nil
	}

	return nil, errors.New("eviction: failed to find victim")
}

// recycleNode returns a node to the pool after it is no longer referenced
// by the lookup map or any caller.
func recycleNode(n *node) {
	n.entry = Entry{}
	n.visited.Store(false)
	n.next, n.prev = nil, nil
	nodePool.Put(n)
}
