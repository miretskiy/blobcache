package blobcache

import (
	"bytes"
	"io"
	"sync/atomic"
)

type Publisher interface {
	Publish(slab *SharedSlab)
}

// Librarian manages the "Visible History" (The Catalog) of Slabs.
// LOCK-FREE: Uses atomic pointer swapping and safe reference counting.
type Librarian struct {
	view      atomic.Pointer[[]*SharedSlab]
	maxCached int
}

func NewLibrarian(maxCached int) *Librarian {
	l := &Librarian{maxCached: maxCached}
	empty := make([]*SharedSlab, 0)
	l.view.Store(&empty)
	return l
}

func (l *Librarian) Publish(slab *SharedSlab) {
	// Disabled: do nothing.
	if l.maxCached <= 0 {
		return
	}

	// Take our own reference. The caller (MemTable) keeps its "Active Writer" ref.
	if !slab.buf.TryInc() {
		return // Slab already dead, skip
	}

	// Optimistic Update Loop (Compare-And-Swap)
	// Ensures linear history even if Publish is called concurrently
	// (though usually MemTable is the single producer).
	for {
		oldPtr := l.view.Load()
		oldList := *oldPtr

		newList := make([]*SharedSlab, 0, len(oldList)+1)
		newList = append(newList, slab)
		newList = append(newList, oldList...)

		var victim *SharedSlab
		if len(newList) > l.maxCached {
			victim = newList[len(newList)-1]
			newList = newList[:len(newList)-1]
		}

		if l.view.CompareAndSwap(oldPtr, &newList) {
			// Success! We installed the new view.
			// Now we can safely unpin the victim.
			if victim != nil {
				victim.buf.Unpin()
			}
			return
		}
		// CAS failed, retry
	}
}

// Acquire searches the catalog for the key.
// WAIT-FREE: No mutexes.
func (l *Librarian) Acquire(key Key) ([]byte, Releaser, bool) {
	// 1. Load the immutable snapshot
	list := *l.view.Load()
	
	// 2. Iterate
	for _, slab := range list {
		// 3. Attempt to Acquire
		// If 'Publish' evicts this slab while we are iterating,
		// slab.Acquire() will return false via TryInc(), protecting us.
		if data, releaser, found := slab.Acquire(key); found {
			return data, releaser, true
		}
	}
	return nil, Releaser{}, false
}

func (l *Librarian) ProtectedView(key Key, fn func(data []byte)) bool {
	list := *l.view.Load()
	for _, slab := range list {
		if slab.ProtectedView(key, fn) {
			return true
		}
	}
	return false
}

func (l *Librarian) View(key Key, fn func(r io.Reader)) bool {
	data, releaser, found := l.Acquire(key)
	if !found {
		return false
	}
	defer releaser.Release()
	
	fn(bytes.NewReader(data))
	return true
}

func (l *Librarian) Close() {
	list := *l.view.Load()
	for _, slab := range list {
		slab.buf.Unpin()
	}
	empty := make([]*SharedSlab, 0)
	l.view.Store(&empty)
}
