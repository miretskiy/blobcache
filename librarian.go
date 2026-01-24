package blobcache

import (
	"bytes"
	"io"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
)

type Publisher interface {
	Publish(slab *SharedSlab)
}

// Librarian manages the "Visible History" (The Catalog) of Slabs.
// LOCK-FREE: Uses atomic pointer swapping and safe reference counting.
type Librarian struct {
	view        atomic.Pointer[[]*SharedSlab]
	closed      atomic.Bool
	maxCached   int
	errReporter ErrorReporter
}

func NewLibrarian(maxCached int, reporter ErrorReporter) *Librarian {
	l := &Librarian{maxCached: maxCached, errReporter: reporter}
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
// Returns (value, storedKey, releaser, found). storedKey is the key bytes stored with
// this hash - caller should verify it matches expected key to detect hash collisions.
// NB: This is a low level method where the caller is expected to quickly consume
// the returned data and promptly release it via Releaser.
func (l *Librarian) Acquire(hashKey Key) (value []byte, storedKey []byte, rel Releaser, found bool) {
	// 1. Load the immutable snapshot
	list := *l.view.Load()

	// 2. Iterate
	for _, slab := range list {
		// 3. Attempt to Acquire
		// If 'Publish' evicts this slab while we are iterating,
		// slab.Acquire() will return false via TryInc(), protecting us.
		data, keyBytes, releaser, ok, errno := slab.Acquire(hashKey)
		if errno != base.ErrNone {
			l.errReporter.ReportBlobError(hashKey, errno)
			return nil, nil, Releaser{}, false
		}
		if ok {
			return data, keyBytes, releaser, true
		}
	}
	return nil, nil, Releaser{}, false
}

// ProtectedView handles the lifecycle automatically via a closure.
// The callback receives (storedKey, value) - caller should verify storedKey matches expected.
// NB: data is valid only for the duration of the function.
func (l *Librarian) ProtectedView(hashKey Key, fn func(storedKey, value []byte)) bool {
	list := *l.view.Load()
	for _, slab := range list {
		found, errno := slab.ProtectedView(hashKey, fn)
		if errno != base.ErrNone {
			l.errReporter.ReportBlobError(hashKey, errno)
			return false
		}
		if found {
			return true
		}
	}
	return false
}

// View is a convenient (but a bit more expensive) way to view the data via io.Reader.
// Note: This method does not return storedKey - use Acquire directly if key verification needed.
// NB: the reader is valid only for the duration of the function.
func (l *Librarian) View(hashKey Key, fn func(r io.Reader)) bool {
	data, _, releaser, found := l.Acquire(hashKey)
	if !found {
		return false
	}
	defer releaser.Release()

	fn(bytes.NewReader(data))
	return true
}

// Invalidate removes a key from all slabs in the Librarian.
// Used by Delete to prevent serving stale data from cache after deletion.
func (l *Librarian) Invalidate(key Key) {
	list := *l.view.Load()
	for _, slab := range list {
		slab.Invalidate(key)
	}
}

func (l *Librarian) Close() {
	if l.closed.CompareAndSwap(false, true) {
		list := *l.view.Load()
		for _, slab := range list {
			slab.buf.Unpin()
		}
		empty := make([]*SharedSlab, 0)
		l.view.Store(&empty)
	}
}
