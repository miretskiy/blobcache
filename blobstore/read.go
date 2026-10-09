package blobstore

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"syscall"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
)

// readSlots has exactly the configured number of slots. Its mutex orders all
// submissions, including opens and replacement. dio orders their execution,
// so lookup-to-submit pins, spare slots and duplicate opens are unnecessary.
// An unfinished opening chain cannot itself be replaced.
type readSlots struct {
	mu     sync.Mutex
	byID   map[uint64]*readSlot
	slots  []readSlot
	hand   int
	closed bool
	before uint64 // IDs below this boundary have been evicted; protected by mu
}

type readSlot struct {
	id         uint64
	vfd        uint32
	referenced bool
	closing    bool           // opening holds a close ticket, so the next open needs no close
	opening    iosched.Ticket // zero for an empty slot; otherwise its last open chain
}

func newReadSlots(n int, first uint32) *readSlots {
	r := &readSlots{byID: make(map[uint64]*readSlot), slots: make([]readSlot, n)}
	for i := range r.slots {
		r.slots[i].vfd = first + uint32(i)
	}
	return r
}

func ticketReady(t iosched.Ticket) bool {
	if t == (iosched.Ticket{}) {
		return true
	}
	select {
	case <-t.Done():
		return true
	default:
		return false
	}
}

// victim is called under mu. CLOCK only decides which handle to replace;
// the scheduler's close barrier protects reads already submitted to it.
func (r *readSlots) victim() *readSlot {
	for range 2 * len(r.slots) {
		s := &r.slots[r.hand]
		r.hand = (r.hand + 1) % len(r.slots)
		if !ticketReady(s.opening) {
			continue
		}
		if s.referenced {
			s.referenced = false
			continue
		}
		return s
	}
	return nil
}

func (r *readSlots) unmap(s *readSlot) {
	if r.byID[s.id] == s {
		delete(r.byID, s.id)
	}
}

// Read reads and verifies a record, using the caller's buffer if large and
// aligned enough. On a handle miss, open and the initial read are one chain;
// followers submit under the same mutex and dio holds them behind that chain.
// Waiting and checksum verification take place outside the mutex.
func (st *Store) Read(loc Location, key, buf []byte) ([]byte, error) {
	size := int(loc.size)
	if size < trailerSize {
		return nil, ErrCorrupt
	}
	if len(buf) < size || (st.cfg.directReads && !align.IsAligned(buf)) {
		buf = allocRecord(size)
	}
	rec := buf[:size]
	// A chain's ticket reports its root's byte count, which may be an open.
	// Erasing the trailer ensures a short read cannot validate stale buffer
	// contents, even when the buffer previously held this same record.
	clear(rec[size-trailerSize:])
	q := st.readers[loc.segment%uint64(len(st.readers))]
	r := q.reads
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	if loc.segment < r.before {
		r.mu.Unlock()
		return nil, ErrNotFound
	}
	slot := r.byID[loc.segment]
	var op iosched.Op
	opening := false
	if slot == nil {
		slot = r.victim()
		if slot == nil {
			r.mu.Unlock()
			return nil, ErrBusy
		}
		op = iosched.VOpenatOp(st.dirFD(loc.segment), segmentName(loc.segment), st.readFlags(), 0, slot.vfd).
			Link(iosched.VReadOp(slot.vfd, rec, int64(loc.offset)))
		if slot.opening != (iosched.Ticket{}) && !slot.closing {
			// A previous failed open may have left the slot empty. Still attempt
			// the new open if close reports EBADF. The ticket reports that error;
			// a subsequent lookup can retry, without leaking the old descriptor.
			op = iosched.VCloseOp(slot.vfd).HardLink(op)
		}
		opening = true
	} else {
		op = iosched.VReadOp(slot.vfd, rec, int64(loc.offset))
	}
	ticket, err := q.submit(op, nil)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if opening {
		r.unmap(slot)
		slot.closing = false
		slot.id, slot.opening = loc.segment, ticket
		r.byID[loc.segment] = slot
	}
	slot.referenced = true
	generation := slot.opening
	r.mu.Unlock()
	_, err = ticket.Wait()
	if opening || err != nil {
		r.mu.Lock()
		if slot.opening == generation {
			if err != nil {
				r.unmap(slot)
			}
			// Retirement may have skipped this unfinished opening chain.
			if slot.id < r.before {
				q.closeReadSlot(slot)
			}
		}
		r.mu.Unlock()
	}
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.EBADF) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return verifyRecord(rec, key)
}

// retireReads prevents reopening retired segments and submits closes without
// waiting. An unfinished open is closed by its reader after the initial chain
// completes. A pending replacement already has its old descriptor's close queued.
func (st *Store) retireReads(before uint64) {
	for _, q := range st.readers {
		r := q.reads
		r.mu.Lock()
		r.before = max(r.before, before)
		for i := range r.slots {
			s := &r.slots[i]
			if s.id < r.before && ticketReady(s.opening) {
				q.closeReadSlot(s)
			}
		}
		r.mu.Unlock()
	}
}

// closeReadSlot is called under the read mutex, after the opening chain finishes.
// dio's close barrier drains already-submitted reads. CLOCK reuses the slot once
// the close ticket completes, so no callback or waiter is needed to clear it.
func (q *ioQueue) closeReadSlot(s *readSlot) {
	if s.opening == (iosched.Ticket{}) || s.closing {
		return
	}
	q.reads.unmap(s)
	ticket, err := q.submit(iosched.VCloseOp(s.vfd), func(n int, err error) {
		// Failed opens leave empty slots; closing one is harmless.
		if !errors.Is(err, syscall.EBADF) {
			q.store.evictionResult(n, err)
		}
	})
	if err != nil {
		q.store.evictionResult(0, err)
		return
	}
	s.opening, s.closing, s.referenced = ticket, true, false
}

func (st *Store) readFlags() int {
	flags := os.O_RDONLY
	if st.cfg.directReads {
		flags |= sys.FlDirectIO.OpenFlags()
	}
	return flags
}
