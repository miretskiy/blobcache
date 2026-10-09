package blobstore

import (
	"errors"
	"os"
	"sync"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
)

// ioQueue owns one scheduler and its virtual descriptors. A shared queue has
// both an append stream and a read cache; dedicated queues have only one.
// The common Store owns routing, segment IDs, capacity and retirement.
type ioQueue struct {
	store      *Store
	sched      iosched.Scheduler
	writeSlots chan uint32
	reads      *readSlots
	active     struct {
		sync.Mutex
		seg    *segment
		closed bool
	}
}

func (st *Store) startQueues(cpus []int) error {
	writers, readers := st.cfg.writerCount(), st.cfg.readerCount()
	for i, cpu := range cpus {
		write := st.cfg.writeRings == 0 || i < writers
		read := st.cfg.writeRings == 0 || i >= writers
		q := &ioQueue{store: st}
		vfiles := 0
		if write {
			q.writeSlots = make(chan uint32, writeSlots)
			for slot := range uint32(writeSlots) {
				q.writeSlots <- slot
			}
			vfiles = writeSlots
		}
		if read {
			r := len(st.readers)
			n := st.cfg.readHandles / readers
			if r < st.cfg.readHandles%readers {
				n++
			}
			q.reads = newReadSlots(n, uint32(vfiles))
			vfiles += n
		}
		opts := []iosched.Option{
			iosched.WithRingDepth(st.cfg.ringDepth),
			iosched.WithVFiles(uint32(vfiles)),
			iosched.WithCoordinatorCPU(cpu),
			iosched.WithReadBandwidth(iosched.DefaultReadBandwidth / int64(readers)),
			iosched.WithReadIOPS(max(1, iosched.DefaultReadIOPS/int64(readers))),
			iosched.WithWriteBandwidth(iosched.DefaultWriteBandwidth / int64(writers)),
			iosched.WithWriteIOPS(max(1, iosched.DefaultWriteIOPS/int64(writers))),
		}
		if st.cfg.ioBudget == 0 {
			opts = append(opts, iosched.WithoutIOBudget())
		} else {
			opts = append(opts, iosched.WithLatencyGoal(st.cfg.ioBudget))
		}
		var err error
		if q.sched, err = newScheduler(opts...); err != nil {
			return err
		}
		st.queues = append(st.queues, q)
		if write {
			st.writers = append(st.writers, q)
		}
		if read {
			st.readers = append(st.readers, q)
		}
	}
	return nil
}

// submit preserves the existing test hook on every queue. No value is copied.
func (q *ioQueue) submit(op iosched.Op, done func(int, error)) (iosched.Ticket, error) {
	if hook := q.store.cfg.preSubmit; hook != nil {
		op = hook(op)
	}
	return iosched.SubmitNotify(q.sched, op, done)
}

// append writes rec, a framed record of key hash h, at the end of the active
// segment, page-aligned for a direct write, and submits the write, all with
// the active segment's lock held: the lock orders a segment's submissions, so
// its open is accepted before its writes and its seal after them. Submit is a
// non-blocking handoff, so the section stays short.
//
// The record that leaves no room for another starts a segment's seal: it is
// written even if it runs past the segment size, and its write is chained
// with the seal (see sealOps), so the file ends past the preallocated size
// with the footer. The next record starts a new segment, whose first write is
// chained after the open of its file.
func (q *ioQueue) append(rec []byte, h KeyHash) (Ticket, error) {
	st := q.store
	a := &q.active
	a.Lock()
	var evicted Eviction
	var blocked *segment
	defer func() {
		a.Unlock()
		st.evict(evicted)
		st.sealBlocked(blocked)
	}()
	if a.closed {
		return Ticket{}, ErrClosed
	}
	size := int64(len(rec))
	s, off := a.seg, int64(0)
	if s == nil {
		var err error
		if s, evicted, blocked, err = q.startLocked(); err != nil {
			return Ticket{}, err
		}
		a.seg = s
	} else if off = s.pos; st.cfg.directWrites {
		off = align.PageAlign(off)
	}
	end := off + size
	count := len(s.entries) + 1
	last := end+footerSize(count+1) > st.cfg.segmentSize
	fileSize := s.size
	if last {
		fileSize = align.PageAlign(end) + footerSize(count)
	}
	if extra := fileSize - s.size; extra > 0 {
		ids, head, err := st.growSegment(s, extra)
		evicted = append(evicted, ids...)
		if err != nil {
			blocked = head
			return Ticket{}, err
		}
	}

	s.pos = end
	s.entries = append(s.entries, footerEntry{hash: h, off: uint32(off), size: uint32(size)})
	op := iosched.VWriteOp(s.slot, rec, off)
	if off == 0 {
		op = iosched.VOpenatOp(st.dirFD(s.id), segmentName(s.id), st.createFlags(), 0o644, s.slot).
			Link(iosched.VFallocateOp(s.slot, st.cfg.segmentSize), op)
	}
	var whenDone func(int, error)
	if last {
		// Every seal step runs even after an I/O error. The footer is a list of
		// candidates; read verification decides which records survived.
		op = op.HardLink(q.sealOps(s, align.PageAlign(end)))
		whenDone = func(_ int, err error) { q.sealed(s, err) }
		a.seg = nil
	}
	ticket, err := q.submit(op, whenDone)
	if err != nil {
		if last {
			q.closeRejectedSeal(s, err)
		}
		return Ticket{}, err
	}
	return Ticket{ticket: ticket, loc: Location{segment: s.id, offset: uint32(off), size: uint32(size)}}, nil
}

// startLocked starts a new segment in a free write slot; its first write
// creates the file. ErrBusy if every write slot is held by a segment still
// being sealed. Called with the active segment's lock held.
func (q *ioQueue) startLocked() (*segment, Eviction, *segment, error) {
	st := q.store
	var slot uint32
	select {
	case slot = <-q.writeSlots:
	default:
		return nil, nil, nil, ErrBusy
	}
	s, evicted, blocked, err := st.newSegment(q, slot)
	if err != nil {
		q.writeSlots <- slot
	}
	return s, evicted, blocked, err
}

// createFlags are the open flags of a new segment file. O_EXCL because ids
// are never reused: a file already there is not ours to overwrite.
func (st *Store) createFlags() int {
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY
	if st.cfg.directWrites {
		flags |= sys.FlDirectIO.OpenFlags()
	}
	return flags
}

// sealOps builds an immutable footer with the final record's submission.
// Close drains earlier operations; hard links ensure cleanup is attempted
// even when data, footer, or sync fails. No callback modifies the footer.
func (q *ioQueue) sealOps(s *segment, off int64) iosched.Op {
	footer := allocRecord(int(footerSize(len(s.entries))))
	encodeSegmentFooter(footer, s.id, s.entries)
	s.entries = nil
	return iosched.VWriteOp(s.slot, footer, off).HardLink(
		iosched.VFdatasyncOp(s.slot), iosched.VCloseOp(s.slot), iosched.FsyncOp(q.store.dirs[s.id%shardCount]))
}

// sealLocked seals a partial segment during Close or disk pressure. Rotation attaches these
// operations to the final record instead, under this same write lock.
func (q *ioQueue) sealLocked(s *segment) {
	op := q.sealOps(s, s.size-footerSize(len(s.entries)))
	done := func(_ int, err error) { q.sealed(s, err) }
	if _, err := q.submit(op, done); err != nil {
		q.closeRejectedSeal(s, err)
	}
}

// A rejected seal never reached dio, so its close cannot drain earlier writes.
// Submit a real close before releasing the slot or exposing it to eviction.
func (q *ioQueue) closeRejectedSeal(s *segment, cause error) {
	done := func(_ int, err error) { q.sealed(s, errors.Join(cause, err)) }
	if _, err := iosched.SubmitNotify(q.sched, iosched.VCloseOp(s.slot), done); err != nil {
		// The scheduler cannot accept cleanup. Close owns the remaining handles;
		// leave this segment ineligible for eviction while releasing the waiter.
		q.store.stats.failedSegments.Add(1)
		q.writeSlots <- s.slot
		q.store.open.Done()
	}
}

// sealed reports completion to the common registry and returns the local slot.
// The notification is an atomic publication, not a channel or background task.
func (q *ioQueue) sealed(s *segment, err error) {
	q.store.segmentSealed(s, err)
	q.writeSlots <- s.slot
	q.store.open.Done()
}
