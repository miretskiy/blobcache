package blobstore

import (
	"errors"
	"io/fs"
	"slices"

	"github.com/miretskiy/dio/v2/iosched"
)

// Eviction lists the segment IDs retired by one FIFO batch. The caller owns
// the slice and decides how and when to sweep its index. Concurrent writes
// can deliver notifications out of order.
type Eviction []uint64

// fitsLocked checks a reservation against the global budget. The registry
// mutex covers the check and the creation/growth that consumes it.
func (st *Store) fitsLocked(bytes int64, count int) bool {
	return (st.cfg.maxSize == 0 || st.stats.diskBytes.Load()+bytes <= st.cfg.maxSize) &&
		(st.cfg.maxSegments == 0 || len(st.segments.fifo)+count <= st.cfg.maxSegments)
}

// evictLocked retires a completed prefix in creation order. The budget can be
// expressed in bytes or segments; neither depends on producer/ring count.
// Logical capacity is released immediately; callers unlink and notify outside locks.
func (st *Store) evictLocked(bytes int64, count int) Eviction {
	limit, unit := st.cfg.maxSize, st.cfg.segmentSize
	used := st.stats.diskBytes.Load() + bytes
	if st.cfg.maxSegments != 0 {
		limit, unit = int64(st.cfg.maxSegments), 1
		used = int64(len(st.segments.fifo) + count)
	}
	if limit == 0 || st.segments.closed || used <= limit-unit {
		return nil
	}
	initial := used
	target := limit - max(limit/5, 2*unit)
	n := 0
	for _, s := range st.segments.fifo {
		if !s.done.Load() || (n >= 2 && used <= target) {
			break
		}
		if st.cfg.maxSegments != 0 {
			used--
		} else {
			used -= s.size
		}
		n++
	}
	if n == 0 || (n == 1 && initial <= limit) {
		return nil
	}
	ids := make(Eviction, n)
	for i, s := range st.segments.fifo[:n] {
		ids[i] = s.id
		st.stats.diskBytes.Add(-s.size)
	}
	st.stats.segments.Add(-int64(n))
	st.stats.evictedSegments.Add(uint64(n))
	st.segments.fifo = slices.Delete(st.segments.fifo, 0, n)
	return ids
}

// blockerLocked returns the unfinished prefix head when a reservation fails.
// Completed segments need no help; a subsequent allocation can retire them.
func (st *Store) blockerLocked() *segment {
	if len(st.segments.fifo) != 0 && !st.segments.fifo[0].done.Load() {
		return st.segments.fifo[0]
	}
	return nil
}

// sealBlocked is common eviction policy acting on a producer's lifecycle.
// Call with no locks held: never hold two producer locks, or acquire a producer
// lock while holding the registry lock. A segment already sealing needs no work.
func (st *Store) sealBlocked(s *segment) {
	if s == nil || s.owner == nil {
		return
	}
	q := s.owner
	q.active.Lock()
	if q.active.seg == s {
		q.sealLocked(s)
		q.active.seg = nil
	}
	q.active.Unlock()
}

// evict submits best-effort unlinks through dio and notifies the caller outside
// store locks. It neither waits for I/O nor starts a goroutine. The callback
// owns ids and may schedule its index sweep elsewhere.
func (st *Store) evict(ids Eviction) {
	if len(ids) != 0 {
		st.retireReads(ids[len(ids)-1] + 1)
	}
	for _, id := range ids {
		_, err := st.writers[id%uint64(len(st.writers))].submit(iosched.UnlinkatOp(st.dirFD(id), segmentName(id)), st.evictionResult)
		if err != nil {
			st.evictionResult(0, err)
		}
	}
	if len(ids) != 0 && st.cfg.onEvict != nil {
		st.cfg.onEvict(ids)
	}
}

// Completion only records errors; it never coordinates eviction or takes locks.
func (st *Store) evictionResult(_ int, err error) {
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		st.stats.evictionErrors.Add(1)
	}
}

// forgetSegment updates accounting only after a file was removed. Recovery
// also uses it to discard an invalid footer; eviction has not started then.
func (st *Store) forgetSegment(id uint64) {
	a := &st.segments
	a.Lock()
	defer a.Unlock()
	for i, s := range a.fifo {
		if s.id != id {
			continue
		}
		st.stats.diskBytes.Add(-s.size)
		st.stats.segments.Add(-1)
		a.fifo = slices.Delete(a.fifo, i, i+1)
		return
	}
}
