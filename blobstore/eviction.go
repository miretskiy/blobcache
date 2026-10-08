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

// evictLocked selects an oldest-first batch at allocation/rotation. Retirement
// immediately releases its logical capacity. The caller submits unlinks for
// files after releasing the write lock, with no background task or retry state.
func (st *Store) evictLocked(required int64) Eviction {
	a := &st.activeSegment
	used := st.stats.diskBytes.Load() + required
	if st.cfg.maxSize == 0 || a.closed || used <= st.cfg.maxSize-st.cfg.segmentSize {
		return nil
	}
	target := st.cfg.maxSize - max(st.cfg.maxSize/5, 2*st.cfg.segmentSize)
	n := 0
	for _, s := range a.segments {
		if !s.done.Load() || (n >= 2 && used <= target) {
			break
		}
		used -= s.size
		n++
	}
	// Prefer batches of at least two, but allow a single completed victim
	// when necessary to admit a blocked reservation in a small store.
	if n == 0 || (n == 1 && st.stats.diskBytes.Load()+required <= st.cfg.maxSize) {
		return nil
	}
	ids := make(Eviction, n)
	for i, s := range a.segments[:n] {
		ids[i] = s.id
		st.stats.diskBytes.Add(-s.size)
	}
	st.retireReads(ids[n-1] + 1)
	st.stats.segments.Add(-int64(n))
	st.stats.evictedSegments.Add(uint64(n))
	a.segments = slices.Delete(a.segments, 0, n)

	return ids
}

// evict submits best-effort unlinks through dio and notifies the caller outside
// store locks. It neither waits for I/O nor starts a goroutine. The callback owns ids and may schedule its index sweep elsewhere.
func (st *Store) evict(ids Eviction) {
	for _, id := range ids {
		_, err := st.submit(iosched.UnlinkatOp(st.dirFD(id), segmentName(id)), st.evictionResult)
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
	a := &st.activeSegment
	a.Lock()
	defer a.Unlock()
	for i, s := range a.segments {
		if s.id != id {
			continue
		}
		st.stats.diskBytes.Add(-s.size)
		st.stats.segments.Add(-1)
		a.segments = slices.Delete(a.segments, i, i+1)
		return
	}
}
