package blobstore

import (
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"
)

// IDs are monotonic counters, independent of wall-clock adjustments. Files
// are <low byte, 2 hex digits>/<ID, 16 hex digits>.seg.
const (
	shardCount = 256
	extSegment = ".seg"
)

func shardName(shard int) string   { return fmt.Sprintf("%02x", shard) }
func segmentName(id uint64) string { return fmt.Sprintf("%016x%s", id, extSegment) }
func segmentPath(root string, id uint64) string {
	return filepath.Join(root, shardName(int(id%shardCount)), segmentName(id))
}

// segment contains lifecycle metadata, not a persistent in-memory record
// index. entries belongs to the active writer and is released at sealing.
// Appends and footer entries are guarded by the owner's writer lock. Size
// changes also hold the common registry lock. Completion publishes done.
type segment struct {
	owner     *ioQueue // nil for recovered segments
	id        uint64
	slot      uint32
	pos, size int64
	entries   []footerEntry
	done      atomic.Bool // all seal operations have completed, successfully or not
}

// newSegment reserves a globally unique ID and capacity for a producer. It
// returns lifecycle work for the caller to perform after releasing its lock.
func (st *Store) newSegment(owner *ioQueue, slot uint32) (*segment, Eviction, *segment, error) {
	st.segments.Lock()
	defer st.segments.Unlock()
	if st.segments.nextID == math.MaxUint64 {
		return nil, nil, nil, fmt.Errorf("blobstore: segment IDs exhausted")
	}
	evicted := st.evictLocked(st.cfg.segmentSize, 1)
	if !st.fitsLocked(st.cfg.segmentSize, 1) {
		return nil, evicted, st.blockerLocked(), ErrBusy
	}
	s := &segment{id: st.segments.nextID, owner: owner, slot: slot, size: st.cfg.segmentSize}
	st.segments.nextID++
	st.segments.fifo = append(st.segments.fifo, s)
	st.stats.diskBytes.Add(s.size)
	st.stats.segments.Add(1)
	st.open.Add(1)
	return s, evicted, nil, nil
}

// growSegment reserves a last record's extension before its producer submits
// the write and seal. The producer holds its own mutex throughout this call.
func (st *Store) growSegment(s *segment, extra int64) (Eviction, *segment, error) {
	st.segments.Lock()
	defer st.segments.Unlock()
	evicted := st.evictLocked(extra, 0)
	if !st.fitsLocked(extra, 0) {
		return evicted, st.blockerLocked(), ErrBusy
	}
	st.stats.diskBytes.Add(extra)
	s.size += extra
	return evicted, nil, nil
}

// segmentSealed is the producer's completion notification to the registry.
// No callbacks, locks or I/O run here, including on the synchronous POSIX path.
func (st *Store) segmentSealed(s *segment, err error) {
	if err != nil {
		st.stats.failedSegments.Add(1)
	}
	s.done.Store(true)
}
