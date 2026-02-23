package blobcache

import (
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// ReadSlab extends SharedSlab with hit-tracking for slab-level eviction.
// Once sealed (published to the read cache's sealed list), the SharedSlab
// fields are read-only. Only visitedCount and visited are mutated, atomically.
type ReadSlab struct {
	SharedSlab
	totalItems   int32         // Set once when slab is sealed (immutable after)
	visitedCount atomic.Int32  // Sampled increments (1/16 probability) on cache hit
	createdAt    int64         // UnixNano timestamp for age-based tiebreaking
	visited      *bloom.Filter // Per-item visited tracking (stays mutable, atomic Add)
}

// visitSampleShift controls the probabilistic sampling rate for RecordHit.
// With shift=4, we sample 1/16 of hits, reducing atomic cache line bouncing
// by 16x under high concurrency (5000+ goroutines on a hot slab).
const visitSampleShift = 4

// visitSampleFactor is 1 << visitSampleShift, used to scale visitedCount
// back to estimated actual hits in ColdScore.
const visitSampleFactor = 1 << visitSampleShift

// ColdScore returns the fraction of items NOT visited since the last reset.
// Range [0.0, 1.0]. Higher = colder = evict first.
// Returns 1.0 for empty slabs (evict immediately).
//
// The visitedCount is sampled at 1/16 rate, so we multiply by 16 to estimate
// actual visits. This can exceed totalItems (more visits than items), in which
// case we return 0.0 (fully hot).
func (rs *ReadSlab) ColdScore() float64 {
	total := rs.totalItems
	if total == 0 {
		return 1.0
	}
	estimatedVisits := int32(rs.visitedCount.Load()) * visitSampleFactor
	if estimatedVisits >= total {
		return 0.0
	}
	return 1.0 - float64(estimatedVisits)/float64(total)
}

// RecordHit uses probabilistic counting to track cache hits.
// Only increments the atomic counter for 1/16 of keys, eliminating
// CPU cache line bouncing under high concurrency.
//
// Uses the already-computed xxhash128 key bits as the random source —
// zero additional cost since the hash is in hand at every call site.
// This samples a uniform 1/16 of distinct keys rather than 1/16 of hits,
// which is ideal for slab-level cold/hot ratio estimation.
func (rs *ReadSlab) RecordHit(h Key) {
	// Per-item visited flag for CompactStrategy (always set, not sampled).
	rs.visited.Add(h)

	// Per-slab aggregate counter for ColdScore (sampled 1/16).
	if h.Lo&(visitSampleFactor-1) == 0 {
		rs.visitedCount.Add(1)
	}
}

// EvictionStrategy determines what happens to items in an evicted slab.
type EvictionStrategy interface {
	// BeforeEvict is called before a slab is wiped.
	// victim is the slab about to be evicted (still readable).
	// dst is the current active slab (may be nil).
	// Returns the number of items preserved.
	BeforeEvict(victim *ReadSlab, dst *ActiveSlab) int
}

// DropStrategy simply drops all items. Zero overhead. This is the default.
type DropStrategy struct{}

// BeforeEvict does nothing — all items are dropped.
func (DropStrategy) BeforeEvict(_ *ReadSlab, _ *ActiveSlab) int { return 0 }

// CompactStrategy copies visited (proven-hot) items from the victim slab into
// the destination active slab before eviction. Items start with visited=0 in
// the new slab — they must prove they're hot again (SIEVE invariant).
//
// This prevents the "popular item eviction" problem where coarse-grained
// slab eviction drops hot items mixed with cold ones.
type CompactStrategy struct{}

// BeforeEvict iterates the victim's index, checks the per-item visited bloom,
// and copies visited items into dst. Returns the number of preserved items.
func (CompactStrategy) BeforeEvict(victim *ReadSlab, dst *ActiveSlab) int {
	if dst == nil {
		return 0
	}

	preserved := 0
	buf := victim.buf.Bytes()

	victim.index.ForEach(func(key xmap.Key, entry SlabEntry, _ *xmap.Pad32) bool {
		if !victim.visited.Test(key) {
			return true // Not visited — drop
		}

		totalSize := entry.Header.TotalSize()
		if totalSize <= 0 {
			return true
		}

		// Extract raw record from victim's mmap buffer.
		pos := int(entry.Pos)
		if pos+totalSize > len(buf) {
			return true // Shouldn't happen, but guard
		}
		rawRecord := buf[pos : pos+totalSize]

		// Allocate in destination slab and copy.
		dstBuf, dstPos := dst.Alloc(totalSize)
		if dstBuf == nil {
			return false // Destination full — stop compaction
		}
		copy(dstBuf, rawRecord)

		// Re-index in destination (visited=0 — must prove hot again).
		dstEntry := SlabEntry{
			Header: entry.Header,
			Pos:    dstPos,
		}
		dst.bloom.Add(key)
		dst.index.Put(key, dstEntry)
		preserved++
		return true
	})

	return preserved
}

// selectVictim returns the index of the coldest slab in the list.
// Returns -1 if the list is empty.
// On tie, evicts the oldest slab (smallest createdAt).
func selectVictim(slabs []*ReadSlab) int {
	if len(slabs) == 0 {
		return -1
	}

	coldest := 0
	coldestScore := slabs[0].ColdScore()

	for i := 1; i < len(slabs); i++ {
		score := slabs[i].ColdScore()
		if score > coldestScore ||
			(score == coldestScore && slabs[i].createdAt < slabs[coldest].createdAt) {
			coldest = i
			coldestScore = score
		}
	}

	return coldest
}

// decayVisitedCounts halves the visitedCount on all slabs in the list.
// Called periodically from the maintenance worker to prevent ColdScore
// convergence — without decay, long-lived slabs accumulate visits and
// become unevictable even after their data goes cold.
//
// Note: The Load-then-Store is intentionally non-atomic. A concurrent
// RecordHit between Load and Store may be lost. This is acceptable:
// the counter is a probabilistic estimate (1/16 sampled) used only for
// coarse slab-level eviction scoring. The bias makes slabs appear
// slightly colder, which is the safe direction (favors eviction over
// keeping stale data).
func decayVisitedCounts(slabs []*ReadSlab) {
	for _, slab := range slabs {
		current := slab.visitedCount.Load()
		slab.visitedCount.Store(current / 2)
	}
}

// newReadSlab wraps a SharedSlab as a ReadSlab with eviction metadata.
// The visited bloom is sized to the actual item count with 1% FPR.
// Minimum 16 items to avoid degenerate bloom filter sizing.
func newReadSlab(ss SharedSlab, totalItems int32) *ReadSlab {
	n := uint(totalItems)
	if n < 16 {
		n = 16
	}
	return &ReadSlab{
		SharedSlab: ss,
		totalItems: totalItems,
		createdAt:  time.Now().UnixNano(),
		visited:    bloom.New(n, 0.01),
	}
}
