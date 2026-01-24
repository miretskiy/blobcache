package blobcache

import (
	"slices"

	"github.com/miretskiy/blobcache/internal/index"
)

// SegmentStats holds computed statistics for a single segment.
// Computed on-demand during compaction selection.
type SegmentStats struct {
	SegmentID      uint32
	TombstoneCount int
	LiveItemCount  int
	// Future: PhysicalBytes, LogicalBytes for merge compaction
}

// WasteRatio returns the proportion of tombstones (0.0 to 1.0).
// Returns 0 if segment is empty.
func (s SegmentStats) WasteRatio() float64 {
	total := s.TombstoneCount + s.LiveItemCount
	if total == 0 {
		return 0
	}
	return float64(s.TombstoneCount) / float64(total)
}

// DefaultTombstoneCompactionThreshold is the minimum tombstone count to trigger compaction.
// Segments with fewer tombstones are not worth the I/O overhead.
const DefaultTombstoneCompactionThreshold = 100

// coolingPeriodMargin adds safety margin to the cooling period.
// This ensures segments are fully aged out of Librarian before compaction.
const coolingPeriodMargin = 2

// isEligibleForCompaction returns true if a segment has "cooled" enough to be compacted.
// The cooling period ensures segments still in Librarian's cache are not compacted,
// preventing dangling references when segment files are deleted.
//
// The boundary is: segID + coolingGap < currentSegID
// where coolingGap = MaxCachedSlabs + margin
func (c *Cache) isEligibleForCompaction(segID uint32) bool {
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + coolingPeriodMargin)

	// Avoid underflow: if currentSegID is small, no segments are eligible
	if currentSegID <= coolingGap {
		return false
	}
	return segID < currentSegID-coolingGap
}

// computeSegmentStats scans all segments and computes statistics.
// Returns a map from SegmentID to stats.
func (c *Cache) computeSegmentStats() (map[uint32]*SegmentStats, error) {
	stats := make(map[uint32]*SegmentStats)

	err := c.index.ForEachSegment(func(m index.DurableBatch) bool {
		ss, ok := stats[m.SegmentID]
		if !ok {
			ss = &SegmentStats{SegmentID: m.SegmentID}
			stats[m.SegmentID] = ss
		}

		for _, item := range m.Items {
			if item.IsDeleted() {
				ss.TombstoneCount++
			} else {
				ss.LiveItemCount++
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return stats, nil
}

// selectSegmentsForTombstoneCompaction returns segment IDs that exceed the tombstone threshold.
// Returns segments sorted by SegmentID (ascending) for deterministic processing.
// Segments still in the "hot zone" (Librarian cache) are excluded via cooling period check.
func (c *Cache) selectSegmentsForTombstoneCompaction(minTombstones int) ([]uint32, error) {
	stats, err := c.computeSegmentStats()
	if err != nil {
		return nil, err
	}

	var selected []uint32
	for _, ss := range stats {
		if ss.TombstoneCount >= minTombstones && c.isEligibleForCompaction(ss.SegmentID) {
			selected = append(selected, ss.SegmentID)
		}
	}

	// Sort for deterministic processing order
	slices.Sort(selected)

	return selected, nil
}

// selectContiguousRanges groups segment IDs into contiguous ranges for merge compaction.
// A contiguous range is a sequence where each segment ID is exactly 1 more than the previous.
//
// Example: [1, 2, 3, 7, 8, 10] -> [[1,2,3], [7,8], [10]]
//
// This respects the Strict Contiguity Rule required by Compactor.Compact().
func selectContiguousRanges(segmentIDs []uint32) [][]uint32 {
	if len(segmentIDs) == 0 {
		return nil
	}

	var ranges [][]uint32
	current := []uint32{segmentIDs[0]}

	for i := 1; i < len(segmentIDs); i++ {
		if segmentIDs[i] == segmentIDs[i-1]+1 {
			// Contiguous - extend current range
			current = append(current, segmentIDs[i])
		} else {
			// Gap - start new range
			ranges = append(ranges, current)
			current = []uint32{segmentIDs[i]}
		}
	}

	// Don't forget the last range
	ranges = append(ranges, current)

	return ranges
}

// selectSegmentsForMerge returns segment ID ranges suitable for merge compaction.
// Selection criteria: segments that are sparse (mostly hole-punched) and form
// contiguous ranges of at least minRangeSize.
//
// Returns slices of contiguous segment IDs that can be passed to Compactor.Compact().
// Segments still in the "hot zone" (Librarian cache) are excluded via cooling period check.
// Future: Will use PhysicalBytes/LogicalBytes ratio from stat.Blocks.
func (c *Cache) selectSegmentsForMerge(maxWasteRatio float64, minRangeSize int) ([][]uint32, error) {
	stats, err := c.computeSegmentStats()
	if err != nil {
		return nil, err
	}

	// Collect segments with high waste ratio that have cooled
	var sparse []uint32
	for _, ss := range stats {
		if ss.WasteRatio() >= maxWasteRatio && c.isEligibleForCompaction(ss.SegmentID) {
			sparse = append(sparse, ss.SegmentID)
		}
	}

	// Sort for contiguity analysis
	slices.Sort(sparse)

	// Group into contiguous ranges
	ranges := selectContiguousRanges(sparse)

	// Filter by minimum range size
	var result [][]uint32
	for _, r := range ranges {
		if len(r) >= minRangeSize {
			result = append(result, r)
		}
	}

	return result, nil
}

// recalculateOldestSegmentID scans all segments and updates oldestLiveSegmentID.
// Should be called after segment merge compaction drops old segments.
//
// Thread-safety: Safe to call concurrently. Uses atomic store.
func (c *Cache) recalculateOldestSegmentID() error {
	var oldest uint32

	err := c.index.ForEachSegment(func(m index.DurableBatch) bool {
		if oldest == 0 || m.SegmentID < oldest {
			oldest = m.SegmentID
		}
		return true
	})
	if err != nil {
		return err
	}

	c.oldestLiveSegmentID.Store(oldest)
	return nil
}
