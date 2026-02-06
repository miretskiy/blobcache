package blobcache

import (
	"github.com/miretskiy/blobcache/internal/index"
)

func init() {
	// Verify thresholds stay in sync
	if DefaultTombstoneCompactionThreshold != index.TombstoneCompactionThreshold {
		panic("DefaultTombstoneCompactionThreshold != index.TombstoneCompactionThreshold")
	}
}

// SegmentStats holds computed statistics for a single segment.
// Computed on-demand during compaction selection.
type SegmentStats struct {
	SegmentID      uint32
	TombstoneCount int
	LiveItemCount  int
	LiveBytes      int64 // Sum of PhysicalLen for live (non-deleted) items
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

// Merge compaction policy constants.
const (
	// minGravity is the minimum number of segments to merge (floor).
	// Ensures sequential I/O efficiency by avoiding micro-merges.
	minGravity = 4

	// maxGravity is the maximum number of segments to merge (cap).
	// Prevents excessive lock hold times during compaction.
	maxGravity = 32

	// maxOutputMultiplier caps output size at 2x target to prevent "mega-segments".
	maxOutputMultiplier = 2.0
)

// selectSegmentsForTombstoneCompaction returns segment IDs that exceed the tombstone threshold.
// Returns segments sorted by SegmentID (ascending) for deterministic processing.
// Segments still in the "hot zone" (Librarian cache) are excluded via cooling period check.
//
// This uses lazy candidate tracking for O(K) complexity where K is the number of
// pending candidates, instead of O(N) scanning of all segments. Candidates are
// tracked incrementally during UpdateSegmentOnDelete() when they cross the threshold.
func (c *Cache) selectSegmentsForTombstoneCompaction(_ int) []uint32 {
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + coolingPeriodMargin)

	// Avoid underflow: if currentSegID is small, no segments are eligible
	if currentSegID <= coolingGap {
		return nil
	}

	maxEligibleID := currentSegID - coolingGap
	return c.index.GetTombstoneCompactionCandidates(maxEligibleID)
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

// MergeCandidate represents a contiguous range of segments selected for merge compaction.
type MergeCandidate struct {
	SegmentIDs       []uint32 // Contiguous segment IDs to merge
	EstimatedLiveBytes int64    // Sum of live bytes across all segments
}

// selectSegmentsForMerge returns segment ID ranges suitable for merge compaction.
//
// Selection uses a "Targeted Gravity" model with a sliding window accumulator:
//  1. Iterates through cooled segments sorted by ID
//  2. Accumulates contiguous sparse segments until reaching targetOutputSize
//  3. Enforces minimum range size (gravity) based on system sparseness
//  4. Caps output at 2x target to prevent "mega-segments"
//
// Parameters:
//   - targetOutputSize: Target output segment size in bytes (typically WriteBufferSize)
//   - minRangeSize: Minimum number of segments required (dynamic gravity)
//
// Returns slices of contiguous segment IDs that can be passed to Compactor.Compact().
//
// Uses lazy candidate tracking for O(K) complexity where K is the number of
// sparse segments, instead of O(N) scanning of all segments.
func (c *Cache) selectSegmentsForMerge(targetOutputSize int64, minRangeSize int) []MergeCandidate {
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + coolingPeriodMargin)

	// Avoid underflow: if currentSegID is small, no segments are eligible
	if currentSegID <= coolingGap {
		return nil
	}

	maxEligibleID := currentSegID - coolingGap
	sparse := c.index.GetMergeCompactionCandidates(maxEligibleID)

	if len(sparse) == 0 {
		return nil
	}

	// Sliding window accumulator: build ranges targeting outputSize
	maxOutputSize := int64(float64(targetOutputSize) * maxOutputMultiplier)
	var result []MergeCandidate

	var currentIDs []uint32
	var currentBytes int64

	finalizeRange := func() {
		// Only accept ranges meeting minimum gravity.
		// Ranges below minRangeSize are dropped: this prevents micro-merges but means
		// very sparse segments wait until enough contiguous sparse neighbors accumulate.
		// This is intentional - the I/O cost of merging 2-3 sparse segments doesn't pay off.
		if len(currentIDs) >= minRangeSize {
			result = append(result, MergeCandidate{
				SegmentIDs:         currentIDs,
				EstimatedLiveBytes: currentBytes,
			})
		}
		currentIDs = nil
		currentBytes = 0
	}

	for i, seg := range sparse {
		// Check contiguity with previous segment
		if len(currentIDs) > 0 && seg.ID != currentIDs[len(currentIDs)-1]+1 {
			// Gap detected - finalize current range and start new one
			finalizeRange()
		}

		// Check if adding this segment would exceed max output size
		if currentBytes+seg.LiveBytes > maxOutputSize && len(currentIDs) >= minRangeSize {
			// Would exceed ceiling and we have enough segments - finalize
			finalizeRange()
		}

		// Add segment to current range
		currentIDs = append(currentIDs, seg.ID)
		currentBytes += seg.LiveBytes

		// Check if we've reached target size with enough segments
		if currentBytes >= targetOutputSize && len(currentIDs) >= minRangeSize {
			// Target reached - check if next segment would still be contiguous
			// If so, consider including it if it doesn't exceed ceiling
			if i+1 < len(sparse) && sparse[i+1].ID == seg.ID+1 {
				nextBytes := currentBytes + sparse[i+1].LiveBytes
				if nextBytes <= maxOutputSize {
					continue // Include next segment in this range
				}
			}
			finalizeRange()
		}
	}

	// Don't forget the last range
	finalizeRange()

	return result
}

// calculateDynamicGravity computes the minimum segment count based on system sparseness.
//
// The "gravity" increases as the system becomes sparser:
//   - At 50% sparse (ratio=0.5): gravity = ceil(1/0.5) = 2, clamped to 4
//   - At 75% sparse (ratio=0.25): gravity = ceil(1/0.25) = 4
//   - At 87.5% sparse (ratio=0.125): gravity = ceil(1/0.125) = 8
//   - At 97% sparse (ratio=0.03): gravity = ceil(1/0.03) = 34, clamped to 32
//
// Parameters:
//   - physicalSize: Actual disk usage (from stat or approxSize after hole punching)
//   - logicalSize: Tracked logical size (sum of item.PhysicalLen)
//
// Returns a value between minGravity (4) and maxGravity (32).
func calculateDynamicGravity(physicalSize, logicalSize int64) int {
	if logicalSize <= 0 || physicalSize <= 0 {
		return minGravity
	}

	// Ratio of physical to logical (1.0 = fully dense, 0.1 = 90% sparse)
	ratio := float64(physicalSize) / float64(logicalSize)

	// Invert to get gravity: sparser systems need more segments to form dense output
	// Protect against division by zero and very small ratios
	if ratio < 0.01 {
		ratio = 0.01
	}

	gravity := int(1.0/ratio + 0.999) // Ceiling

	// Clamp to bounds
	if gravity < minGravity {
		gravity = minGravity
	}
	if gravity > maxGravity {
		gravity = maxGravity
	}

	return gravity
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
