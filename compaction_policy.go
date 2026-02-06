package blobcache

// coolingPeriodMargin adds safety margin to the cooling period.
// This ensures segments are fully aged out of Librarian before compaction.
const coolingPeriodMargin = 2

// Merge compaction policy constants.
const (
	// minGravity is the minimum number of segments to merge (floor).
	// Ensures sequential I/O efficiency by avoiding micro-merges.
	minGravity = 4

	// maxGravity is the maximum number of segments to merge (cap).
	// Elastic window: at ratio 0.40 ("death zone"), gravity expands to bridge gaps
	// between sparse segments, effectively "healing" the physical layout.
	// 128 balances wide search window vs lock hold times.
	maxGravity = 128

	// maxOutputMultiplier caps output size at 2x target to prevent "mega-segments".
	maxOutputMultiplier = 2.0

	// minOutputMultiplier sets the minimum worthwhile output size.
	// At 0.40 (~25.6MB for 64MB target), we heal fragmentation before ratio hits
	// the "death zone". Too high (0.75) lets fragmentation win.
	minOutputMultiplier = 0.40
)

// selectSegmentsForTombstoneCompaction returns segment IDs that exceed the tombstone threshold.
// Returns segments sorted by SegmentID (ascending) for deterministic processing.
// Segments still in the "hot zone" (Librarian cache) are excluded via cooling period check.
//
// This uses lazy candidate tracking for O(K) complexity where K is the number of
// pending candidates, instead of O(N) scanning of all segments. Candidates are
// tracked incrementally during UpdateSegmentOnDelete() when they cross the threshold.
func (c *Cache) selectSegmentsForTombstoneCompaction() []uint32 {
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + coolingPeriodMargin)

	// Avoid underflow: if currentSegID is small, no segments are eligible
	if currentSegID <= coolingGap {
		return nil
	}

	maxEligibleID := currentSegID - coolingGap
	return c.index.GetTombstoneCompactionCandidates(maxEligibleID)
}

// MergeCandidate represents a contiguous range of segments selected for merge compaction.
type MergeCandidate struct {
	SegmentIDs         []uint32 // Contiguous segment IDs to merge
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
func (c *Cache) selectSegmentsForMerge(targetOutputSize int64, minRangeSize int, wasteThreshold float64) []MergeCandidate {
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + coolingPeriodMargin)

	// Avoid underflow: if currentSegID is small, no segments are eligible
	if currentSegID <= coolingGap {
		return nil
	}

	maxEligibleID := currentSegID - coolingGap
	sparse := c.index.GetMergeCompactionCandidates(maxEligibleID, wasteThreshold)

	if len(sparse) == 0 {
		return nil
	}

	// Sliding window accumulator: build ranges targeting outputSize
	maxOutputSize := int64(float64(targetOutputSize) * maxOutputMultiplier)
	minOutputSize := int64(float64(targetOutputSize) * minOutputMultiplier)
	var result []MergeCandidate

	var currentIDs []uint32
	var currentBytes int64

	finalizeRange := func() {
		// Accept ranges meeting BOTH gravity and size thresholds.
		// Ranges below minRangeSize are dropped: too few segments for I/O efficiency.
		// Ranges below minOutputSize are dropped: not worth the metadata churn.
		// This means sparse segments wait until enough contiguous neighbors with
		// enough live data accumulate. Better to do one large merge than many tiny ones.
		if len(currentIDs) >= minRangeSize && currentBytes >= minOutputSize {
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

// calculateDynamicGravity computes the minimum segment count based on system
// sparseness and average blob size.
//
// The "gravity" increases as the system becomes sparser (elastic window):
//   - At 50% sparse (ratio=0.5): gravity = ceil(1/0.5) = 2, clamped to 4
//   - At 75% sparse (ratio=0.25): gravity = ceil(1/0.25) = 4
//   - At 87.5% sparse (ratio=0.125): gravity = ceil(1/0.125) = 8
//   - At 97% sparse (ratio=0.03): gravity = ceil(1/0.03) = 34
//   - At 99% sparse (ratio=0.01): gravity = ceil(1/0.01) = 100
//
// Size-aware scaling (avgBlobSize):
//   - Large blobs (>=256KB) create fewer, larger holes per eviction — less
//     fragmentation per unit of waste — so gravity doubles (more patient).
//   - Small blobs create many small extents per MB of waste, so gravity stays low.
//   - Linear interpolation between 1.0x (0 bytes) and 2.0x (256KB+).
//
// Returns a value between minGravity (4) and maxGravity (128).
func calculateDynamicGravity(physicalSize, logicalSize, avgBlobSize int64) int {
	if logicalSize <= 0 || physicalSize <= 0 {
		return minGravity
	}

	// Ratio of physical to logical (1.0 = fully dense, 0.1 = 90% sparse)
	// Clamp ratio at 0.008 to prevent runaway gravity (99.2% sparse → gravity=125)
	ratio := max(float64(physicalSize)/float64(logicalSize), 0.008)

	// Invert to get gravity: sparser systems need more segments to form dense output
	gravity := int(1.0/ratio + 0.999) // Ceiling

	// Size-aware scaling: large blobs need wider search since each eviction
	// frees more bytes per hole punch, reducing fragmentation per unit of waste.
	if avgBlobSize > 0 {
		sizeMultiplier := 1.0 + min(float64(avgBlobSize)/float64(256*1024), 1.0)
		gravity = int(float64(gravity) * sizeMultiplier)
	}

	// Clamp to bounds [minGravity, maxGravity]
	return max(minGravity, min(gravity, maxGravity))
}

// dynamicMergeThreshold returns the waste ratio threshold for merge compaction
// based on average blob size.  Large blobs create fewer, larger holes per
// eviction so we can afford to wait longer before merging.
//
//   - 4KB blobs:    0.90 (many small extents per MB)
//   - 64KB blobs:   0.90 (transition point)
//   - 256KB+ blobs: 0.95 (few large extents, less fragmentation)
func dynamicMergeThreshold(avgBlobSize int64) float64 {
	if avgBlobSize >= 256*1024 {
		return 0.95
	}
	if avgBlobSize >= 64*1024 {
		ratio := float64(avgBlobSize-64*1024) / float64(192*1024) // 256K-64K = 192K
		return 0.90 + ratio*0.05
	}
	return 0.90
}

// recalculateOldestSegmentID updates oldestLiveSegmentID from the in-memory registry.
// Should be called after segment merge compaction drops old segments.
//
// O(1) lookup from the segment registry (no disk scanning).
// Thread-safety: Safe to call concurrently. Uses atomic store.
func (c *Cache) recalculateOldestSegmentID() {
	c.oldestLiveSegmentID.Store(c.index.GetOldestSegmentID())
}
