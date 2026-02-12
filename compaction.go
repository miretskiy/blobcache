package blobcache

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// compactionRun is a page-aligned range of data in the source segment that
// can be copied with a single copy_file_range call. srcOffset and length are
// both block-aligned. The range may include invisible garbage (dead records,
// inter-record padding) — only entries referenced by the index are visible.
type compactionRun struct {
	srcOffset int64                // Page-aligned byte offset in source .seg file
	length    int64                // Page-aligned total bytes to copy
	entries   []record.FooterEntry // Live entries within this run (original source offsets)
}

// RewriteResult holds the outcome of a single segment rewrite.
type RewriteResult struct {
	OldSegID       uint32
	NewSegID       uint32
	LiveItems      int
	TombstonesKept int
	Dissolved      int  // Tombstones dissolved (no older shadow)
	AllDead        bool // Segment was 100% dead → deleted without rewrite
}

// copyFileRangeFull copies exactly length bytes between files using sys.CopyFileRange.
// Loops until all bytes are copied (handles partial writes).
func copyFileRangeFull(src, dst *os.File, srcOff, dstOff *int64, length int) error {
	remaining := length
	for remaining > 0 {
		n, err := sys.CopyFileRange(src, dst, srcOff, dstOff, remaining)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("copy_file_range: zero bytes copied with %d remaining", remaining)
		}
		remaining -= n
	}
	return nil
}

// maxRunGapBytes is the maximum gap between live records that will be absorbed
// into a single run. Small gaps (from deleted records or padding) are cheaper to
// copy as invisible garbage than to split into separate copy_file_range calls.
const maxRunGapBytes = 4 * sys.BlockSize // 16KB — up to 4 pages of dead data

// buildCompactionRuns sorts live entries by source offset, merges nearby records
// (absorbing small gaps of dead data), and aligns each run to page boundaries.
//
// Page alignment enables:
//   - Block-aligned copy_file_range arguments (correct for O_DIRECT on both sides)
//   - Reflink eligibility on XFS (both source and destination page-aligned)
//
// The extra bytes from alignment (up to 2 pages per run) are invisible because
// no index entry points to them. Entries retain their original source offsets;
// the caller computes destination offsets as: dstStart + (srcEntryOff - alignedSrcStart).
func buildCompactionRuns(entries []record.FooterEntry) []compactionRun {
	if len(entries) == 0 {
		return nil
	}

	// Sort by source offset ascending.
	sorted := make([]record.FooterEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Pos < sorted[j].Pos
	})

	// Phase 1: Merge records with small gaps (≤ maxRunGapBytes) into raw runs.
	type rawRun struct {
		start   int64 // First record's offset (unaligned)
		end     int64 // Past last record's end (unaligned)
		entries []record.FooterEntry
	}

	current := rawRun{
		start:   sorted[0].Pos,
		end:     sorted[0].Pos + physicalRecordLen(&sorted[0]),
		entries: []record.FooterEntry{sorted[0]},
	}

	var rawRuns []rawRun
	for i := 1; i < len(sorted); i++ {
		entryStart := sorted[i].Pos
		entryEnd := entryStart + physicalRecordLen(&sorted[i])
		gap := entryStart - current.end

		if gap <= maxRunGapBytes {
			// Absorb gap — dead data between records is invisible.
			current.end = entryEnd
			current.entries = append(current.entries, sorted[i])
		} else {
			rawRuns = append(rawRuns, current)
			current = rawRun{
				start:   entryStart,
				end:     entryEnd,
				entries: []record.FooterEntry{sorted[i]},
			}
		}
	}
	rawRuns = append(rawRuns, current)

	// Phase 2: Align each run to page boundaries.
	// Start rounds DOWN, end rounds UP. Extra bytes are invisible garbage.
	runs := make([]compactionRun, len(rawRuns))
	for i, raw := range rawRuns {
		alignedStart := raw.start &^ sys.BlockMask // Round DOWN
		alignedEnd := sys.PageAlign(raw.end)       // Round UP
		runs[i] = compactionRun{
			srcOffset: alignedStart,
			length:    alignedEnd - alignedStart,
			entries:   raw.entries,
		}
	}

	return runs
}

// physicalRecordLen returns the total on-disk size of a record.
func physicalRecordLen(e *record.FooterEntry) int64 {
	return int64(record.HeaderSize) + int64(e.KeyLen) + e.PhysicalSize
}

// rewriteSegment rewrites a single segment, copying only live records to a new
// segment file using copy_file_range. Block-aligns runs in the output for
// efficient re-compaction (reflinks on XFS).
//
// The caller must hold the segment shard RLock before calling.
func (c *Cache) rewriteSegment(segID uint32) (RewriteResult, error) {
	result := RewriteResult{OldSegID: segID}

	// 1. Get manifest from in-memory cache.
	manifest, err := c.index.GetSegmentManifestRaw(segID)
	if err != nil {
		return result, fmt.Errorf("get manifest for segment %d: %w", segID, err)
	}

	// 2. Classify entries using RAM index as source of truth for current status.
	// The in-memory manifest Entries may not reflect tombstones applied after
	// registration — always check the RAM index for current deleted status.
	var liveEntries []record.FooterEntry
	var tombstoneEntries []record.FooterEntry // Tombstones to preserve in output .meta
	oldestSegID := c.oldestLiveSegmentID.Load()
	isTail := segID == oldestSegID

	for i := range manifest.Entries {
		e := &manifest.Entries[i]

		// Check RAM index for current status.
		item, found := c.index.Peek(e.Key)
		if !found || item.SegmentID != segID || item.Offset != uint32(e.Pos) {
			// Not in RAM, or points to a different segment/offset → stale, skip.
			continue
		}

		if item.IsDeleted() || e.IsDeleted() {
			// Tombstone: check if older shadow exists.
			if !isTail && c.index.HasOlderShadow(e.Key, segID) {
				tombstoneEntries = append(tombstoneEntries, *e)
			} else {
				result.Dissolved++
			}
			continue
		}

		liveEntries = append(liveEntries, *e)
	}

	result.LiveItems = len(liveEntries)
	result.TombstonesKept = len(tombstoneEntries)

	// 3. If 0 live items and 0 tombstones → drop segment entirely.
	if len(liveEntries) == 0 && len(tombstoneEntries) == 0 {
		result.AllDead = true
		return result, nil
	}

	// 4. Build runs from live entries (sorted by source offset, merged contiguous).
	runs := buildCompactionRuns(liveEntries)

	// 5. Open source .seg file with O_DIRECT.
	srcPath := getSegmentPath(c.Path, c.Shards, segID)
	srcFile, err := sys.OpenFileForRead(srcPath, sys.FlDirectIO)
	if err != nil {
		return result, fmt.Errorf("open source segment %d: %w", segID, err)
	}
	defer func() { _ = srcFile.Close() }()

	// 6. Allocate new segment ID.
	newSegID := c.segIDs.NextSegmentID()
	result.NewSegID = newSegID

	// 7. Calculate output file size: header block + page-aligned runs.
	// Runs are already page-aligned, so just sum them.
	dstSize := int64(sys.BlockSize) // File header (padded to 4KB)
	for _, run := range runs {
		dstSize += run.length
	}

	// 8. Create temp output file with O_DIRECT.
	dstPath := getSegmentPath(c.Path, c.Shards, newSegID)
	tmpPath := dstPath + ".compact.tmp"
	dstFile, err := sys.CreateAndAllocateFile(tmpPath, sys.FlDirectIO, dstSize)
	if err != nil {
		return result, fmt.Errorf("create compaction output: %w", err)
	}
	defer func() {
		if dstFile != nil {
			_ = dstFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	// 9. Write file header (padded to 4KB block).
	headerBuf := make([]byte, sys.BlockSize)
	copy(headerBuf, record.FileHeaderBytes[:])
	if _, err := dstFile.WriteAt(headerBuf, 0); err != nil {
		return result, fmt.Errorf("write compaction header: %w", err)
	}

	// 10. Copy page-aligned runs to output.
	// Runs have page-aligned srcOffset and length, so destination offsets are
	// naturally page-aligned (header is one page, each run length is page-aligned).
	// Entry destination = dstRunStart + (srcEntryOff - srcRunStart).
	type entryMapping struct {
		entry     record.FooterEntry // Entry with NEW position
		oldOffset int64              // Original position in source segment
	}
	dstOff := int64(sys.BlockSize)
	var outputMappings []entryMapping

	for _, run := range runs {
		runDstStart := dstOff

		// Compute new positions: same page-relative offset as source.
		for i := range run.entries {
			oldPos := run.entries[i].Pos
			newPos := runDstStart + (oldPos - run.srcOffset)
			run.entries[i].Pos = newPos
			outputMappings = append(outputMappings, entryMapping{
				entry:     run.entries[i],
				oldOffset: oldPos,
			})
		}

		// Copy the run (both offsets and length are page-aligned).
		srcOff := run.srcOffset
		err := copyFileRangeFull(srcFile, dstFile, &srcOff, &dstOff, int(run.length))
		if err != nil {
			return result, fmt.Errorf("copy run at offset %d: %w", run.srcOffset, err)
		}
	}

	// 11. Fdatasync output.
	if err := sys.Fdatasync(dstFile); err != nil {
		return result, fmt.Errorf("fdatasync compaction output: %w", err)
	}

	// 12. Close output + source.
	if err := dstFile.Close(); err != nil {
		return result, fmt.Errorf("close compaction output: %w", err)
	}
	dstFile = nil // prevent deferred cleanup

	// 13. Rename temp → final segment path.
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return result, fmt.Errorf("rename compaction output: %w", err)
	}

	// 14. Write .meta via WriteFooter.
	// Build output entries (live) + tombstones.
	outputEntries := make([]record.FooterEntry, len(outputMappings))
	for i := range outputMappings {
		outputEntries[i] = outputMappings[i].entry
	}

	allEntries := make([]record.FooterEntry, 0, len(outputEntries)+len(tombstoneEntries))
	allEntries = append(allEntries, outputEntries...)
	for i := range tombstoneEntries {
		// Tombstones don't have physical data in the new segment, set Pos to 0.
		tombstoneEntries[i].Pos = 0
		tombstoneEntries[i].PhysicalSize = 0
		tombstoneEntries[i].SetDeleted()
		allEntries = append(allEntries, tombstoneEntries[i])
	}

	if err := WriteFooter(newSegID, allEntries, dstPath, sys.FlDSync); err != nil {
		return result, fmt.Errorf("write compaction footer: %w", err)
	}

	// 15. Register new segment in index.
	c.index.AddSegmentFromEntries(newSegID, allEntries)

	// 16. Relocate live items in RAM index from old→new segment.
	relocations := make([]index.RelocationRequest, len(outputMappings))
	for i := range outputMappings {
		m := &outputMappings[i]
		relocations[i] = index.RelocationRequest{
			Key:          m.entry.Key,
			OldSegmentID: index.SegmentID(segID),
			OldOffset:    index.Offset(m.oldOffset),
			NewSegmentID: index.SegmentID(newSegID),
			NewOffset:    index.Offset(m.entry.Pos),
			Mode:         index.RelocateLive,
		}
	}
	c.index.RelocateBatch(relocations)

	// 17. Drop old segment.
	if err := c.index.DropSegment(segID); err != nil {
		return result, fmt.Errorf("drop old segment %d: %w", segID, err)
	}
	c.archivist.DropSegmentCache(segID)
	if err := DeleteSegmentFiles(c.Path, c.Shards, segID); err != nil {
		return result, fmt.Errorf("delete old segment %d files: %w", segID, err)
	}

	return result, nil
}

// maybeRewriteSegments identifies and rewrites sparse segments in WAL mode.
// Processes all eligible segments per cycle (single-segment ops, bounded work each).
func (c *Cache) maybeRewriteSegments() error {
	// Cooling boundary.
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + index.CoolingPeriodMargin)
	if currentSegID <= coolingGap {
		return nil
	}
	maxEligibleID := currentSegID - coolingGap

	candidates := c.index.GetRewriteCandidates(maxEligibleID, c.CompactionWasteThreshold)
	if len(candidates) == 0 {
		return nil
	}

	var (
		rewritten int
		deleted   int
		dissolved int
		errs      []error
	)

	for _, segID := range candidates {
		// Check segment metadata for 100% dead optimization.
		meta := c.index.GetSegmentMetadata(segID)
		if meta != nil && meta.LiveItemCount == 0 && meta.TombstoneCount > 0 {
			// Check tombstones: can we dissolve them all?
			allDissolvable := true
			if manifest, err := c.index.GetSegmentManifestRaw(segID); err == nil {
				for i := range manifest.Entries {
					e := &manifest.Entries[i]
					if e.IsDeleted() && c.index.HasOlderShadow(e.Key, segID) {
						allDissolvable = false
						break
					}
				}
			}

			if allDissolvable {
				// Pure delete — no rewrite needed.
				shard := c.index.SegmentLockShard(segID)
				shard.Lock()
				if err := c.index.DropSegment(segID); err != nil {
					shard.Unlock()
					errs = append(errs, fmt.Errorf("drop dead segment %d: %w", segID, err))
					continue
				}
				c.archivist.DropSegmentCache(segID)
				shard.Unlock()

				if err := DeleteSegmentFiles(c.Path, c.Shards, segID); err != nil {
					log.Warn("delete dead segment files", "segID", segID, "error", err)
				}
				deleted++
				continue
			}
		}

		// Rewrite segment under shared lock.
		shard := c.index.SegmentLockShard(segID)
		shard.RLock()
		result, err := c.rewriteSegment(segID)
		shard.RUnlock()

		if err != nil {
			errs = append(errs, fmt.Errorf("rewrite segment %d: %w", segID, err))
			continue
		}

		if result.AllDead {
			// rewriteSegment found 0 live + 0 tombstones; drop entirely.
			shard.Lock()
			if err := c.index.DropSegment(segID); err != nil {
				shard.Unlock()
				errs = append(errs, fmt.Errorf("drop dead segment %d: %w", segID, err))
				continue
			}
			c.archivist.DropSegmentCache(segID)
			shard.Unlock()

			if err := DeleteSegmentFiles(c.Path, c.Shards, segID); err != nil {
				log.Warn("delete dead segment files", "segID", segID, "error", err)
			}
			deleted++
		} else {
			rewritten++
		}

		dissolved += result.Dissolved
	}

	if rewritten+deleted > 0 {
		log.Info("segment compaction completed",
			"rewritten", rewritten,
			"deleted", deleted,
			"dissolved_tombstones", dissolved)

		// Update oldest live segment ID.
		oldest := c.index.GetOldestSegmentID()
		if oldest > 0 {
			c.oldestLiveSegmentID.Store(oldest)
		}

		// Trigger bloom rebuild if tombstones were dissolved.
		if dissolved > 0 {
			c.bloomStats.deletions.Add(int64(dissolved))
			if err := c.maybeTriggerBloomRebuild(); err != nil {
				log.Error("bloom rebuild failed after compaction", "error", err)
			}
		}
	}

	return errors.Join(errs...)
}
