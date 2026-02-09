package blobcache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"golang.org/x/time/rate"
)

// SegmentCacheReleaseFn is called to release cached segment file handles
// before deleting segment files during compaction.
type SegmentCacheReleaseFn func(segmentID uint32)

// CompactorKnobs provides testing hooks for compaction behavior.
type CompactorKnobs struct {
	// BeforeRelocate is called before each Relocate operation during compaction.
	// Allows tests to inject concurrent modifications to the index.
	BeforeRelocate func(k index.Key)
}

// Compactor handles segment compaction using a copy-forward approach.
// It merges contiguous segments, filters out stale/deleted items, and
// produces a single compacted segment with block-aligned records.
type Compactor struct {
	index             *index.DurableIndex
	segIDs            SegmentIDProvider
	basePath          string
	shards            int
	ioFlags           sys.OpenFlag
	releaseCachedFile SegmentCacheReleaseFn

	// rateLimiter throttles copy_file_range calls to smooth metadata update rate.
	// Each reflink is a metadata operation; without throttling, a burst of hundreds
	// of copy_file_range calls can overwhelm the filesystem. Nil means unlimited.
	rateLimiter *rate.Limiter

	// segmentFiles caches open file handles for source segments.
	// Populated during compaction, closed after each Compact() call.
	segmentFiles map[uint32]segmentFileInfo

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &CompactorKnobs{...}
	Knobs *CompactorKnobs
}

// segmentFileInfo holds a cached file handle for a source segment.
// Reflinks on XFS depend on block-aligned offsets, not O_DIRECT.
type segmentFileInfo struct {
	file *os.File
	size int64
}

// NewCompactor creates a Compactor with the given dependencies.
// releaseCachedFile is called before deleting segment files to release cached handles.
func NewCompactor(
	idx *index.DurableIndex,
	segIDs SegmentIDProvider,
	basePath string,
	shards int,
	ioFlags sys.OpenFlag,
	releaseCachedFile SegmentCacheReleaseFn,
) *Compactor {
	return &Compactor{
		index:             idx,
		segIDs:            segIDs,
		basePath:          basePath,
		shards:            shards,
		ioFlags:           ioFlags,
		releaseCachedFile: releaseCachedFile,
		segmentFiles:      make(map[uint32]segmentFileInfo),
	}
}

// Close releases resources held by the Compactor.
func (c *Compactor) Close() {}

// CompactResult contains the outcome of a compaction operation.
type CompactResult struct {
	NewSegmentID        uint32   // ID of the newly created segment (0 if nothing written)
	OldSegmentIDs       []uint32 // IDs of segments that were compacted
	ItemsCompacted      int      // Number of live items written to new segment
	TombstonesKept      int      // Number of tombstones preserved (older shadow exists)
	TombstonesDropped   int      // Number of tombstones garbage collected (tail only)
	TombstonesDissolved int      // Number of tombstones dissolved (no older shadow)
	StaleSkipped        int      // Number of stale entries skipped (superseded by newer writes)

	// I/O stats for bandwidth analysis
	WriteOps    int   // Number of write operations
	WriteBytes  int64 // Total bytes written via pwrite
	SpliceOps   int   // Number of copy_file_range calls (reflinks on XFS when aligned)
	SpliceBytes int64 // Bytes copied via copy_file_range
	DurationMs  int64 // Total compaction duration in milliseconds
	ThrottleMs  int64 // Time spent waiting for rate limiter tokens

	// Targeted Gravity metrics
	EstimatedInputMB float64 // Pre-compaction estimate of live data (from policy)
	ActualOutputMB   float64 // Actual output size (WriteBytes / 1MB)
}

// relocInfo tracks an item being relocated during compaction.
type relocInfo struct {
	item   index.Item
	footer record.FooterEntry // Original footer entry (avoids re-reading record header)
	oldSeg uint32
	oldOff uint32
}

// Compact merges a contiguous range of segments into a single new segment.
//
// The segmentIDs must be sorted in ascending order and form a contiguous range
// (Strict Contiguity Rule). This prevents the "Leapfrog Hazard" where skipping
// segments could resurrect deleted keys.
//
// Parameters:
//   - segmentIDs: Segment IDs to compact (must be contiguous and ascending)
//   - dropTombstones: If true, tombstones are garbage collected instead of preserved.
//     ONLY set this to true when compacting the tail (oldest) segment range.
//     Otherwise, the Leapfrog Hazard can resurrect deleted keys.
//
// Locking: Acquires shared (read) locks on all segment shards to allow concurrent
// compactions while blocking Delete operations on these segments.
func (c *Compactor) Compact(segmentIDs []uint32, dropTombstones bool) (_ CompactResult, retErr error) {
	result := CompactResult{OldSegmentIDs: segmentIDs}

	if len(segmentIDs) == 0 {
		return result, nil
	}

	// Close cached segment files after compaction completes
	defer func() {
		retErr = errors.Join(retErr, c.closeSegmentFiles())
	}()

	startTime := time.Now()

	// Acquire shared locks on segment shards.
	// RLock allows concurrent compactions while blocking Delete operations.
	// Multiple segments may map to the same shard (segID % 256). Multiple
	// RLocks from the same goroutine are safe: Go's sync.RWMutex uses an
	// additive reader count, so each RLock increments the counter without
	// deadlock. Each RLock must have a matching RUnlock (handled in defer).
	var shards []*sync.RWMutex
	for _, segID := range segmentIDs {
		shard := c.index.SegmentLockShard(segID)
		shard.RLock()
		shards = append(shards, shard)
	}
	defer func() {
		for _, s := range shards {
			s.RUnlock()
		}
	}()

	// Collect items from all segments, checking contiguity as we go
	toRelocate, tombstones, _, staleCount, dissolvedCount, err := c.collectItems(segmentIDs)
	if err != nil {
		return result, err
	}
	result.StaleSkipped = staleCount
	result.TombstonesDissolved = dissolvedCount

	if len(toRelocate) == 0 && len(tombstones) == 0 {
		// Nothing to compact, just drop the old segments
		return result, c.dropSegments(segmentIDs)
	}

	// Allocate new segment ID and write compacted data
	newSegID := c.segIDs.NextSegmentID()
	result.NewSegmentID = newSegID

	footerEntries, err := c.writeCompactedSegment(newSegID, toRelocate, &result)
	if err != nil {
		return result, err
	}

	// Handle tombstones based on dropTombstones flag
	if dropTombstones {
		// Tail segment: GC tombstones (safe - no older data can conflict)
		// CRITICAL: We must remove these from the RAM index, otherwise they become
		// memory leaks (zombie entries pointing to deleted segments).
		for _, ts := range tombstones {
			c.index.Delete(ts.Key)
		}
		result.TombstonesDropped = len(tombstones)
		tombstones = nil // Now safe to clear - won't relocate them
	} else {
		// Non-tail segment: Preserve tombstones (required for crash safety)
		// Add tombstone entries to footer so they survive crash recovery.
		for _, ts := range tombstones {
			entry := record.FooterEntry{
				Key:   ts.Key,
				Pos:   0, // Tombstones have no physical location
				Flags: 0,
			}
			entry.SetDeleted()
			footerEntries = append(footerEntries, entry)
		}
		result.TombstonesKept = len(tombstones)
	}

	// Write .meta file (footer with all items) for crash recovery
	segPath := getSegmentPath(c.basePath, c.shards, newSegID)
	if err := WriteFooter(newSegID, footerEntries, segPath, c.ioFlags); err != nil {
		return result, fmt.Errorf("compaction: write footer: %w", err)
	}

	// Build relocation requests for batch processing
	// Call test hooks before building requests (simulates concurrent modifications)
	requests := make([]index.RelocationRequest, 0, len(toRelocate)+len(tombstones))

	for _, ri := range toRelocate {
		if c.Knobs != nil && c.Knobs.BeforeRelocate != nil {
			c.Knobs.BeforeRelocate(ri.item.Key)
		}
		requests = append(requests, index.RelocationRequest{
			Key:          ri.item.Key,
			OldSegmentID: index.SegmentID(ri.oldSeg),
			OldOffset:    index.Offset(ri.oldOff),
			NewSegmentID: index.SegmentID(ri.item.SegmentID),
			NewOffset:    index.Offset(ri.item.Offset),
			Mode:         index.RelocateLive,
		})
	}

	for _, ts := range tombstones {
		requests = append(requests, index.RelocationRequest{
			Key:          ts.Key,
			OldSegmentID: index.SegmentID(ts.SegmentID),
			OldOffset:    index.Offset(ts.Offset),
			NewSegmentID: index.SegmentID(newSegID),
			NewOffset:    0,
			Mode:         index.RelocateTombstone,
		})
	}

	// Batch relocate: acquires each shard lock exactly once
	c.index.RelocateBatch(requests)

	// Drop old segment metadata and files
	if err := c.dropSegments(segmentIDs); err != nil {
		return result, err
	}

	result.DurationMs = time.Since(startTime).Milliseconds()
	return result, nil
}

// collectItems gathers live items and tombstones from the given segments.
// It validates contiguity as it processes each segment.
// Tombstones are checked against HasOlderShadow for early dissolution.
// Returns: live items to relocate, tombstones (to preserve), max seqID, stale count, dissolved count, error.
func (c *Compactor) collectItems(segmentIDs []uint32) ([]relocInfo, []index.Item, uint64, int, int, error) {
	var toRelocate []relocInfo
	var tombstones []index.Item
	var maxSeqID uint64
	var staleCount int
	var dissolvedCount int
	var prevSegID uint32

	// Floor segment ID (minimum being compacted) for tombstone dissolution decisions
	floorID := segmentIDs[0]

	for i, segID := range segmentIDs {
		// Validate ascending order
		if i > 0 {
			if segID <= prevSegID {
				return nil, nil, 0, 0, 0, fmt.Errorf("compaction: segment IDs must be in ascending order, got %d after %d",
					segID, prevSegID)
			}

			// Contiguity check: segment IDs must be consecutive OR gap must be verified empty
			if segID != prevSegID+1 {
				// Gap detected - verify no segments exist in (prevSegID, segID)
				// This prevents the Leapfrog Hazard where compacting [10, 15] while
				// segment 12 exists would skip segment 12's data.
				if err := c.index.VerifyNoSegmentsInRange(prevSegID, segID); err != nil {
					return nil, nil, 0, 0, 0, err
				}
			}
		}
		prevSegID = segID

		manifest, err := c.index.GetSegmentManifestRaw(segID)
		if err != nil {
			continue // Segment might have been deleted
		}

		if manifest.MaxSeqID > maxSeqID {
			maxSeqID = manifest.MaxSeqID
		}

		for j := range manifest.Entries {
			entry := &manifest.Entries[j]
			item := manifest.Item(j)

			if item.IsDeleted() {
				// Check if tombstone can be safely dissolved (no older shadow exists)
				if !c.index.HasOlderShadow(item.Key, floorID) {
					// No older version exists in any segment before floorID.
					// Safe to dissolve immediately - remove from RAM index.
					c.index.Delete(item.Key)
					dissolvedCount++
					continue
				}
				// Older version may exist - must preserve tombstone
				tombstones = append(tombstones, item)
				continue
			}

			// Staleness check: verify RAM index still points to this location
			ramItem, found := c.index.Get(item.Key)
			if !found || ramItem.SegmentID != item.SegmentID || ramItem.Offset != item.Offset {
				// Stale: RAM has newer version or item was deleted
				staleCount++
				continue
			}

			// Check if item was tombstoned after segment registration.
			// In-memory entries from registration don't reflect later tombstones;
			// the RAM index is the authoritative source for deleted status.
			if ramItem.IsDeleted() {
				if !c.index.HasOlderShadow(item.Key, floorID) {
					c.index.Delete(item.Key)
					dissolvedCount++
					continue
				}
				tombstoneItem := item
				tombstoneItem.SetDeleted()
				tombstones = append(tombstones, tombstoneItem)
				continue
			}

			toRelocate = append(toRelocate, relocInfo{
				item:   item,
				footer: *entry,
				oldSeg: item.SegmentID,
				oldOff: item.Offset,
			})
		}
	}

	return toRelocate, tombstones, maxSeqID, staleCount, dissolvedCount, nil
}

// getSegmentFile returns a cached file handle for a source segment.
// Opens a single buffered fd — reflinks depend on block-aligned offsets, not O_DIRECT.
// Hints FADV_SEQUENTIAL for readahead during copy_file_range on ext4 (RocksDB pattern).
func (c *Compactor) getSegmentFile(segmentID uint32) (segmentFileInfo, error) {
	if info, ok := c.segmentFiles[segmentID]; ok {
		return info, nil
	}

	path := getSegmentPath(c.basePath, c.shards, segmentID)

	f, err := sys.OpenFileForRead(path, 0)
	if err != nil {
		return segmentFileInfo{}, fmt.Errorf("compaction: open segment %d: %w", segmentID, err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return segmentFileInfo{}, fmt.Errorf("compaction: stat segment %d: %w", segmentID, err)
	}

	// Hint sequential access for copy_file_range readahead (effective on ext4).
	// On XFS with reflinks, copy_file_range is metadata-only so this is a no-op.
	if sys.UseFadvise {
		_ = sys.Fadvise(f.Fd(), 0, stat.Size(), sys.FadvSequential)
	}

	info := segmentFileInfo{file: f, size: stat.Size()}
	c.segmentFiles[segmentID] = info
	return info, nil
}

// closeSegmentFiles closes all cached segment file handles.
// Evicts source segment pages from page cache via FADV_DONTNEED before closing
// to prevent compaction from polluting the kernel page cache (RocksDB pattern).
// Called after each compaction to avoid accumulating open files.
func (c *Compactor) closeSegmentFiles() (retErr error) {
	for segID, info := range c.segmentFiles {
		// Evict source pages from page cache — compaction data is cold.
		if sys.UseFadvise {
			_ = sys.Fadvise(info.file.Fd(), 0, info.size, sys.FadvDontNeed)
		}
		if err := info.file.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close segment %d: %w", segID, err))
		}
		delete(c.segmentFiles, segID)
	}
	return retErr
}

// writeCompactedSegment writes records to a new segment with block-aligned offsets.
//
// Records from padded MemTable/WAL are born with block-aligned offsets. The
// compactor preserves this alignment in the destination. On XFS with reflink=1,
// copy_file_range produces shared extents when both offsets are block-aligned,
// making compaction nearly zero-I/O for the data portion.
//
// Legacy segments (pre-padding) have unaligned records: the first compaction does
// a physical copy to normalize offsets, enabling reflinks in subsequent compactions.
func (c *Compactor) writeCompactedSegment(
	newSegID uint32,
	toRelocate []relocInfo,
	result *CompactResult,
) ([]record.FooterEntry, error) {
	// Create destination WITHOUT O_DIRECT or fallocate.
	// copy_file_range operates in-kernel; on XFS with reflink=1, it creates shared
	// extents (metadata-only). Pre-allocating via fallocate would defeat reflinks by
	// giving the destination its own physical blocks before copy_file_range runs.
	segPath := getSegmentPath(c.basePath, c.shards, newSegID)
	f, err := sys.CreateFile(segPath, 0)
	if err != nil {
		return nil, fmt.Errorf("compaction: create segment: %w", err)
	}

	// Write file header at offset 0. First record starts at BlockSize.
	if _, err := f.WriteAt(record.FileHeaderBytes[:], 0); err != nil {
		return nil, errors.Join(
			fmt.Errorf("compaction: write file header: %w", err),
			f.Close(),
		)
	}

	// First record starts at block boundary (past padded file header)
	dstOff := sys.PageAlign(int64(record.FileHeaderSize))
	footerEntries := make([]record.FooterEntry, 0, len(toRelocate))

	for i := range toRelocate {
		ri := &toRelocate[i]
		recordLen := int64(ri.item.PhysicalLen)

		// Destination is always block-aligned (invariant maintained by this loop)
		recordStart := dstOff

		// Copy the record using the appropriate strategy based on source alignment
		seg, err := c.getSegmentFile(ri.oldSeg)
		if err != nil {
			return nil, errors.Join(err, f.Close())
		}
		if err := c.copyRecord(seg, f, int64(ri.oldOff), &dstOff, recordLen, result); err != nil {
			return nil, errors.Join(
				fmt.Errorf("compaction: copy record segment %d offset %d: %w",
					ri.oldSeg, ri.oldOff, err),
				f.Close(),
			)
		}

		// Advance dstOff to next block boundary for the next record
		dstOff = sys.PageAlign(dstOff)

		// Update item with new location
		ri.item.SegmentID = newSegID
		ri.item.Offset = uint32(recordStart)

		// Build footer entry from original manifest data, updating position
		fe := ri.footer
		fe.Pos = recordStart
		footerEntries = append(footerEntries, fe)

		result.ItemsCompacted++
	}

	// Sync only when durability is required (CAS mode with FlDSync/FlSync).
	// In cache mode, old segments survive until explicitly deleted — if we crash
	// before sync completes, recovery replays from the intact source segments.
	var syncErr error
	if c.ioFlags&(sys.FlDSync|sys.FlSync) != 0 {
		syncErr = sys.Fdatasync(f)
	}
	if err := errors.Join(syncErr, f.Close()); err != nil {
		return nil, fmt.Errorf("compaction: sync segment: %w", err)
	}

	return footerEntries, nil
}

// copyRecord copies a single record from source to destination via copy_file_range.
//
// Records from padded MemTable/WAL have block-aligned source offsets, and the
// compactor places them at block-aligned destination offsets. On XFS with reflink=1,
// this produces shared extents (reflinks) for all whole blocks — typically 99%+ of
// a large blob. The kernel handles the trailing partial block as a physical copy.
//
// Legacy segments (pre-padding) have unaligned source offsets: copy_file_range falls
// back to a physical copy. After one compaction pass, output is normalized for future
// reflinks.
func (c *Compactor) copyRecord(
	seg segmentFileInfo,
	dst *os.File,
	srcOff int64,
	dstOff *int64,
	recordLen int64,
	result *CompactResult,
) error {
	// Throttle before issuing the syscall to smooth metadata update rate.
	if c.rateLimiter != nil {
		tokens := min(int(recordLen), c.rateLimiter.Burst())
		waitStart := time.Now()
		_ = c.rateLimiter.WaitN(context.Background(), tokens)
		result.ThrottleMs += time.Since(waitStart).Milliseconds()
	}

	bufSrc := srcOff
	if _, err := copyFileRangeFull(seg.file, dst, &bufSrc, dstOff, int(recordLen)); err != nil {
		return err
	}
	result.SpliceOps++
	result.SpliceBytes += recordLen
	return nil
}

// copyFileRangeFull copies exactly `length` bytes using copy_file_range,
// looping if the kernel returns a short copy.
func copyFileRangeFull(src, dst *os.File, srcOff, dstOff *int64, length int) (int, error) {
	var total int
	for total < length {
		n, err := sys.CopyFileRange(src, dst, srcOff, dstOff, length-total)
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, fmt.Errorf("copy_file_range: zero bytes copied (want %d more)", length-total)
		}
		total += n
	}
	return total, nil
}

// dropSegments removes segment metadata from Bitcask and deletes segment files.
func (c *Compactor) dropSegments(segmentIDs []uint32) (retErr error) {
	for _, segID := range segmentIDs {
		// Drop from Bitcask index
		if err := c.index.DropSegment(segID); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("drop segment %d index: %w", segID, err))
		}

		// Release caller's cached file handle before deleting
		if c.releaseCachedFile != nil {
			c.releaseCachedFile(segID)
		}

		// Delete segment file and footer
		if err := DeleteSegmentFiles(c.basePath, c.shards, segID); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}

	return retErr
}
