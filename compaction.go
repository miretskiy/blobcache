package blobcache

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
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

	// ioBuf is a reusable buffer for small reads/writes (headers, head/tail blocks).
	// Lazily allocated on first use. Regular heap memory (not mmap'd) since
	// it's used with buffered fds, not O_DIRECT.
	ioBuf []byte

	// segmentFiles caches open file handles and sizes for source segments.
	// Each entry has two fds: O_DIRECT (for aligned copy_file_range) and
	// buffered (for header reads and unaligned data).
	// Populated during compaction, closed after each Compact() call.
	segmentFiles map[uint32]segmentFileInfo

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &CompactorKnobs{...}
	Knobs *CompactorKnobs
}

// segmentFileInfo holds cached file handles for a source segment.
// Two fds allow using O_DIRECT for aligned copy_file_range (reflinks on XFS)
// while using buffered I/O for unaligned reads (headers, head/tail of records).
type segmentFileInfo struct {
	direct   *os.File // O_DIRECT — for body copy_file_range at aligned offsets
	buffered *os.File // Buffered — for header reads, head/tail block copies
	size     int64
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
	ReadOps    int   // Number of read operations (header reads + head/tail blocks)
	ReadBytes  int64 // Total bytes read from old segments
	WriteOps   int   // Number of write operations (head/tail block writes)
	WriteBytes int64 // Total bytes written via pwrite (head/tail blocks)
	SpliceOps  int   // Number of copy_file_range calls (zero-copy on aligned sources)
	SpliceBytes int64 // Bytes copied via copy_file_range
	DurationMs int64 // Total compaction duration in milliseconds

	// Targeted Gravity metrics
	EstimatedInputMB float64 // Pre-compaction estimate of live data (from policy)
	ActualOutputMB   float64 // Actual output size (WriteBytes / 1MB)
}

// relocInfo tracks an item being relocated during compaction.
type relocInfo struct {
	item   index.Item
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

		manifest, ok := c.index.GetSegmentManifest(segID)
		if !ok {
			continue // Segment might have been deleted
		}

		if manifest.MaxSeqID > maxSeqID {
			maxSeqID = manifest.MaxSeqID
		}

		for _, item := range manifest.Items {
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

			toRelocate = append(toRelocate, relocInfo{
				item:   item,
				oldSeg: item.SegmentID,
				oldOff: item.Offset,
			})
		}
	}

	return toRelocate, tombstones, maxSeqID, staleCount, dissolvedCount, nil
}

// getSegmentFile returns cached file handles for a source segment.
// Opens two fds: O_DIRECT for aligned copy_file_range, buffered for headers/unaligned reads.
func (c *Compactor) getSegmentFile(segmentID uint32) (segmentFileInfo, error) {
	if info, ok := c.segmentFiles[segmentID]; ok {
		return info, nil
	}

	path := getSegmentPath(c.basePath, c.shards, segmentID)

	// O_DIRECT fd for aligned copy_file_range (reflinks on XFS)
	directFd, err := sys.OpenFileForRead(path, c.ioFlags)
	if err != nil {
		return segmentFileInfo{}, fmt.Errorf("compaction: open segment %d (direct): %w", segmentID, err)
	}

	// Buffered fd for header reads and unaligned head/tail data
	bufferedFd, err := sys.OpenFileForRead(path, 0)
	if err != nil {
		_ = directFd.Close()
		return segmentFileInfo{}, fmt.Errorf("compaction: open segment %d (buffered): %w", segmentID, err)
	}

	stat, err := directFd.Stat()
	if err != nil {
		_ = directFd.Close()
		_ = bufferedFd.Close()
		return segmentFileInfo{}, fmt.Errorf("compaction: stat segment %d: %w", segmentID, err)
	}

	info := segmentFileInfo{direct: directFd, buffered: bufferedFd, size: stat.Size()}
	c.segmentFiles[segmentID] = info
	return info, nil
}

// closeSegmentFiles closes all cached segment file handles.
// Called after each compaction to avoid accumulating open files.
func (c *Compactor) closeSegmentFiles() (retErr error) {
	for segID, info := range c.segmentFiles {
		if err := info.direct.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close segment %d (direct): %w", segID, err))
		}
		if err := info.buffered.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close segment %d (buffered): %w", segID, err))
		}
		delete(c.segmentFiles, segID)
	}
	return retErr
}

// writeCompactedSegment writes records to a new segment with block-aligned offsets.
//
// Alignment normalization: each record starts at a 4KB boundary in the destination.
// This ensures that subsequent compactions can use copy_file_range with O_DIRECT
// for reflink-based zero-copy on XFS.
//
// For each record, the source offset determines the copy strategy:
//
//   - Aligned source (srcOff % 4096 == 0): O_DIRECT copy_file_range for the body
//     (whole blocks → reflinks on XFS), buffered read/write for the tail (< 4KB).
//
//   - Unaligned source: buffered copy_file_range for the entire record (data copy,
//     no reflinks possible). After normalization, the output is aligned for future
//     compactions.
//
// The destination is opened without O_DIRECT. A single fdatasync at the end
// ensures durability.
func (c *Compactor) writeCompactedSegment(
	newSegID uint32,
	toRelocate []relocInfo,
	result *CompactResult,
) ([]record.FooterEntry, error) {
	// Lazy-allocate I/O buffer for headers + head/tail block copies.
	if c.ioBuf == nil {
		c.ioBuf = make([]byte, sys.BlockSize)
	}

	// Calculate total size with alignment padding.
	// Each record starts at a block boundary; the file header is padded to BlockSize.
	totalSize := sys.PageAlign(int64(record.FileHeaderSize))
	for i := range toRelocate {
		totalSize += sys.PageAlign(int64(toRelocate[i].item.PhysicalLen))
	}
	allocSize := sys.PageAlign(totalSize)

	// Create destination WITHOUT O_DIRECT — copy_file_range operates in-kernel.
	segPath := getSegmentPath(c.basePath, c.shards, newSegID)
	f, err := sys.CreateAndAllocateFile(segPath, 0, allocSize)
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

		// Read record header from buffered fd for footer metadata.
		hdr, err := c.readRecordHeader(ri.item)
		if err != nil {
			return nil, errors.Join(err, f.Close())
		}
		result.ReadOps++
		result.ReadBytes += int64(record.HeaderSize)

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

		// Build footer entry from parsed header
		footerEntries = append(footerEntries, record.FooterEntry{
			Key:          ri.item.Key,
			Pos:          recordStart,
			LogicalSize:  hdr.LogicalSize,
			PhysicalSize: hdr.PhysicalSize,
			SeqID:        hdr.SeqID,
			Flags:        hdr.Flags,
			KeyLen:       hdr.KeyLen,
		})

		result.ItemsCompacted++
	}

	// Truncate to exact logical size (removes fallocate padding beyond last record)
	if err := f.Truncate(dstOff); err != nil {
		return nil, errors.Join(
			fmt.Errorf("compaction: truncate: %w", err),
			f.Close(),
		)
	}

	// Single fdatasync + close
	if err := errors.Join(sys.Fdatasync(f), f.Close()); err != nil {
		return nil, fmt.Errorf("compaction: sync segment: %w", err)
	}

	return footerEntries, nil
}

// copyRecord copies a single record from source to destination using a
// Head/Body/Tail sandwich strategy to maximize reflink usage:
//
//	Head: bytes from srcOff to the next block boundary (buffered read/write)
//	Body: whole blocks in the middle (O_DIRECT copy_file_range → reflinks on XFS)
//	Tail: trailing partial block (buffered read/write)
//
// When source is block-aligned (from a prior compaction), Head is empty and the
// entire record minus the tail gets reflinks. When source is unaligned (MemTable
// flush), the head shift aligns the body for reflinks (~99%+ of data for large blobs).
//
// On non-Linux platforms (sys.RequiresAlignment == false), the entire record is
// copied via buffered copy_file_range (emulated as read/write).
func (c *Compactor) copyRecord(
	seg segmentFileInfo,
	dst *os.File,
	srcOff int64,
	dstOff *int64,
	recordLen int64,
	result *CompactResult,
) error {
	if !sys.RequiresAlignment {
		// Non-Linux: no O_DIRECT, no reflinks. Buffered copy for entire record.
		bufSrc := srcOff
		if _, err := copyFileRangeFull(seg.buffered, dst, &bufSrc, dstOff, int(recordLen)); err != nil {
			return err
		}
		result.SpliceOps++
		result.SpliceBytes += recordLen
		return nil
	}

	// Head: partial block from srcOff to next block boundary.
	// Zero when source is already aligned (from prior compaction).
	headLen := int64(0)
	if pad := srcOff & sys.BlockMask; pad != 0 {
		headLen = min(sys.BlockSize-pad, recordLen)
	}

	// Body: whole blocks after head, eligible for O_DIRECT reflinks.
	remaining := recordLen - headLen
	bodyLen := (remaining / sys.BlockSize) * sys.BlockSize
	tailLen := remaining - bodyLen

	// Phase 1: Head — buffered read/write to shift past source misalignment
	if headLen > 0 {
		buf := c.ioBuf[:headLen]
		if _, err := seg.buffered.ReadAt(buf, srcOff); err != nil {
			return fmt.Errorf("read head: %w", err)
		}
		if _, err := dst.WriteAt(buf, *dstOff); err != nil {
			return fmt.Errorf("write head: %w", err)
		}
		result.ReadOps++
		result.ReadBytes += headLen
		result.WriteOps++
		result.WriteBytes += headLen
		*dstOff += headLen
	}

	// Phase 2: Body — O_DIRECT copy_file_range (reflinks on XFS)
	if bodyLen > 0 {
		bodySrc := srcOff + headLen
		if _, err := copyFileRangeFull(seg.direct, dst, &bodySrc, dstOff, int(bodyLen)); err != nil {
			return err
		}
		result.SpliceOps++
		result.SpliceBytes += bodyLen
	}

	// Phase 3: Tail — buffered read/write for trailing partial block
	if tailLen > 0 {
		buf := c.ioBuf[:tailLen]
		tailSrc := srcOff + headLen + bodyLen
		if _, err := seg.buffered.ReadAt(buf, tailSrc); err != nil {
			return fmt.Errorf("read tail: %w", err)
		}
		if _, err := dst.WriteAt(buf, *dstOff); err != nil {
			return fmt.Errorf("write tail: %w", err)
		}
		result.ReadOps++
		result.ReadBytes += tailLen
		result.WriteOps++
		result.WriteBytes += tailLen
		*dstOff += tailLen
	}

	return nil
}

// readRecordHeader reads just the record header from a source segment.
// Uses the buffered fd for a simple ReadAt — no alignment constraints.
func (c *Compactor) readRecordHeader(e index.Item) (record.Header, error) {
	seg, err := c.getSegmentFile(e.SegmentID)
	if err != nil {
		return record.Header{}, err
	}

	buf := c.ioBuf[:record.HeaderSize]
	if _, err := seg.buffered.ReadAt(buf, int64(e.Offset)); err != nil {
		return record.Header{}, fmt.Errorf("compaction: read header segment %d offset %d: %w",
			e.SegmentID, e.Offset, err)
	}

	return record.DecodeHeader(buf)
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
