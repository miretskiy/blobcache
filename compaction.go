package blobcache

import (
	"errors"
	"fmt"
	"time"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// SegmentReader provides read access to segment files.
type SegmentReader interface {
	// ReadBlobRaw reads raw record bytes from a segment. No interpretation.
	ReadBlobRaw(e index.Item) ([]byte, Releaser, error)

	// DropSegmentCache closes and removes a segment's cached file handle.
	// Called before deleting segment files.
	DropSegmentCache(segmentID uint32)
}

// CompactorKnobs provides testing hooks for compaction behavior.
type CompactorKnobs struct {
	// BeforeRelocate is called before each Relocate operation during compaction.
	// Allows tests to inject concurrent modifications to the index.
	BeforeRelocate func(k index.Key)
}

// CompactBufferSize is the size of the aligned buffer used for compaction writes.
// Large enough to batch many blobs, small enough to not waste memory.
const CompactBufferSize = 64 * 1024 * 1024 // 64MB

// Compactor handles segment compaction using a copy-forward approach.
// It merges contiguous segments, filters out stale/deleted items, and
// produces a single compacted segment.
type Compactor struct {
	index      *index.DurableIndex
	reader     SegmentReader
	segIDs     SegmentIDProvider
	basePath   string
	shards     int
	ioFlags    sys.OpenFlag
	footerPool poolProvider

	// compactBuf is a reusable aligned buffer for O_DIRECT compaction writes.
	// Lazily allocated on first use, reused across compaction cycles.
	compactBuf *MmapBuffer

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &CompactorKnobs{...}
	Knobs *CompactorKnobs
}

// NewCompactor creates a Compactor with the given dependencies.
func NewCompactor(
	idx *index.DurableIndex,
	reader SegmentReader,
	segIDs SegmentIDProvider,
	basePath string,
	shards int,
	ioFlags sys.OpenFlag,
	footerPool poolProvider,
) *Compactor {
	return &Compactor{
		index:      idx,
		reader:     reader,
		segIDs:     segIDs,
		basePath:   basePath,
		shards:     shards,
		ioFlags:    ioFlags,
		footerPool: footerPool,
	}
}

// Close releases resources held by the Compactor.
// Must be called when the Compactor is no longer needed.
func (c *Compactor) Close() {
	if c.compactBuf != nil {
		c.compactBuf.Unpin()
		c.compactBuf = nil
	}
}

// CompactResult contains the outcome of a compaction operation.
type CompactResult struct {
	NewSegmentID      uint32   // ID of the newly created segment (0 if nothing written)
	OldSegmentIDs     []uint32 // IDs of segments that were compacted
	ItemsCompacted    int      // Number of live items written to new segment
	TombstonesKept    int      // Number of tombstones preserved
	TombstonesDropped int      // Number of tombstones garbage collected (tail only)
	StaleSkipped      int      // Number of stale entries skipped (superseded by newer writes)

	// I/O stats for bandwidth analysis
	ReadOps    int   // Number of read operations
	ReadBytes  int64 // Total bytes read from old segments
	WriteOps   int   // Number of write operations
	WriteBytes int64 // Total bytes written to new segment
	DurationMs int64 // Total compaction duration in milliseconds
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
func (c *Compactor) Compact(segmentIDs []uint32, dropTombstones bool) (CompactResult, error) {
	result := CompactResult{OldSegmentIDs: segmentIDs}

	if len(segmentIDs) == 0 {
		return result, nil
	}

	startTime := time.Now()

	// Acquire shared locks (RLock allows concurrent compactions, blocks Delete)
	// Multiple segments may map to same shard - multiple RLocks is fine
	var shards []*xmap.Shard[index.SegmentMetadata, xmap.Pad32]
	for _, segID := range segmentIDs {
		shard := c.index.SegmentMetaShard(segID)
		shard.RLock()
		shards = append(shards, shard)
	}
	defer func() {
		for _, s := range shards {
			s.RUnlock()
		}
	}()

	// Collect items from all segments, checking contiguity as we go
	toRelocate, tombstones, _, staleCount, err := c.collectItems(segmentIDs)
	if err != nil {
		return result, err
	}
	result.StaleSkipped = staleCount

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
// Returns: live items to relocate, tombstones, max seqID, stale count, error.
func (c *Compactor) collectItems(segmentIDs []uint32) ([]relocInfo, []index.Item, uint64, int, error) {
	var toRelocate []relocInfo
	var tombstones []index.Item
	var maxSeqID uint64
	var staleCount int
	var prevSegID uint32

	for i, segID := range segmentIDs {
		// Validate ascending order
		if i > 0 {
			if segID <= prevSegID {
				return nil, nil, 0, 0, fmt.Errorf("compaction: segment IDs must be in ascending order, got %d after %d",
					segID, prevSegID)
			}

			// Contiguity check: segment IDs must be consecutive OR gap must be verified empty
			if segID != prevSegID+1 {
				// Gap detected - verify no segments exist in (prevSegID, segID)
				// This prevents the Leapfrog Hazard where compacting [10, 15] while
				// segment 12 exists would skip segment 12's data.
				if err := c.index.VerifyNoSegmentsInRange(prevSegID, segID); err != nil {
					return nil, nil, 0, 0, err
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
				// Preserve tombstones for crash safety
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

	return toRelocate, tombstones, maxSeqID, staleCount, nil
}

// writeCompactedSegment creates the new segment file and writes all live blobs.
// Returns footer entries for the .meta file.
//
// Uses a single aligned buffer (compactBuf) to accumulate data and write in
// O_DIRECT-compatible aligned chunks. This avoids page cache pollution and
// ensures proper alignment for Direct I/O.
func (c *Compactor) writeCompactedSegment(
	newSegID uint32,
	toRelocate []relocInfo,
	result *CompactResult,
) ([]record.FooterEntry, error) {
	// Lazy-allocate compaction buffer on first use
	if c.compactBuf == nil {
		c.compactBuf = c.footerPool.AcquireAligned(CompactBufferSize)
	}
	buf := c.compactBuf.Bytes()

	// Calculate total size for fallocate (round up to block alignment)
	totalSize := int64(record.FileHeaderSize)
	for i := range toRelocate {
		totalSize += int64(toRelocate[i].item.PhysicalLen)
	}
	allocSize := (totalSize + sys.BlockMask) &^ sys.BlockMask

	// Create segment file with O_DIRECT
	segPath := getSegmentPath(c.basePath, c.shards, newSegID)
	f, err := sys.CreateAndAllocateFile(segPath, c.ioFlags, allocSize)
	if err != nil {
		return nil, fmt.Errorf("compaction: create segment file: %w", err)
	}

	// Track buffer position and logical file offset
	bufPos := 0
	logicalOffset := uint32(0)
	footerEntries := make([]record.FooterEntry, 0, len(toRelocate))

	// Helper to flush aligned portion of buffer
	flushAligned := func() error {
		if bufPos == 0 {
			return nil
		}
		// Round down to block boundary
		alignedLen := bufPos &^ int(sys.BlockMask)
		if alignedLen == 0 {
			return nil // Not enough data for aligned write yet
		}

		n, err := sys.WriteAligned(buf[:alignedLen], f, c.ioFlags)
		if err != nil {
			return fmt.Errorf("compaction: write aligned chunk: %w", err)
		}
		result.WriteOps++
		result.WriteBytes += int64(n)

		// Move unaligned tail to start of buffer
		tail := bufPos - alignedLen
		if tail > 0 {
			copy(buf[0:tail], buf[alignedLen:bufPos])
		}
		bufPos = tail
		return nil
	}

	// Copy file header into buffer
	copy(buf[bufPos:], record.FileHeaderBytes[:])
	bufPos += record.FileHeaderSize
	logicalOffset = uint32(record.FileHeaderSize)

	// Process each blob: read, accumulate in buffer, flush when full
	for i := range toRelocate {
		ri := &toRelocate[i]

		// Read raw record from old segment
		data, releaser, err := c.reader.ReadBlobRaw(ri.item)
		if err != nil {
			releaser.Release()
			return nil, errors.Join(
				fmt.Errorf("compaction: read blob from segment %d: %w", ri.oldSeg, err),
				f.Close(),
			)
		}
		result.ReadOps++
		result.ReadBytes += int64(len(data))

		// Parse header to extract footer metadata
		hdr, err := record.DecodeHeader(data[:record.HeaderSize])
		if err != nil {
			releaser.Release()
			return nil, errors.Join(
				fmt.Errorf("compaction: decode header from segment %d: %w", ri.oldSeg, err),
				f.Close(),
			)
		}

		// Check if blob fits in remaining buffer space
		if bufPos+len(data) > len(buf) {
			// Flush aligned portion to make room
			if err := flushAligned(); err != nil {
				releaser.Release()
				return nil, errors.Join(err, f.Close())
			}

			// If blob is larger than buffer, we have a problem
			// (shouldn't happen with 64MB buffer and typical blob sizes)
			if bufPos+len(data) > len(buf) {
				releaser.Release()
				return nil, errors.Join(
					fmt.Errorf("compaction: blob size %d exceeds buffer capacity %d", len(data), len(buf)-bufPos),
					f.Close(),
				)
			}
		}

		// Copy blob data into buffer
		copy(buf[bufPos:], data)
		releaser.Release()

		// Update item with new location
		ri.item.SegmentID = newSegID
		ri.item.Offset = logicalOffset

		// Build footer entry
		footerEntries = append(footerEntries, record.FooterEntry{
			Key:          ri.item.Key,
			Pos:          int64(logicalOffset),
			LogicalSize:  hdr.LogicalSize,
			PhysicalSize: hdr.PhysicalSize,
			SeqID:        hdr.SeqID,
			Flags:        hdr.Flags,
			KeyLen:       hdr.KeyLen,
		})

		bufPos += len(data)
		logicalOffset += uint32(len(data))
		result.ItemsCompacted++
	}

	// Final flush: pad to alignment and write remaining data
	if bufPos > 0 {
		alignedLen := (bufPos + int(sys.BlockMask)) &^ int(sys.BlockMask)
		// Zero the padding bytes
		for i := bufPos; i < alignedLen; i++ {
			buf[i] = 0
		}

		n, err := sys.WriteAligned(buf[:alignedLen], f, c.ioFlags)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("compaction: write final chunk: %w", err), f.Close())
		}
		result.WriteOps++
		result.WriteBytes += int64(n)
	}

	// Truncate to exact logical size (removes padding bytes)
	if err := f.Truncate(totalSize); err != nil {
		return nil, errors.Join(fmt.Errorf("compaction: truncate to exact size: %w", err), f.Close())
	}

	// Sync and close
	if err := errors.Join(sys.SyncFile(f, c.ioFlags), f.Close()); err != nil {
		return nil, fmt.Errorf("compaction: sync segment: %w", err)
	}

	return footerEntries, nil
}

// dropSegments removes segment metadata from Bitcask and deletes segment files.
func (c *Compactor) dropSegments(segmentIDs []uint32) error {
	var errs []error

	for _, segID := range segmentIDs {
		// Drop from Bitcask index
		if err := c.index.DropSegment(segID); err != nil {
			errs = append(errs, fmt.Errorf("drop segment %d index: %w", segID, err))
		}

		// Close cached file handle before deleting
		c.reader.DropSegmentCache(segID)

		// Delete segment file and footer
		if err := DeleteSegmentFiles(c.basePath, c.shards, segID); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
