package blobcache

import (
	"errors"
	"fmt"

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

// CompactResult contains the outcome of a compaction operation.
type CompactResult struct {
	NewSegmentID      uint32   // ID of the newly created segment (0 if nothing written)
	OldSegmentIDs     []uint32 // IDs of segments that were compacted
	ItemsCompacted    int      // Number of live items written to new segment
	TombstonesKept    int      // Number of tombstones preserved
	TombstonesDropped int      // Number of tombstones garbage collected (tail only)
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
	toRelocate, tombstones, maxSeqID, err := c.collectItems(segmentIDs)
	if err != nil {
		return result, err
	}

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

	// Build new items list
	newItems := make([]index.Item, 0, len(toRelocate)+len(tombstones))
	for i := range toRelocate {
		newItems = append(newItems, toRelocate[i].item)
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
		for _, ts := range tombstones {
			ts.SegmentID = newSegID
			ts.Offset = 0 // Tombstones have no data
			newItems = append(newItems, ts)
			result.TombstonesKept++
		}
	}

	// Write footer file for crash recovery
	segPath := getSegmentPath(c.basePath, c.shards, newSegID)
	if err := WriteFooter(newSegID, footerEntries, segPath, c.footerPool, c.ioFlags); err != nil {
		return result, fmt.Errorf("compaction: write footer: %w", err)
	}

	// Write index entries to Bitcask
	if err := c.index.CompactBatch(newSegID, newItems, maxSeqID); err != nil {
		return result, fmt.Errorf("compaction: write index: %w", err)
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

	return result, nil
}

// collectItems gathers live items and tombstones from the given segments.
// It validates contiguity as it processes each segment.
func (c *Compactor) collectItems(segmentIDs []uint32) ([]relocInfo, []index.Item, uint64, error) {
	var toRelocate []relocInfo
	var tombstones []index.Item
	var maxSeqID uint64
	var prevSegID uint32

	for i, segID := range segmentIDs {
		// Validate ascending order
		if i > 0 {
			if segID <= prevSegID {
				return nil, nil, 0, fmt.Errorf("compaction: segment IDs must be in ascending order, got %d after %d",
					segID, prevSegID)
			}

			// Contiguity check: segment IDs must be consecutive OR gap must be verified empty
			if segID != prevSegID+1 {
				// Gap detected - verify no segments exist in (prevSegID, segID)
				// This prevents the Leapfrog Hazard where compacting [10, 15] while
				// segment 12 exists would skip segment 12's data.
				if err := c.index.VerifyNoSegmentsInRange(prevSegID, segID); err != nil {
					return nil, nil, 0, err
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
				continue
			}

			toRelocate = append(toRelocate, relocInfo{
				item:   item,
				oldSeg: item.SegmentID,
				oldOff: item.Offset,
			})
		}
	}

	return toRelocate, tombstones, maxSeqID, nil
}

// writeCompactedSegment creates the new segment file and writes all live blobs.
// Returns footer entries for the .iseg file.
func (c *Compactor) writeCompactedSegment(
	newSegID uint32,
	toRelocate []relocInfo,
	result *CompactResult,
) ([]record.FooterEntry, error) {
	// Calculate total size for fallocate
	totalSize := int64(record.FileHeaderSize)
	for i := range toRelocate {
		totalSize += int64(toRelocate[i].item.PhysicalLen)
	}

	w, err := CreateSegmentWriter(newSegID, c.basePath, c.shards, c.ioFlags, c.footerPool, totalSize)
	if err != nil {
		return nil, fmt.Errorf("compaction: create segment file: %w", err)
	}

	// Write file header
	if err := w.WriteHeader(); err != nil {
		return nil, errors.Join(fmt.Errorf("compaction: write header: %w", err), w.Close())
	}

	offset := uint32(record.FileHeaderSize)
	footerEntries := make([]record.FooterEntry, 0, len(toRelocate))

	// Write live blobs
	// TODO: Consider using copy_file_range on Linux to copy directly between fds
	for i := range toRelocate {
		ri := &toRelocate[i]

		// Read raw record from old segment
		data, releaser, err := c.reader.ReadBlobRaw(ri.item)
		if err != nil {
			releaser.Release()
			return nil, errors.Join(
				fmt.Errorf("compaction: read blob from segment %d: %w", ri.oldSeg, err),
				w.Close(),
			)
		}

		// Parse header to extract footer metadata
		hdr, err := record.DecodeHeader(data[:record.HeaderSize])
		if err != nil {
			releaser.Release()
			return nil, errors.Join(
				fmt.Errorf("compaction: decode header from segment %d: %w", ri.oldSeg, err),
				w.Close(),
			)
		}

		// Write to new segment
		if _, err := w.File().Write(data); err != nil {
			releaser.Release()
			return nil, errors.Join(fmt.Errorf("compaction: write blob: %w", err), w.Close())
		}
		releaser.Release()

		// Update item with new location
		ri.item.SegmentID = newSegID
		ri.item.Offset = offset

		// Build complete footer entry for recovery
		footerEntries = append(footerEntries, record.FooterEntry{
			Key:          ri.item.Key,
			Pos:          int64(offset),
			LogicalSize:  hdr.LogicalSize,
			PhysicalSize: hdr.PhysicalSize,
			SeqID:        hdr.SeqID,
			Flags:        hdr.Flags,
			KeyLen:       hdr.KeyLen,
		})

		offset += uint32(len(data))
		result.ItemsCompacted++
	}

	// Sync and close
	if err := w.Close(); err != nil {
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
