package blobcache

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/xmap"
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

// DefaultCompactBufSize is used when no buffer size is specified.
const DefaultCompactBufSize = 64 << 20 // 64MB

// Compactor handles segment compaction using a copy-forward approach.
// It merges contiguous segments, filters out stale/deleted items, and
// produces a single compacted segment.
type Compactor struct {
	index             *index.DurableIndex
	segIDs            SegmentIDProvider
	basePath          string
	shards            int
	ioFlags           sys.OpenFlag
	bufSize           int
	releaseCachedFile SegmentCacheReleaseFn

	// compactBuf is a reusable aligned buffer for O_DIRECT compaction I/O.
	// Lazily allocated on first use via sys.AllocAligned, reused across compaction cycles.
	compactBuf []byte

	// segmentFiles caches open O_DIRECT file handles and sizes for source segments.
	// Populated during compaction, closed after each Compact() call.
	// Caching size avoids repeated Stat() syscalls during aligned reads.
	segmentFiles map[uint32]segmentFileInfo

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &CompactorKnobs{...}
	Knobs *CompactorKnobs
}

// segmentFileInfo holds a cached segment file handle and its size.
type segmentFileInfo struct {
	file *os.File
	size int64
}

// NewCompactor creates a Compactor with the given dependencies.
// bufSize specifies the buffer size for compaction I/O (0 for default 64MB).
// releaseCachedFile is called before deleting segment files to release cached handles.
func NewCompactor(
	idx *index.DurableIndex,
	segIDs SegmentIDProvider,
	basePath string,
	shards int,
	ioFlags sys.OpenFlag,
	bufSize int,
	releaseCachedFile SegmentCacheReleaseFn,
) *Compactor {
	if bufSize <= 0 {
		bufSize = DefaultCompactBufSize
	}
	return &Compactor{
		index:             idx,
		segIDs:            segIDs,
		basePath:          basePath,
		shards:            shards,
		ioFlags:           ioFlags,
		bufSize:           bufSize,
		releaseCachedFile: releaseCachedFile,
		segmentFiles:      make(map[uint32]segmentFileInfo),
	}
}

// Close releases resources held by the Compactor.
func (c *Compactor) Close() {
	if c.compactBuf != nil {
		sys.FreeAligned(c.compactBuf)
		c.compactBuf = nil
	}
}

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
	ReadOps    int   // Number of read operations
	ReadBytes  int64 // Total bytes read from old segments
	WriteOps   int   // Number of write operations
	WriteBytes int64 // Total bytes written to new segment
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

	// Cleanup orphaned segment files on error.
	// If writeCompactedSegment or WriteFooter fails, the .seg file may exist
	// without a valid header or .meta file, causing "invalid file magic" on recovery.
	defer func() {
		if retErr != nil && newSegID != 0 {
			// Best-effort cleanup - don't mask the original error
			_ = DeleteSegmentFiles(c.basePath, c.shards, newSegID)
		}
	}()

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

// writeCompactedSegment creates the new segment file and writes all live blobs.
// Returns footer entries for the .meta file.
//
// Uses O_DIRECT for both reads and writes to avoid page cache pollution.
// Reads use aligned-read-then-shift via readBlobAligned (caches segment file handles).
// Writes accumulate in an aligned buffer and flush in 4KB chunks.
func (c *Compactor) writeCompactedSegment(
		newSegID uint32,
		toRelocate []relocInfo,
		result *CompactResult,
) ([]record.FooterEntry, error) {
	// Lazy-allocate compaction buffer on first use
	if c.compactBuf == nil {
		c.compactBuf = sys.AllocAligned(c.bufSize)
	}
	buf := c.compactBuf

	// Calculate total size for fallocate
	totalSize := int64(record.FileHeaderSize)
	for i := range toRelocate {
		totalSize += int64(toRelocate[i].item.PhysicalLen)
	}
	allocSize := (totalSize + sys.BlockMask) &^ sys.BlockMask

	// Create segment file with O_DIRECT
	segPath := getSegmentPath(c.basePath, c.shards, newSegID)
	f, err := sys.CreateAndAllocateFile(segPath, c.ioFlags, allocSize)
	if err != nil {
		return nil, fmt.Errorf("compaction: create segment: %w", err)
	}

	bufPos := 0       // Bytes in buffer (not yet written to disk)
	diskWritten := 0  // Bytes written to disk
	footerEntries := make([]record.FooterEntry, 0, len(toRelocate))

	// Copy file header into buffer
	copy(buf[bufPos:], record.FileHeaderBytes[:])
	bufPos += record.FileHeaderSize

	// Process each blob using sliding window read/write
	for i := range toRelocate {
		ri := &toRelocate[i]
		blobLen := int(ri.item.PhysicalLen)

		// Calculate aligned read size needed for this blob
		_, alignedReadSize := sys.AlignRange(0, blobLen+sys.BlockSize)

		// Check if this is an XL blob (too large to fit in buffer even when empty)
		isXL := int(alignedReadSize) > len(buf)

		if isXL {
			// XL blob: flush pending data, then read/write directly with temp buffer
			xlRes, err := c.writeXLBlob(f, buf, bufPos, ri, diskWritten, newSegID, result)
			if err != nil {
				return nil, errors.Join(err, f.Close())
			}
			// Update position tracking
			diskWritten += xlRes.flushWritten + xlRes.xlWritten
			bufPos = 0 // Buffer was flushed

			footerEntries = append(footerEntries, record.FooterEntry{
				Key:          ri.item.Key,
				Pos:          int64(ri.item.Offset),
				LogicalSize:  xlRes.hdr.LogicalSize,
				PhysicalSize: xlRes.hdr.PhysicalSize,
				SeqID:        xlRes.hdr.SeqID,
				Flags:        xlRes.hdr.Flags,
				KeyLen:       xlRes.hdr.KeyLen,
			})
			result.ItemsCompacted++
			continue
		}

		// Normal blob: use sliding window approach
		// Ensure buffer has room for: current data + aligned landing zone + blob
		landingZone := (bufPos + sys.BlockMask) &^ sys.BlockMask

		if landingZone+int(alignedReadSize) > len(buf) {
			// Flush full blocks to make room
			written, err := c.flushFullBlocks(f, buf, bufPos)
			if err != nil {
				return nil, errors.Join(err, f.Close())
			}
			if written > 0 {
				result.WriteOps++
				result.WriteBytes += int64(written)
				diskWritten += written
			}
			bufPos -= written // Shift position after flush moved tail to front
			// Recalculate landing zone after flush
			landingZone = (bufPos + sys.BlockMask) &^ sys.BlockMask
		}

		// Perform aligned read into landing zone; data is shifted to dst[0:] by readBlobAligned
		n, err := c.readBlobAligned(ri.item, buf[landingZone:])
		if err != nil {
			return nil, errors.Join(err, f.Close())
		}
		result.ReadOps++
		result.ReadBytes += int64(n)

		// Parse header to extract footer metadata
		hdr, err := record.DecodeHeader(buf[landingZone : landingZone+record.HeaderSize])
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("compaction: decode header from segment %d: %w", ri.oldSeg, err),
				f.Close(),
			)
		}

		// Shift from landing zone to bufPos to keep data dense
		if landingZone > bufPos {
			copy(buf[bufPos:], buf[landingZone:landingZone+n])
		}

		// Update item with new location (disk position + buffer position)
		ri.item.SegmentID = newSegID
		ri.item.Offset = uint32(diskWritten + bufPos)

		// Build footer entry
		footerEntries = append(footerEntries, record.FooterEntry{
			Key:          ri.item.Key,
			Pos:          int64(diskWritten + bufPos),
			LogicalSize:  hdr.LogicalSize,
			PhysicalSize: hdr.PhysicalSize,
			SeqID:        hdr.SeqID,
			Flags:        hdr.Flags,
			KeyLen:       hdr.KeyLen,
		})

		bufPos += n
		result.ItemsCompacted++
	}

	// Final flush, truncate, sync, and close
	// Actual file size is diskWritten + bufPos (before final flush padding)
	actualSize := int64(diskWritten + bufPos)
	if err := c.finalFlush(f, buf, bufPos, actualSize, result); err != nil {
		return nil, errors.Join(err, f.Close())
	}

	if err := errors.Join(sys.SyncFile(f, c.ioFlags), f.Close()); err != nil {
		return nil, fmt.Errorf("compaction: sync segment: %w", err)
	}

	return footerEntries, nil
}

// xlWriteResult contains the outcome of writing an XL blob.
type xlWriteResult struct {
	hdr          record.Header
	flushWritten int // Bytes written during pre-flush (including padding)
	xlWritten    int // Bytes written for XL blob (including padding)
}

// writeXLBlob handles blobs that are larger than the compaction buffer.
// It flushes any pending data, allocates a temporary aligned buffer for the XL blob,
// reads and writes it directly, then frees the temp buffer.
//
// Returns xlWriteResult so caller can update disk position tracking.
// diskWritten is the bytes already written to disk (before buffer contents).
func (c *Compactor) writeXLBlob(
	f *os.File,
	buf []byte,
	bufPos int,
	ri *relocInfo,
	diskWritten int,
	newSegID uint32,
	result *CompactResult,
) (xlWriteResult, error) {
	var res xlWriteResult
	blobLen := int(ri.item.PhysicalLen)

	// 1. Flush any pending data in the main buffer first (preserve ordering)
	if bufPos > 0 {
		alignedEnd := (bufPos + sys.BlockMask) &^ sys.BlockMask
		for i := bufPos; i < alignedEnd; i++ {
			buf[i] = 0
		}
		n, err := sys.WriteAligned(buf[:alignedEnd], f, c.ioFlags)
		if err != nil {
			return res, fmt.Errorf("compaction: flush before XL: %w", err)
		}
		result.WriteOps++
		result.WriteBytes += int64(n)
		res.flushWritten = alignedEnd
	}

	// 2. Allocate temporary aligned buffer for XL blob
	_, alignedSize := sys.AlignRange(0, blobLen+sys.BlockSize)
	xlBuf := sys.AllocAligned(int(alignedSize))
	defer sys.FreeAligned(xlBuf)

	// 3. Read XL blob into temp buffer
	n, err := c.readBlobAligned(ri.item, xlBuf)
	if err != nil {
		return res, err
	}
	result.ReadOps++
	result.ReadBytes += int64(n)

	// 4. Parse header for footer metadata
	res.hdr, err = record.DecodeHeader(xlBuf[:record.HeaderSize])
	if err != nil {
		return res, fmt.Errorf("compaction: decode XL header: %w", err)
	}

	// 5. Update item location: XL blob starts after previous disk writes + flush
	ri.item.SegmentID = newSegID
	ri.item.Offset = uint32(diskWritten + res.flushWritten)

	// 6. Write XL blob with padding (xlBuf is already zero-initialized from AllocAligned)
	writeSize := int(sys.PageAlign(int64(n)))
	written, err := sys.WriteAligned(xlBuf[:writeSize], f, c.ioFlags)
	if err != nil {
		return res, fmt.Errorf("compaction: write XL blob: %w", err)
	}
	result.WriteOps++
	result.WriteBytes += int64(written)
	res.xlWritten = writeSize

	return res, nil
}

// flushFullBlocks writes complete 4KB blocks from the buffer, shifting the tail to front.
// Returns the number of bytes written (multiple of 4KB), or error.
func (c *Compactor) flushFullBlocks(f *os.File, buf []byte, bufPos int) (int, error) {
	alignedLen := bufPos &^ sys.BlockMask
	if alignedLen == 0 {
		return 0, nil
	}

	n, err := sys.WriteAligned(buf[:alignedLen], f, c.ioFlags)
	if err != nil {
		return 0, fmt.Errorf("compaction: write aligned chunk: %w", err)
	}

	// Shift unaligned tail to front of buffer
	tail := bufPos - alignedLen
	if tail > 0 {
		copy(buf[0:tail], buf[alignedLen:bufPos])
	}

	return n, nil
}

// finalFlush writes remaining data with padding, then truncates to exact size.
func (c *Compactor) finalFlush(
		f *os.File, buf []byte, bufPos int, totalSize int64, res *CompactResult,
) error {
	if bufPos == 0 {
		return nil
	}

	alignedEnd := (bufPos + int(sys.BlockMask)) &^ int(sys.BlockMask)
	// Zero padding bytes
	for i := bufPos; i < alignedEnd; i++ {
		buf[i] = 0
	}

	n, err := sys.WriteAligned(buf[:alignedEnd], f, c.ioFlags)
	if err != nil {
		return fmt.Errorf("compaction: write final chunk: %w", err)
	}
	res.WriteOps++
	res.WriteBytes += int64(n)

	// Truncate to exact logical size (removes padding)
	return f.Truncate(totalSize)
}

// readBlobAligned reads a blob into a page-aligned buffer using O_DIRECT.
// Uses cached segment file handles and sizes to avoid opening files and Stat() syscalls.
// Returns the number of blob bytes written to dst[0:].
func (c *Compactor) readBlobAligned(e index.Item, dst []byte) (int, error) {
	if int(e.PhysicalLen) > len(dst) {
		return 0, fmt.Errorf("compaction: dst buffer too small (%d > %d)", e.PhysicalLen, len(dst))
	}

	// Get or open segment file with O_DIRECT (size is cached to avoid Stat() per blob)
	seg, err := c.getSegmentFile(e.SegmentID)
	if err != nil {
		return 0, err
	}

	// Calculate O_DIRECT compliant offsets and lengths using AlignRange
	alignedOff, alignedLen := sys.AlignRange(int64(e.Offset), int(e.PhysicalLen))
	padding := int64(e.Offset) - alignedOff

	// Check cached file size - if aligned read would exceed file, use unaligned read
	// (handles small test files that are < 4KB)
	if alignedOff+alignedLen > seg.size {
		// File too small for aligned read - fall back to exact read
		n, err := seg.file.ReadAt(dst[:e.PhysicalLen], int64(e.Offset))
		if err != nil {
			return 0, fmt.Errorf("compaction: pread segment %d: %w", e.SegmentID, err)
		}
		return n, nil
	}

	if int(alignedLen) > len(dst) {
		return 0, fmt.Errorf("compaction: dst buffer too small for aligned read (%d > %d)", alignedLen, len(dst))
	}

	// Perform aligned read
	n, err := sys.PreadAligned(seg.file, dst[:alignedLen], alignedOff, c.ioFlags)
	if err != nil {
		return 0, fmt.Errorf("compaction: aligned pread segment %d: %w", e.SegmentID, err)
	}
	if int64(n) < padding+int64(e.PhysicalLen) {
		return 0, fmt.Errorf("compaction: short read from segment %d: got %d, need %d",
			e.SegmentID, n, padding+int64(e.PhysicalLen))
	}

	// Shift blob data from dst[padding:] to dst[0:]
	if padding > 0 {
		copy(dst, dst[padding:padding+int64(e.PhysicalLen)])
	}

	return int(e.PhysicalLen), nil
}

// getSegmentFile returns a cached O_DIRECT file handle and size, or opens a new one.
// Caches Stat() result to avoid repeated syscalls during aligned reads.
func (c *Compactor) getSegmentFile(segmentID uint32) (segmentFileInfo, error) {
	if info, ok := c.segmentFiles[segmentID]; ok {
		return info, nil
	}

	path := getSegmentPath(c.basePath, c.shards, segmentID)
	f, err := sys.OpenFileForRead(path, c.ioFlags)
	if err != nil {
		return segmentFileInfo{}, fmt.Errorf("compaction: open segment %d: %w", segmentID, err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return segmentFileInfo{}, fmt.Errorf("compaction: stat segment %d: %w", segmentID, err)
	}

	info := segmentFileInfo{file: f, size: stat.Size()}
	c.segmentFiles[segmentID] = info
	return info, nil
}

// closeSegmentFiles closes all cached segment file handles.
// Called after each compaction to avoid accumulating open files.
func (c *Compactor) closeSegmentFiles() (retErr error) {
	for segID, info := range c.segmentFiles {
		if err := info.file.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close segment %d: %w", segID, err))
		}
		delete(c.segmentFiles, segID)
	}
	return retErr
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
