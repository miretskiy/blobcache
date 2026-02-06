package index

import (
	"errors"
	"fmt"
	"sync"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
)

// DurableIndex wraps BlobIndex with append-only .meta file persistence.
// It provides durable storage for blob metadata while leveraging the
// GC-optimized in-memory index for fast lookups.
type DurableIndex struct {
	blobs    *BlobIndex
	segments *persistence
}

// OpenIndex creates a DurableIndex by loading persisted metadata from disk.
// The basePath should contain a "segments" subdirectory with .meta files.
func OpenIndex(basePath string, shards int, initialCapacity int) (*DurableIndex, error) {
	p, err := newPersistence(basePath, shards)
	if err != nil {
		return nil, err
	}

	idx := &DurableIndex{
		blobs:    NewBlobIndex(initialCapacity),
		segments: p,
	}

	// Load all persisted items into memory and register segment snapshots
	err = p.scanAll(func(m DurableBatch) bool {
		idx.AddSegment(m.SegmentID, m.Items)
		return true
	})
	if err != nil {
		_ = p.close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return idx, nil
}

// SetBlobErrno marks a blob with an error code in the in-memory index.
// This is a RAM-only operation; the error is not persisted to disk.
func (idx *DurableIndex) SetBlobErrno(key Key, errno base.BlobErrno) {
	s := idx.blobs.Shard(key)

	s.Lock()
	defer s.Unlock()

	i, ok := s.Items[key]
	if !ok {
		return
	}
	s.Extra.nodes[i].item.SetErrno(errno)
}

// Get retrieves an item from the RAM index.
func (idx *DurableIndex) Get(k Key) (Item, bool) {
	return idx.blobs.Get(k)
}

// Put inserts or updates an item in the RAM index.
func (idx *DurableIndex) Put(item Item) {
	idx.blobs.Put(item)
}

// Delete removes an item from the RAM index.
func (idx *DurableIndex) Delete(k Key) bool {
	return idx.blobs.Delete(k)
}

func (idx *DurableIndex) EvictBatch(targetBytes int64) []Item {
	return idx.blobs.EvictBatch(targetBytes)
}

func (idx *DurableIndex) NumItems() int {
	return idx.blobs.NumItems()
}

func (idx *DurableIndex) BlobStats() Stats {
	return idx.blobs.Stats()
}

func (idx *DurableIndex) Relocate(
		k Key, oldSeg, newSeg SegmentID, oldOff, newOff Offset, mode RelocateMode,
) bool {
	return idx.blobs.Relocate(k, oldSeg, newSeg, oldOff, newOff, mode)
}

func (idx *DurableIndex) RelocateBatch(requests []RelocationRequest) int {
	return idx.blobs.RelocateBatch(requests)
}

// GetSegmentManifest retrieves the metadata for a specific segment.
// It reconstructs the manifest from fragmented chunks if necessary.
// Returns (manifest, true) if found, or (zero-value, false) if not.
func (idx *DurableIndex) GetSegmentManifest(segmentID uint32) (DurableBatch, bool) {
	var fullManifest DurableBatch
	var found bool

	err := idx.segments.scanSegment(segmentID, func(m DurableBatch) bool {
		if !found {
			fullManifest = m
			found = true
		} else {
			// Read items from subsequent chunks
			fullManifest.Items = append(fullManifest.Items, m.Items...)
		}
		return true
	})

	if err != nil || !found {
		return DurableBatch{}, false
	}
	return fullManifest, true
}

// AddSegment registers a new segment with its items.
// Creates a frozen Bloom filter snapshot (for tombstone dissolution queries),
// computes initial metadata, and ingests live items into the RAM index.
//
// Persistence is handled separately via .meta files:
//   - During flush: WriteFooter writes the .meta file before this is called
//   - During recovery: .meta files are read and items passed to this method
func (idx *DurableIndex) AddSegment(segmentID uint32, items []Item) {
	// Compute initial metadata from items
	var liveCount int32
	var tombstoneCount int32
	var liveBytes int64

	// Create frozen Bloom filter snapshot from ALL items (including deleted).
	// This enables hasOlderShadow queries for tombstone dissolution.
	filter := bloom.New(32_000, 0.03) // 32k items, 3% FPR (~29KB)
	for _, item := range items {
		filter.AddHash(item.Key)
		if item.IsDeleted() {
			tombstoneCount++
		} else {
			liveCount++
			liveBytes += int64(item.PhysicalLen)
		}
	}
	filter.Freeze()

	// Register segment with metadata
	idx.segments.registerSegment(SegmentMetadata{
		ID:             segmentID,
		LiveItemCount:  liveCount,
		TombstoneCount: tombstoneCount,
		LiveBytes:      liveBytes,
		SegmentKeys:    filter,
	})

	// Ingest all items into RAM index (including tombstones for proper Delete tracking)
	for _, item := range items {
		idx.Put(item)
	}
}

// Evict removes the coldest item using SIEVE and returns it.
// This is a RAM-only operation; persistence sync is caller's responsibility.
func (idx *DurableIndex) Evict() (Item, error) {
	evicted := idx.EvictBatch(1)
	if len(evicted) == 0 {
		return Item{}, errors.New("eviction: empty")
	}
	return evicted[0], nil
}

// DeleteSegment removes all entries for a segment from both RAM and disk.
func (idx *DurableIndex) DeleteSegment(segmentID uint32) error {
	// Load manifest to get items for RAM deletion
	err := idx.segments.scanSegment(segmentID, func(m DurableBatch) bool {
		if m.SegmentID != segmentID {
			panic(fmt.Sprintf("scanSegment(%d) returned entries for segment %d", segmentID, m.SegmentID))
		}
		for _, item := range m.Items {
			idx.Delete(item.Key)
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("scan segment %d failed: %w", segmentID, err)
	}

	// Delete the .meta file
	return idx.segments.dropSegment(segmentID)
}

// Tombstone writes a tombstone to the incremental log and marks item as deleted in RAM.
// This is used for explicit Delete() operations where we have the user key.
func (idx *DurableIndex) Tombstone(segID uint32, keyHash Key, userKey []byte) error {
	// Write to incremental log first
	if err := idx.segments.tombstone(segID, keyHash, userKey); err != nil {
		return err
	}

	// Mark deleted in RAM
	idx.markDeleted(keyHash)
	return nil
}

// MarkDeleted sets the deleted flag on an item in the in-memory index (RAM only).
// This is used for eviction where we don't have user keys and don't need persistence.
func (idx *DurableIndex) MarkDeleted(k Key) {
	idx.markDeleted(k)
}

// markDeleted is the internal implementation (unexported).
func (idx *DurableIndex) markDeleted(k Key) {
	s := idx.blobs.Shard(k)
	s.Lock()
	defer s.Unlock()

	if i, ok := s.Items[k]; ok {
		s.Extra.nodes[i].item.SetDeleted()
	}
}

// DeleteBlobs marks blobs as deleted in RAM and writes tombstones to incremental log.
// Used for eviction (no user keys, hash-only tombstones).
func (idx *DurableIndex) DeleteBlobs(items ...Item) error {
	if len(items) == 0 {
		return nil
	}

	// Write ALL tombstones in one transaction (avoids N lock acquisitions + radix clones)
	if err := idx.segments.tombstoneBatch(items); err != nil {
		return fmt.Errorf("batch tombstone: %w", err)
	}

	// Mark deleted in RAM (fast loop)
	for _, item := range items {
		idx.markDeleted(item.Key)
	}

	return nil
}

// Close releases all resources held by the index.
func (idx *DurableIndex) Close() error {
	return idx.segments.close()
}

// ForEachBlob iterates over all blobs currently in the memory index.
func (idx *DurableIndex) ForEachBlob(fn func(Item) bool) {
	for i := range ShardCount {
		s := idx.blobs.ShardAt(i)
		s.RLock()
		for _, nodeIdx := range s.Items {
			if !fn(s.Extra.nodes[nodeIdx].item) {
				s.RUnlock()
				return
			}
		}
		s.RUnlock()
	}
}

// ForEachSegment iterates over all segment manifests stored on disk.
func (idx *DurableIndex) ForEachSegment(fn ScanBatchFn) error {
	return idx.segments.scanAll(fn)
}

// DurableStats provides a snapshot of the durable index state.
type DurableStats struct {
	Stats            // Embedded in-memory stats
	SegmentCount int // Number of segments on disk
}

// DropSegment deletes the .meta file for a segment without touching RAM.
// Used after compaction when segment data has been fully relocated to a new segment.
func (idx *DurableIndex) DropSegment(segID uint32) error {
	idx.segments.unregisterSegment(segID)
	return idx.segments.dropSegment(segID)
}

// DropSegments removes multiple segments atomically.
// More efficient than multiple DropSegment calls when dropping merged segments.
// Stops on first error.
func (idx *DurableIndex) DropSegments(segIDs []uint32) error {
	idx.segments.unregisterSegments(segIDs)
	for _, segID := range segIDs {
		if err := idx.segments.dropSegment(segID); err != nil {
			return err
		}
	}
	return nil
}

// HasOlderShadow checks if any segment with ID < floorID might contain the key.
// Returns true if any older segment's Bloom filter tests positive.
//
// This is the core tombstone dissolution query:
//   - If true: tombstone MUST be preserved (older version may exist)
//   - If false: tombstone can be safely dissolved (no older version)
func (idx *DurableIndex) HasOlderShadow(key Key, floorID uint32) bool {
	return idx.segments.hasOlderShadow(key, floorID)
}

// SegmentLockShard returns the RWMutex for a given segment ID.
// Used for coordinating Delete and Compaction operations.
//
// Locking protocol:
// - Delete: Lock() (exclusive, blocks compaction)
// - Compaction: RLock() (shared, multiple compactions allowed)
func (idx *DurableIndex) SegmentLockShard(segID uint32) *sync.RWMutex {
	return &idx.segments.segmentLocks[segID%numSegmentLockShards]
}

// ForEachSegmentMeta iterates over all registered segments in ID order.
// Used for computing segment statistics without scanning disk files.
func (idx *DurableIndex) ForEachSegmentMeta(fn func(meta SegmentMetadata) bool) {
	idx.segments.forEachSegment(fn)
}

// GetOldestSegmentID returns the ID of the oldest registered segment, or 0 if none.
// O(1) lookup from the in-memory registry. Thread-safe for concurrent reads.
func (idx *DurableIndex) GetOldestSegmentID() uint32 {
	return idx.segments.getOldestSegmentID()
}

// UpdateSegmentOnDelete updates a segment's metadata after items are deleted.
// Called during eviction or explicit delete to track tombstone accumulation.
func (idx *DurableIndex) UpdateSegmentOnDelete(segID uint32, deletedCount int32, deletedBytes int64) {
	idx.segments.updateSegmentOnDelete(segID, deletedCount, deletedBytes)
}

// GetTombstoneCompactionCandidates returns segment IDs that have crossed the
// tombstone threshold (100+) and have cooled past the given boundary.
//
// This is O(K) where K is the number of pending candidates, avoiding O(N) scan
// of all segments. Returns sorted segment IDs for deterministic processing.
func (idx *DurableIndex) GetTombstoneCompactionCandidates(maxEligibleID uint32) []uint32 {
	return idx.segments.getTombstoneCompactionCandidates(maxEligibleID)
}

// GetMergeCompactionCandidates returns segments that have crossed the waste ratio
// threshold (90%+) and have cooled past the given boundary.
//
// This is O(K) where K is the number of pending candidates, avoiding O(N) scan
// of all segments. Returns candidates sorted by segment ID for deterministic processing.
func (idx *DurableIndex) GetMergeCompactionCandidates(maxEligibleID uint32) []SparseSegment {
	return idx.segments.getMergeCompactionCandidates(maxEligibleID)
}

// GetDirtySegments returns segments whose DirtyBytes exceed the threshold.
// Used for deferred hole punching - batch many small deletes into fewer large punches.
func (idx *DurableIndex) GetDirtySegments(threshold int64) []DirtySegment {
	return idx.segments.getDirtySegments(threshold)
}

// ResetDirtyBytes clears the DirtyBytes counter for a segment after hole punching.
func (idx *DurableIndex) ResetDirtyBytes(segID uint32) {
	idx.segments.resetDirtyBytes(segID)
}

// CompactTombstones merges the tombstone incremental log into the segment manifest.
// The onTombstone callback is invoked for each tombstone, allowing the caller to
// perform I/O operations (e.g., hole punching) before the metadata is updated.
//
// This is a metadata cleanup operation:
// - Collapses tombstone batches into the segment Items (marked as deleted)
// - Rewrites the .meta file without tombstone batches
// - Allows caller to reclaim space via callback
//
// The caller must hold the segment lock before calling this method.
func (idx *DurableIndex) CompactTombstones(segID uint32, onTombstone TombstoneFn) error {
	return idx.segments.compactTombstones(segID, onTombstone)
}

// VerifyNoSegmentsInRange checks that no segments exist in the open interval (startID, endID).
// Used during compaction to validate the Strict Contiguity Rule.
//
// Example: Compacting [10, 15] requires verifying that segments 11, 12, 13, 14 don't exist.
// If they do exist, compacting [10, 15] would skip them (Leapfrog Hazard), potentially
// resurrecting deleted keys.
//
// Returns nil if the range is safe (no segments in gap), or an error if segments found.
// Uses a single range scan across all segment IDs for efficiency.
func (idx *DurableIndex) VerifyNoSegmentsInRange(startID, endID uint32) error {
	if endID <= startID+1 {
		return nil // Adjacent or same segment, no gap to check
	}

	// Single range scan: [startID+1:chunk0] to [endID-1:chunkMax]
	// If ANY key exists, a segment is present in the gap
	var foundSegID uint32
	found := false

	err := idx.segments.scanRange(startID+1, endID-1, func(m DurableBatch) bool {
		found = true
		foundSegID = m.SegmentID
		return false // Stop on first hit
	})

	if err != nil {
		return err
	}

	if found {
		return fmt.Errorf("segment %d exists in gap (%d, %d) - Leapfrog Hazard would occur if compaction proceeds",
			foundSegID, startID, endID)
	}

	return nil
}
