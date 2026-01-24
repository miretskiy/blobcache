package index

import (
	"errors"
	"fmt"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// DurableIndex wraps BlobIndex with Bitcask persistence.
// It provides durable storage for blob metadata while leveraging the
// GC-optimized in-memory index for fast lookups.
type DurableIndex struct {
	blobs    *BlobIndex
	segments *persistence
}

// OpenIndex creates a DurableIndex by loading persisted metadata from disk.
// The basePath should contain a "db" subdirectory for Bitcask storage.
func OpenIndex(basePath string, initialCapacity int) (*DurableIndex, error) {
	p, err := newPersistence(basePath)
	if err != nil {
		return nil, err
	}

	idx := &DurableIndex{
		blobs:    NewBlobIndex(initialCapacity),
		segments: p,
	}

	// Load all persisted items into memory
	err = p.scanAll(func(m DurableBatch) bool {
		for _, item := range m.Items {
			if !item.IsDeleted() {
				idx.Put(item)
			}
		}
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

func (idx *DurableIndex) Relocate(k Key, oldSeg, newSeg SegmentID, oldOff, newOff Offset, mode RelocateMode) bool {
	return idx.blobs.Relocate(k, oldSeg, newSeg, oldOff, newOff, mode)
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

// IngestBatch writes a batch of items for a segment to both RAM and disk.
func (idx *DurableIndex) IngestBatch(segID uint32, items []Item, maxSeqID uint64) error {
	if err := idx.segments.writeBatch(segID, items, maxSeqID); err != nil {
		return err
	}

	for _, item := range items {
		idx.Put(item)
	}
	return nil
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
	var keys [][]byte

	err := idx.segments.scanSegment(segmentID, func(m DurableBatch) bool {
		if m.SegmentID != segmentID {
			panic(fmt.Sprintf("scanSegment(%d) returned entries for segment %d", segmentID, m.SegmentID))
		}
		for _, item := range m.Items {
			idx.Delete(item.Key)
		}
		if len(m.IndexKey) > 0 {
			keys = append(keys, m.IndexKey)
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("scan segment %d failed: %w", segmentID, err)
	}

	if len(keys) == 0 {
		return nil
	}

	// Delete manifest keys from Bitcask
	txn := idx.segments.db.Transaction()
	defer txn.Discard()
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			return fmt.Errorf("failed to delete segment %d key: %w", segmentID, err)
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit segment %d deletion: %w", segmentID, err)
	}
	return nil
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

	var errs []error

	// Write tombstones (without user keys - eviction case)
	for _, item := range items {
		if err := idx.segments.tombstone(item.SegmentID, item.Key, nil); err != nil {
			errs = append(errs, fmt.Errorf("tombstone seg=%d: %w", item.SegmentID, err))
		}
	}

	// Mark deleted in RAM
	for _, item := range items {
		idx.markDeleted(item.Key)
	}

	return errors.Join(errs...)
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

// DropSegment deletes all Bitcask entries for a segment without touching RAM.
// Used after compaction when segment data has been fully relocated to a new segment.
func (idx *DurableIndex) DropSegment(segID uint32) error {
	return idx.segments.dropSegment(segID)
}

// CompactBatch writes compacted items to a new segment in Bitcask.
// Unlike IngestBatch, this does NOT update the in-memory index.
// The caller must use Relocate to atomically update item locations after writing.
func (idx *DurableIndex) CompactBatch(segID uint32, items []Item, maxSeqID uint64) error {
	return idx.segments.writeBatch(segID, items, maxSeqID)
}

// SegmentMetaShard returns the xmap shard for a given segment ID.
// Exposes the shard's RWMutex for coordinating Delete and Compaction.
//
// Locking protocol:
// - Delete: shard.Lock() (exclusive, blocks compaction)
// - Compaction: shard.RLock() (shared, multiple compactions allowed)
func (idx *DurableIndex) SegmentMetaShard(segID uint32) *xmap.Shard[SegmentMetadata, xmap.Pad32] {
	key := Key{Lo: uint64(segID), Hi: 0}
	return idx.segments.segmentMeta.Shard(key)
}

// CompactTombstones merges the tombstone incremental log into the segment manifest.
// The onTombstone callback is invoked for each tombstone, allowing the caller to
// perform I/O operations (e.g., hole punching) before the metadata is updated.
//
// This is a metadata cleanup operation:
// - Collapses tombstone log entries into the segment Items (marked as deleted)
// - Drops tombstone log entries from Bitcask
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
