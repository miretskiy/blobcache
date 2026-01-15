package index

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/miretskiy/blobcache/base"
)

// KeyFromHash converts a uint64 hash to a 128-bit Key.
// The hash is placed in the low 8 bytes with zeros in the high 8 bytes.
// This provides backward compatibility during the transition to 128-bit hashes.
//
// NOTE: k[0] must be uniformly distributed for good shard distribution.
// XXHash3's output is uniform, so the first byte of the hash works well.
func KeyFromHash(hash uint64) Key {
	var k Key
	binary.LittleEndian.PutUint64(k[0:8], hash)
	return k
}

// DurableIndex wraps BlobIndex with Bitcask persistence.
// It provides durable storage for blob metadata while leveraging the
// GC-optimized in-memory index for fast lookups.
type DurableIndex struct {
	*BlobIndex
	segments *persistence
}

// Open creates a DurableIndex by loading persisted metadata from disk.
// The basePath should contain a "db" subdirectory for Bitcask storage.
func Open(basePath string, initialCapacity int) (*DurableIndex, error) {
	p, err := newPersistence(basePath)
	if err != nil {
		return nil, err
	}

	idx := &DurableIndex{
		BlobIndex: New(initialCapacity),
		segments:  p,
	}

	// Load all persisted items into memory
	err = p.scanAll(func(m SegmentManifest) bool {
		for _, item := range m.Items {
			if !item.IsDeleted() {
				k := KeyFromHash(item.Hash)
				idx.Put(k, item)
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

// DeprecatedGetByHash looks up an item by its uint64 hash.
// Deprecated: This is a bridge method for 64-bit hash compatibility.
// Will be removed when transitioning to 128-bit hashes.
func (idx *DurableIndex) DeprecatedGetByHash(hash uint64) (Item, bool) {
	return idx.Get(KeyFromHash(hash))
}

// SetBlobErrno marks a blob with an error code in the in-memory index.
// This is a RAM-only operation; the error is not persisted to disk.
func (idx *DurableIndex) SetBlobErrno(hash uint64, errno base.BlobErrno) {
	k := KeyFromHash(hash)
	shardIdx := k[0]
	s := &idx.shards[shardIdx]

	s.mu.Lock()
	defer s.mu.Unlock()

	i, ok := s.items[k]
	if !ok {
		return
	}
	s.nodes[i].item.SetErrno(errno)
}

// GetSegmentManifest retrieves the metadata for a specific segment.
// It reconstructs the manifest from fragmented chunks if necessary.
// Returns (manifest, true) if found, or (zero-value, false) if not.
func (idx *DurableIndex) GetSegmentManifest(segmentID uint32) (SegmentManifest, bool) {
	var fullManifest SegmentManifest
	var found bool

	err := idx.segments.scanSegment(segmentID, func(m SegmentManifest) bool {
		if !found {
			fullManifest = m
			found = true
		} else {
			// Append items from subsequent chunks
			fullManifest.Items = append(fullManifest.Items, m.Items...)
		}
		return true
	})

	if err != nil || !found {
		return SegmentManifest{}, false
	}
	return fullManifest, true
}

// IngestBatch writes a batch of items for a segment to both RAM and disk.
func (idx *DurableIndex) IngestBatch(segID uint32, items []Item) error {
	if err := idx.segments.writeBatch(segID, items); err != nil {
		return err
	}

	for _, item := range items {
		k := KeyFromHash(item.Hash)
		idx.Put(k, item)
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

	err := idx.segments.scanSegment(segmentID, func(m SegmentManifest) bool {
		if m.SegmentID != segmentID {
			panic(fmt.Sprintf("scanSegment(%d) returned entries for segment %d", segmentID, m.SegmentID))
		}
		for _, item := range m.Items {
			k := KeyFromHash(item.Hash)
			idx.Delete(k)
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

	if err := idx.segments.delete(keys...); err != nil {
		return fmt.Errorf("failed to delete segment %d index keys: %w", segmentID, err)
	}
	return nil
}

// DeleteBlobs removes multiple blobs from RAM and immediately
// synchronizes those changes to the persistence layer.
func (idx *DurableIndex) DeleteBlobs(items ...Item) error {
	if len(items) == 0 {
		return nil
	}

	// Fast Path: Immediate RAM removal
	for _, item := range items {
		k := KeyFromHash(item.Hash)
		idx.Delete(k)
	}

	// Durable Path: Synchronous Bitcask sync
	return idx.flushDeletions(items)
}

// flushDeletions groups items by SegmentID and updates Bitcask.
func (idx *DurableIndex) flushDeletions(items []Item) error {
	if len(items) == 0 {
		return nil
	}

	// Group by SegmentID
	bySegment := make(map[uint32]map[uint64]struct{})
	for _, item := range items {
		if _, ok := bySegment[item.SegmentID]; !ok {
			bySegment[item.SegmentID] = make(map[uint64]struct{})
		}
		bySegment[item.SegmentID][item.Hash] = struct{}{}
	}

	var errs []error
	for segID, hashes := range bySegment {
		if err := idx.segments.DeleteRecordsFromSegment(segID, hashes); err != nil {
			errs = append(errs, fmt.Errorf("flush: segment %d failed: %w", segID, err))
		}
	}

	return errors.Join(errs...)
}

// Close releases all resources held by the index.
func (idx *DurableIndex) Close() error {
	return idx.segments.close()
}

// ForEachBlob iterates over all blobs currently in the memory index.
func (idx *DurableIndex) ForEachBlob(fn func(Item) bool) {
	for i := 0; i < ShardCount; i++ {
		s := &idx.shards[i]
		s.mu.RLock()
		for _, nodeIdx := range s.items {
			if !fn(s.nodes[nodeIdx].item) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}

// ForEachSegment iterates over all segment manifests stored on disk.
func (idx *DurableIndex) ForEachSegment(fn ScanManifestFn) error {
	return idx.segments.scanAll(fn)
}

// DurableStats provides a snapshot of the durable index state.
type DurableStats struct {
	Stats            // Embedded in-memory stats
	SegmentCount int // Number of segments on disk
}

// DurableStats returns statistics including persistence info.
func (idx *DurableIndex) DurableStats() DurableStats {
	stats := DurableStats{
		Stats: idx.Stats(),
	}

	// Count segments from disk
	_ = idx.ForEachSegment(func(SegmentManifest) bool {
		stats.SegmentCount++
		return true
	})

	return stats
}
