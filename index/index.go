package index

import (
	"errors"
	"fmt"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/zhangyunhao116/skipmap"
)

// Entry represents a blob stored in blob cache.
type Entry struct {
	metadata.BlobRecord
	SegmentID int64
}

// ValueFn is the callback for various index scan operations.
type ValueFn func(Entry) bool

// ScanSegmentFn is the callback for scanning segment records.
type ScanSegmentFn func(metadata.SegmentRecord) bool

// Index coordinates high-speed RAM lookups, persistence, and eviction.
type Index struct {
	blobs    *skipmap.Uint64Map[*node] // *node is owned by evictor
	evictor  *sievePolicy
	segments *persistence
}

func NewIndex(basePath string) (*Index, error) {
	p, err := newPersistence(basePath)
	if err != nil {
		return nil, err
	}

	idx := &Index{
		blobs:    skipmap.NewUint64[*node](),
		segments: p,
		evictor:  &sievePolicy{},
	}

	err = p.scanAll(func(seg metadata.SegmentRecord) bool {
		for _, rec := range seg.Records {
			if !rec.IsDeleted() {
				idx.blobs.Store(rec.Hash, idx.evictor.Add(Entry{rec, seg.SegmentID}))
			}
		}
		return true
	})
	return idx, err
}

func (idx *Index) Get(hash uint64) (Entry, bool) {
	n, ok := idx.blobs.Load(hash)
	if !ok {
		return Entry{}, false
	}
	n.visited.Store(true)
	return n.entry, true
}

// GetSegmentRecord retrieves the metadata for a specific segment.
// It reconstructs the record from fragmented chunks if necessary.
// Returns (record, true) if found, or (zero-value, false) if not.
func (idx *Index) GetSegmentRecord(segmentID int64) (metadata.SegmentRecord, bool) {
	var fullRecord metadata.SegmentRecord
	var found bool

	// We utilize scanSegment which handles the Range boundary safety.
	err := idx.segments.scanSegment(segmentID, func(seg metadata.SegmentRecord) bool {
		if !found {
			fullRecord = seg
			found = true
		} else {
			// Append records from subsequent chunks
			fullRecord.Records = append(fullRecord.Records, seg.Records...)
		}
		return true
	})

	// If there was a lower-level I/O error or the segment wasn't found,
	// we return false.
	if err != nil || !found {
		return metadata.SegmentRecord{}, false
	}

	return fullRecord, true
}

func (idx *Index) IngestBatch(segID int64, batch []metadata.BlobRecord) error {
	if err := idx.segments.writeBatch(segID, batch); err != nil {
		return err
	}

	for _, rec := range batch {
		entry := Entry{rec, segID}
		idx.blobs.Store(rec.Hash, idx.evictor.Add(entry))
	}
	return nil
}

func (idx *Index) Evict() (Entry, error) {
	// 1. Identification & Unlinking (FIFO list surgery)
	n, err := idx.evictor.EvictNext()
	if err != nil {
		return Entry{}, err
	}

	// 2. RAM Purge (Lookup map removal)
	idx.blobs.LoadAndDelete(n.entry.Hash)

	// 3. Metadata extraction (Struct copy)
	entry := n.entry

	// 4. Memory reclamation (Safe because we have the copy)
	recycleNode(n)

	return entry, nil
}

func (idx *Index) DeleteSegment(segmentID int64) error {
	var keys [][]byte

	// 1. Collect keys and purge RAM
	// We must capture the scan error to know if we actually saw the whole segment
	err := idx.segments.scanSegment(segmentID, func(seg metadata.SegmentRecord) bool {
		// DIAGNOSTIC: Ensure we are only touching what we intended
		if seg.SegmentID != segmentID {
			panic(fmt.Sprintf("CRITICAL: scanSegment(%d) returned records for segment %d", segmentID, seg.SegmentID))
		}
		for _, rec := range seg.Records {
			if n, ok := idx.blobs.LoadAndDelete(rec.Hash); ok {
				idx.evictor.removeNode(n)
			}
		}
		if len(seg.IndexKey) > 0 {
			keys = append(keys, seg.IndexKey)
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("scan segment %d failed: %w", segmentID, err)
	}

	if len(keys) == 0 {
		// If we found nothing to delete, it might already be gone or the scan failed silently
		return nil
	}

	// 2. Transactional Delete from Persistent Store
	// This should be a variadic call to the underlying storage
	if err := idx.segments.delete(keys...); err != nil {
		return fmt.Errorf("failed to delete segment %d index keys: %w", segmentID, err)
	}

	return nil
}

// DeleteBlobs removes multiple blobs from RAM and immediately
// synchronizes those changes to the persistence layer.
func (idx *Index) DeleteBlobs(entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// 1. Fast Path: Immediate RAM removal
	for _, e := range entries {
		// removeNode from lookup map
		if n, ok := idx.blobs.LoadAndDelete(e.Hash); ok {
			// Unlink from Sieve
			idx.evictor.removeNode(n)
		}
	}

	// 2. Durable Path: Synchronous Bitcask sync
	return idx.flushDeletions(entries)
}

// flushDeletions groups the provided entries by SegmentID and
// pushes the updates to Bitcask in a single transaction per segment.
func (idx *Index) flushDeletions(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// 1. Group by SegmentID
	bySegment := make(map[int64]map[uint64]struct{})
	for _, e := range entries {
		if _, ok := bySegment[e.SegmentID]; !ok {
			bySegment[e.SegmentID] = make(map[uint64]struct{})
		}
		bySegment[e.SegmentID][e.Hash] = struct{}{}
	}

	// 2. Synchronous Update
	var errs []error
	for segID, hashes := range bySegment {
		if err := idx.segments.DeleteRecordsFromSegment(segID, hashes); err != nil {
			errs = append(errs, fmt.Errorf("flush: segment %d failed: %w", segID, err))
		}
	}

	return errors.Join(errs...)
}

func (idx *Index) Close() error {
	idx.blobs.Range(func(_ uint64, n *node) bool {
		idx.evictor.removeNode(n)
		return true
	})
	return idx.segments.close()
}

// ForEachBlob iterates over all blobs currently in the memory index.
// It passes a copy (Entry) to the callback to ensure the internal node
// remains encapsulated and safe from external modification.
func (idx *Index) ForEachBlob(fn func(Entry) bool) {
	idx.blobs.Range(func(key uint64, n *node) bool {
		return fn(n.entry)
	})
}

// ForEachSegment iterates over all segment metadata records stored on disk.
// This is useful for background maintenance or reporting.
func (idx *Index) ForEachSegment(fn ScanSegmentFn) error {
	return idx.segments.scanAll(fn)
}

// IndexStats provides a snapshot of the current index state.
type IndexStats struct {
	BlobCount    int
	SegmentCount int
}

func (idx *Index) Stats() IndexStats {
	stats := IndexStats{
		BlobCount: idx.evictor.count, // Fast O(1) from policy
	}

	// Count segments from disk (O(N) but useful for background tasks)
	_ = idx.ForEachSegment(func(metadata.SegmentRecord) bool {
		stats.SegmentCount++
		return true
	})

	return stats
}
