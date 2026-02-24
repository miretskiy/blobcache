package index

import (
	"fmt"
	"sync"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/record"
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

	// Load all persisted items into memory and register segment snapshots.
	// Use entries from disk scan for in-memory manifest caching.
	err = p.scanAll(func(m DurableBatch) bool {
		if m.Entries != nil {
			idx.AddSegmentFromEntries(m.SegmentID, m.Entries)
		} else {
			idx.AddSegment(m.SegmentID, m.Items)
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

// Peek retrieves an item from the RAM index without marking it as visited.
// Used for metadata decisions (e.g., tombstone dissolution) where perturbing
// SIEVE eviction order is undesirable.
func (idx *DurableIndex) Peek(k Key) (Item, bool) {
	return idx.blobs.Peek(k)
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

// GetSegmentManifestRaw returns raw footer entries for a segment.
// Checks in-memory cache first (populated by AddSegmentFromEntries), falls back
// to reading .meta from disk for backward compatibility (test-registered segments).
func (idx *DurableIndex) GetSegmentManifestRaw(segmentID uint32) (SegmentManifest, error) {
	if m, ok := idx.segments.getInMemoryManifest(segmentID); ok {
		return m, nil
	}
	return idx.segments.readSegmentManifest(segmentID)
}

// AddSegmentFromEntries registers a new segment from raw footer entries.
// Stores entries in SegmentMetadata for in-memory manifest access (eliminates
// .meta disk reads during compaction and spatial eviction).
// Derives Items internally for the RAM index and Bloom filter.
func (idx *DurableIndex) AddSegmentFromEntries(segmentID uint32, entries []record.FooterEntry) {
	items := make([]Item, len(entries))
	for i := range entries {
		items[i] = footerEntryToItem(segmentID, &entries[i])
	}

	var liveCount, tombstoneCount int32
	var liveBytes int64

	n := max(uint(len(entries)), 64)
	filter := bloom.New(n, 0.03)
	for i, item := range items {
		filter.AddHash(entries[i].Key)
		if item.IsDeleted() {
			tombstoneCount++
		} else {
			liveCount++
			liveBytes += int64(item.PhysicalLen)
		}
	}
	filter.Freeze()

	idx.segments.registerSegment(SegmentMetadata{
		ID:             segmentID,
		LiveItemCount:  liveCount,
		TombstoneCount: tombstoneCount,
		LiveBytes:      liveBytes,
		SegmentKeys:    filter,
		Entries:        entries,
	})

	for _, item := range items {
		idx.Put(item)
	}
}

// AddSegment registers a new segment with its items.
// Creates a frozen Bloom filter snapshot (for tombstone dissolution queries),
// computes initial metadata, and ingests live items into the RAM index.
//
// Note: This path does NOT cache entries for in-memory manifest access.
// Use AddSegmentFromEntries for production paths where footer entries are available.
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
	//
	// Size dynamically: bloom.New(n, 0.03) → ~1 bit/key at 3% FPR.
	// For 1MB blobs in 64MB segment: ~64 items → 72 bytes.
	// For 4KB blobs: ~16K items → ~14KB. (Was hardcoded 32K → ~29KB always.)
	// Floor at 64 to avoid degenerate filters for very small segments.
	n := max(uint(len(items)), 64)
	filter := bloom.New(n, 0.03)
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

// Evict selects cold items via SIEVE and removes them from the RAM index.
// Returns evicted items (may span multiple segments). The caller is responsible
// for persisting tombstones and updating segment metadata.
func (idx *DurableIndex) Evict(targetBytes int64) []Item {
	return idx.blobs.EvictBatch(targetBytes)
}

// DrainSegment atomically removes all live items belonging to a segment from
// the RAM index, then unregisters and deletes the .meta file.
//
// Used in cache mode to reclaim disk space from sparse segments: once all live
// items are removed from the index, the entire .seg file can be deleted.
// Live items become cache misses — acceptable in cache mode since they were
// already in mostly-dead segments destined for cleanup.
//
// Does NOT write tombstones — the entire segment file is deleted afterward,
// so there's nothing to tombstone against.
//
// Returns the total bytes and count of items drained from the RAM index.
// The caller must hold the exclusive segment lock before calling this method.
func (idx *DurableIndex) DrainSegment(segID uint32) (drainedBytes int64, drainedCount int) {
	// 1. Get manifest from in-memory cache (zero disk I/O in steady state).
	manifest, ok := idx.segments.getInMemoryManifest(segID)
	if !ok {
		return 0, 0
	}

	// 2. For each entry, atomically verify-and-remove from RAM.
	for i := range manifest.Entries {
		item := footerEntryToItem(manifest.SegmentID, &manifest.Entries[i])
		if item.IsDeleted() {
			continue
		}
		// deleteIfAt: atomic check (segID, offset) + remove under shard lock.
		// If the item was overwritten to a newer segment, returns false (safe skip).
		if evicted, ok := idx.blobs.deleteIfAt(item.Key, segID, item.Offset); ok {
			drainedBytes += int64(evicted.PhysicalLen)
			drainedCount++
		}
	}

	// 3. Unregister from segment registry + delete .meta file.
	idx.segments.unregisterSegment(segID)
	_ = idx.segments.dropSegment(segID)

	return drainedBytes, drainedCount
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

// HasOlderShadow checks if a key might have a live version in any segment
// with ID < floorID.
//
// This is the core tombstone dissolution query:
//   - If true: tombstone MUST be preserved (older version may exist)
//   - If false: tombstone can be safely dissolved (no older version)
//
// Uses direct RAM index lookup — exact, O(1), no false positives for the
// common case. Three outcomes:
//
//  1. Key not in RAM (evicted): dissolve. Trade-off: if the key was
//     overwritten to a newer segment then evicted, the old segment still
//     has live data but the key is absent from RAM. Benign in cache mode
//     (stale data reappears after crash, gets re-evicted) and impossible
//     in CAS mode (content-addressed keys are never overwritten).
//
//  2. Key in RAM with SegmentID < floorID: older shadow confirmed, keep.
//
//  3. Key in RAM with SegmentID >= floorID: the RAM entry is likely the
//     tombstone itself (it overwrote any older live entry during AddSegment).
//     If live, a newer write supersedes the tombstone — dissolve.
//     If deleted, conservatively check whether ANY registered segments exist
//     before floorID. This enables dissolution during tail compaction (the
//     primary case) while safely preserving during non-tail compaction.
func (idx *DurableIndex) HasOlderShadow(key Key, floorID uint32) bool {
	item, found := idx.blobs.Peek(key)
	if !found {
		return false
	}
	if item.SegmentID < floorID {
		return true
	}
	// SegmentID >= floorID: item is in/after compaction range.
	// If live, a newer write supersedes the tombstone regardless.
	if !item.IsDeleted() {
		return false
	}
	// Tombstone in compaction range — can't determine from RAM alone.
	// Conservative: keep if older segments exist (non-tail compaction).
	// Dissolve if this IS the tail (no segments before floorID).
	oldest := idx.segments.getOldestSegmentID()
	return oldest != 0 && oldest < floorID
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

// GetOldestSegmentID returns the ID of the oldest registered segment, or 0 if none.
// O(1) lookup from the in-memory registry. Thread-safe for concurrent reads.
func (idx *DurableIndex) GetOldestSegmentID() uint32 {
	return idx.segments.getOldestSegmentID()
}

// GetSegmentCount returns the number of registered segments.
// Thread-safe for concurrent reads.
func (idx *DurableIndex) GetSegmentCount() int {
	return idx.segments.getSegmentCount()
}

// GetGlobalAvgBlobSize returns the average live blob size across all segments.
// Returns 0 if there are no live items.
func (idx *DurableIndex) GetGlobalAvgBlobSize() int64 {
	return idx.segments.getGlobalAvgBlobSize()
}

// UpdateSegmentOnDelete updates a segment's metadata after items are deleted.
// Called during eviction or explicit delete to track merge compaction candidates.
func (idx *DurableIndex) UpdateSegmentOnDelete(segID uint32, deletedCount int32, deletedBytes int64) {
	idx.segments.updateSegmentOnDelete(segID, deletedCount, deletedBytes)
}

// GetTombstoneCompactionCandidates returns segment IDs that have crossed the
// tombstone threshold (100+) and have cooled past the given boundary.
//
// This is O(K) where K is the number of pending candidates, avoiding O(N) scan
// of all segments. Returns sorted segment IDs for deterministic processing.
// Candidates are NOT consumed; call AcknowledgeTombstoneCompaction after success.
func (idx *DurableIndex) GetTombstoneCompactionCandidates(maxEligibleID uint32) []uint32 {
	return idx.segments.getTombstoneCompactionCandidates(maxEligibleID)
}

// AcknowledgeTombstoneCompaction removes a segment from the pending tombstone
// compaction set after successful compaction.
func (idx *DurableIndex) AcknowledgeTombstoneCompaction(segID uint32) {
	idx.segments.acknowledgeTombstoneCompaction(segID)
}

// CompactTombstones merges the tombstone incremental log into the segment manifest.
// Rewrites the .meta file with tombstone batches collapsed into footer entries.
//
// The caller must hold the segment lock before calling this method.
func (idx *DurableIndex) CompactTombstones(segID uint32) error {
	return idx.segments.compactTombstones(segID)
}

// GetDrainCandidates returns all cooled segments (ID < maxEligibleID) sorted
// by LiveBytes ascending (least populated first). Used by pressure-driven drain
// to pick the cheapest segments to sacrifice.
func (idx *DurableIndex) GetDrainCandidates(maxEligibleID uint32) []SparseSegment {
	return idx.segments.getDrainCandidates(maxEligibleID)
}

// GetRewriteCandidates returns cooled segment IDs with waste ratio at or above
// the threshold, sorted ascending. Used by WAL-mode compaction to find segments
// that should be rewritten. Includes 100% dead segments.
func (idx *DurableIndex) GetRewriteCandidates(maxEligibleID uint32, wasteThreshold float64) []uint32 {
	return idx.segments.getRewriteCandidates(maxEligibleID, wasteThreshold)
}

// SnapshotSegmentIDs returns a sorted copy of all registered segment IDs.
// Used by KeyIndex reconciliation to ensure Pebble matches the RAM index.
func (idx *DurableIndex) SnapshotSegmentIDs() []uint32 {
	return idx.segments.snapshotSegmentIDs()
}

// GetSegmentMetadata returns the metadata for a segment, or nil if not found.
func (idx *DurableIndex) GetSegmentMetadata(segID uint32) *SegmentMetadata {
	idx.segments.segments.RLock()
	defer idx.segments.segments.RUnlock()
	return idx.segments.segments.byID[segID]
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
