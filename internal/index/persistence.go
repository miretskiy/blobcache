package index

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// Persistence format constants for the unified .meta file.
//
// File layout:
//
//	[SegmentFooter block (page-aligned, written by WriteFooter)]
//	[Tombstone batch 1 (appended on delete)]
//	[Tombstone batch 2 (appended on delete)]
//	...
//
// The SegmentFooter serves as the "Base Manifest" containing all items.
// Tombstones are appended as items are deleted, then merged on read.
const (
	// TombstoneHeaderSize is the header for each tombstone batch.
	// Wire format: Magic(1) + Count(4) + Timestamp(8)
	TombstoneHeaderSize = 13

	// TombstoneKeySize is the serialized size of a tombstone key (16 bytes).
	// Wire format: Key.Lo(8) + Key.Hi(8)
	TombstoneKeySize = 16

	// TombstoneMagic identifies a tombstone batch (distinguishes from footer data).
	TombstoneMagic byte = 0xDD
)

// DurableBatch holds items read from a segment's .meta file.
// Items are converted from the SegmentFooter's FooterEntry array.
type DurableBatch struct {
	SegmentID uint32
	CTime     int64
	MaxSeqID  uint64 // Highest SeqID in this segment (WAL recovery checkpoint)
	Items     []Item
	Entries   []record.FooterEntry // Raw footer entries (optional, for in-memory caching)
}

// ScanBatchFn is the callback for scanning segment manifests.
type ScanBatchFn func(DurableBatch) bool

// SegmentMetadata tracks per-segment state for compaction decisions.
// Stored by pointer in the segment registry for O(1) lookup and efficient sorting.
//
// Fields are plain types (no atomics) - all access is under the segments lock.
type SegmentMetadata struct {
	ID             uint32
	TombstoneCount int32
	LiveItemCount  int32
	LiveBytes      int64 // Sum of PhysicalLen for live (non-deleted) items

	// SegmentKeys is a frozen Bloom filter snapshot of all keys written to this segment.
	// Immutable after creation — represents physical content at segment creation time.
	// Currently unused (tombstone dissolution uses RAM index lookup instead of Bloom
	// filters). Retained for potential future use.
	SegmentKeys *bloom.Filter

	// Entries holds the raw footer entries for this segment, including full metadata
	// (LogicalSize, PhysicalSize, SeqID, KeyLen) needed by compaction to write output
	// footers without re-reading .meta files from disk. Nil for test-registered segments.
	// Tombstones applied after registration are NOT reflected here; callers must check
	// the RAM index for current deleted status.
	Entries []record.FooterEntry
}

// WasteRatio returns the proportion of tombstones (0.0 to 1.0).
// Returns 0 if segment is empty.
func (m *SegmentMetadata) WasteRatio() float64 {
	total := m.TombstoneCount + m.LiveItemCount
	if total == 0 {
		return 0
	}
	return float64(m.TombstoneCount) / float64(total)
}

// numSegmentLockShards is the number of shards for segment locking.
// Must be a power of 2 for efficient modulo via bitmask.
const numSegmentLockShards = 256

// metaFile manages append-only tombstone operations for a segment's .meta file.
type metaFile struct {
	mu              sync.Mutex
	path            string
	w               *bufio.Writer
	f               *os.File
	footerBlockSize int64 // Size of the footer block (tombstones start after this)
}

// persistence manages all .meta files for durable segment metadata storage.
//
// Lock ordering (two independent tiers — never nested):
//
//	Tier 1: segmentLocks[256] — Delete vs Compaction coordination.
//	  - Delete acquires Lock() on one shard (exclusive, blocks compaction reads).
//	  - Compaction acquires RLock() on N shards (shared; multiple compactions OK).
//	  - Hold time: milliseconds–seconds (covers I/O during tombstone compaction).
//	  - Multiple RLocks on the same shard are safe: Go's sync.RWMutex uses an
//	    additive reader count, so concurrent RLock calls from the same goroutine
//	    increment the counter without deadlock.
//
//	Tier 2: segments.RWMutex — registry metadata protection.
//	  - Guards byID, sorted, and pendingTombstone.
//	  - Hold time: microseconds (in-memory map/slice operations only).
//
//	Rule: these tiers are independent. Code must NEVER hold a segments lock
//	while acquiring a segmentLock (or vice versa). This prevents deadlock
//	and keeps contention isolated.
type persistence struct {
	basePath string
	shards   int // Number of directory shards for segment files

	// Open .meta file handles (keyed by segment ID).
	// Lazy-opened on first tombstone write for that segment.
	files sync.Map // map[uint32]*metaFile

	// Tier 1: Sharded row locks for Delete vs Compaction coordination.
	// Fixed 256-way sharding. Segment ID → shard via segID % 256.
	segmentLocks [numSegmentLockShards]sync.RWMutex

	// Tier 2: Segment registry — metadata + Bloom filters for all registered segments.
	// Protected by segments.RWMutex.
	//
	// Design for O(1) operations:
	// - byID: O(1) lookup by segment ID
	// - sorted: O(1) oldest segment lookup, re-sorted only on add/remove
	segments struct {
		sync.RWMutex

		// O(1) lookup by segment ID
		byID map[uint32]*SegmentMetadata

		// Sorted by ID ascending for O(1) oldest segment lookup.
		// Re-sorted only when segments are added or removed (rare).
		sorted []*SegmentMetadata

		// Lazy tombstone compaction candidate tracking.
		// Populated during updateSegmentOnDelete() when threshold is crossed.
		pendingTombstone map[uint32]struct{} // tombstones >= TombstoneCompactionThreshold

		// Incremental counters for O(1) global avg blob size.
		// Updated by registerSegment, unregisterSegment(s), updateSegmentOnDelete.
		totalLiveBytes int64
		totalLiveItems int64
	}
}

// TombstoneCompactionThreshold is the minimum tombstone count to mark a segment
// as a tombstone compaction candidate. Segments crossing this threshold during
// updateSegmentOnDelete() are added to the pending compaction set.
const TombstoneCompactionThreshold = 100

// CoolingPeriodMargin adds safety margin to the cooling period.
// This ensures segments are fully aged out of Librarian before compaction.
const CoolingPeriodMargin = 2

func newPersistence(basePath string, shards int) (*persistence, error) {
	p := &persistence{
		basePath: basePath,
		shards:   shards,
	}
	p.segments.byID = make(map[uint32]*SegmentMetadata)
	p.segments.pendingTombstone = make(map[uint32]struct{})
	return p, nil
}

// metaPath returns the path to a segment's .meta file.
// Uses same shard directory as segment files: basePath/segments/SHARD/SEGID.meta
func (p *persistence) metaPath(segID uint32) string {
	shardNo := segID % uint32(max(1, p.shards))
	return filepath.Join(p.basePath, "segments",
		fmt.Sprintf("%04d", shardNo),
		fmt.Sprintf("%d.meta", segID),
	)
}

// --- Footer to Item Conversion ---

// footerEntryToItem converts a FooterEntry to an Item.
func footerEntryToItem(segID uint32, e *record.FooterEntry) Item {
	physicalLen := int64(record.HeaderSize) + int64(e.KeyLen) + e.PhysicalSize
	item := Item{
		Key:         e.Key,
		SegmentID:   segID,
		Offset:      uint32(e.Pos),
		PhysicalLen: uint32(physicalLen),
	}
	item.SetCompression(e.Compression())
	if e.IsDeleted() {
		item.SetDeleted()
	}
	return item
}

// --- File Operations ---

// findFooterBlockSize determines the size of the footer block in a .meta file.
// The footer block is page-aligned and ends with the tail magic.
func findFooterBlockSize(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	fileSize := stat.Size()

	// Try reading the tail from the end of file first (no tombstones case)
	if fileSize >= record.TailSize {
		f, err := os.Open(path)
		if err != nil {
			return 0, err
		}
		defer f.Close()

		tailBuf := make([]byte, record.TailSize)
		if _, err := f.ReadAt(tailBuf, fileSize-record.TailSize); err == nil {
			if tail, err := record.DecodeSegmentTail(tailBuf); err == nil {
				// Valid tail at end - footer block is the entire file
				return sys.PageAlign(tail.DataLen + record.TailSize), nil
			}
		}
	}

	// Tombstones were appended - scan for tail magic
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Read file in chunks looking for tail magic
	// The tail magic is at offset (footerBlockSize - 8)
	const pageSize = 4096
	buf := make([]byte, pageSize)

	for offset := int64(0); offset < fileSize; offset += pageSize {
		n, err := f.ReadAt(buf, offset)
		if err != nil && n == 0 {
			break
		}

		// Look for tail magic in this page
		for i := 0; i <= n-record.TailSize; i++ {
			// Check if this looks like a valid tail position
			magic := binary.LittleEndian.Uint64(buf[i+12 : i+20])
			if magic == record.TailMagic {
				// Found tail magic - verify by reading the full tail
				tailOffset := offset + int64(i)
				possibleBlockEnd := tailOffset + record.TailSize
				roundedEnd := sys.PageAlign(possibleBlockEnd)

				// The footer block should end at a page boundary
				if roundedEnd <= fileSize {
					tailBuf := make([]byte, record.TailSize)
					if _, err := f.ReadAt(tailBuf, tailOffset); err == nil {
						if tail, err := record.DecodeSegmentTail(tailBuf); err == nil {
							// Verify: data length should match position
							expectedStart := tailOffset - tail.DataLen
							if expectedStart >= 0 {
								return sys.PageAlign(tail.DataLen + record.TailSize), nil
							}
						}
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("could not find footer block in %s", path)
}

// openMetaFile opens a .meta file for appending tombstones.
func (p *persistence) openMetaFile(segID uint32) (*metaFile, error) {
	// Check if already open
	if v, ok := p.files.Load(segID); ok {
		return v.(*metaFile), nil
	}

	path := p.metaPath(segID)

	// Find footer block size (tombstones start after this)
	footerBlockSize, err := findFooterBlockSize(path)
	if err != nil {
		return nil, fmt.Errorf("find footer block size for %s: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open meta file %s: %w", path, err)
	}

	mf := &metaFile{
		path:            path,
		f:               f,
		w:               bufio.NewWriterSize(f, 4096), // 4KB buffer for tombstone batching
		footerBlockSize: footerBlockSize,
	}

	// Store or return existing (race-safe)
	actual, loaded := p.files.LoadOrStore(segID, mf)
	if loaded {
		// Another goroutine beat us - close our file and use theirs
		_ = f.Close()
		return actual.(*metaFile), nil
	}

	return mf, nil
}

// tombstone writes a single tombstone to the segment's .meta file.
// Tombstones are buffered and NOT fsynced for performance.
func (p *persistence) tombstone(segID uint32, keyHash Key, _ []byte) error {
	mf, err := p.openMetaFile(segID)
	if err != nil {
		return err
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()

	// Write tombstone batch header (single tombstone)
	var hdr [TombstoneHeaderSize]byte
	hdr[0] = TombstoneMagic
	binary.LittleEndian.PutUint32(hdr[1:5], 1) // count = 1
	binary.LittleEndian.PutUint64(hdr[5:13], uint64(time.Now().Unix()))

	if _, err := mf.w.Write(hdr[:]); err != nil {
		return err
	}

	// Write tombstone key
	var key [TombstoneKeySize]byte
	binary.LittleEndian.PutUint64(key[0:8], keyHash.Lo)
	binary.LittleEndian.PutUint64(key[8:16], keyHash.Hi)

	_, err = mf.w.Write(key[:])
	return err
}

// tombstoneBatch writes multiple tombstones in a single batch.
// Used by eviction where we delete many items at once.
func (p *persistence) tombstoneBatch(items []Item) error {
	if len(items) == 0 {
		return nil
	}

	// Group items by segment ID
	bySegment := make(map[uint32][]Item)
	for _, item := range items {
		bySegment[item.SegmentID] = append(bySegment[item.SegmentID], item)
	}

	// Write tombstone batch for each segment
	for segID, segItems := range bySegment {
		mf, err := p.openMetaFile(segID)
		if err != nil {
			return err
		}

		mf.mu.Lock()

		// Write tombstone batch header
		var hdr [TombstoneHeaderSize]byte
		hdr[0] = TombstoneMagic
		binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(segItems)))
		binary.LittleEndian.PutUint64(hdr[5:13], uint64(time.Now().Unix()))

		if _, err := mf.w.Write(hdr[:]); err != nil {
			mf.mu.Unlock()
			return err
		}

		// Write all tombstone keys
		for _, item := range segItems {
			var key [TombstoneKeySize]byte
			binary.LittleEndian.PutUint64(key[0:8], item.Key.Lo)
			binary.LittleEndian.PutUint64(key[8:16], item.Key.Hi)

			if _, err := mf.w.Write(key[:]); err != nil {
				mf.mu.Unlock()
				return err
			}
		}

		mf.mu.Unlock()
	}

	return nil
}

// flushMetaFile flushes buffered writes for a segment's .meta file.
func (p *persistence) flushMetaFile(segID uint32) error {
	v, ok := p.files.Load(segID)
	if !ok {
		return nil
	}
	mf := v.(*metaFile)

	mf.mu.Lock()
	defer mf.mu.Unlock()

	return mf.w.Flush()
}

// readMetaFile reads a segment's .meta file and returns items with merged tombstones.
func (p *persistence) readMetaFile(segID uint32) (DurableBatch, error) {
	// Flush any pending buffered writes for this segment before reading
	if v, ok := p.files.Load(segID); ok {
		mf := v.(*metaFile)
		mf.mu.Lock()
		if err := mf.w.Flush(); err != nil {
			mf.mu.Unlock()
			return DurableBatch{}, fmt.Errorf("flush pending meta for segment %d: %w", segID, err)
		}
		mf.mu.Unlock()
	}

	path := p.metaPath(segID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DurableBatch{}, nil
		}
		return DurableBatch{}, fmt.Errorf("open meta file %s: %w", path, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return DurableBatch{}, err
	}
	fileSize := stat.Size()

	if fileSize < record.TailSize {
		return DurableBatch{}, fmt.Errorf("meta file %s too small: %d bytes", path, fileSize)
	}

	// Find footer block size
	footerBlockSize, err := findFooterBlockSize(path)
	if err != nil {
		return DurableBatch{}, err
	}

	// Read SegmentFooter using record package
	footer, _, err := record.ReadFooterBlock(f, footerBlockSize, int64(segID))
	if err != nil {
		return DurableBatch{}, fmt.Errorf("read footer from %s: %w", path, err)
	}

	// Convert FooterEntries to Items
	items := make([]Item, len(footer.Entries))
	for i := range footer.Entries {
		items[i] = footerEntryToItem(uint32(footer.SegmentID), &footer.Entries[i])
	}

	// Read tombstones (appended after footer block)
	tombstones := make(map[Key]struct{})

	if fileSize > footerBlockSize {
		// Read tombstone data
		tombstoneData := make([]byte, fileSize-footerBlockSize)
		if _, err := f.ReadAt(tombstoneData, footerBlockSize); err != nil {
			return DurableBatch{}, fmt.Errorf("read tombstones from %s: %w", path, err)
		}

		// Parse tombstone batches
		pos := 0
		for pos < len(tombstoneData) {
			if pos+TombstoneHeaderSize > len(tombstoneData) {
				break // Truncated header (crash during write)
			}

			magic := tombstoneData[pos]
			if magic != TombstoneMagic {
				break // Unknown data (corruption or end of tombstones)
			}

			count := binary.LittleEndian.Uint32(tombstoneData[pos+1 : pos+5])
			// timestamp at pos+5..pos+13 (unused during read)

			pos += TombstoneHeaderSize

			// Read tombstone keys
			for range count {
				if pos+TombstoneKeySize > len(tombstoneData) {
					break // Truncated key (crash during write)
				}
				k := Key{
					Lo: binary.LittleEndian.Uint64(tombstoneData[pos : pos+8]),
					Hi: binary.LittleEndian.Uint64(tombstoneData[pos+8 : pos+16]),
				}
				tombstones[k] = struct{}{}
				pos += TombstoneKeySize
			}
		}
	}

	// Apply tombstones to items and entries
	for i := range items {
		if _, deleted := tombstones[items[i].Key]; deleted {
			items[i].SetDeleted()
		}
	}
	for i := range footer.Entries {
		if _, deleted := tombstones[footer.Entries[i].Key]; deleted {
			footer.Entries[i].SetDeleted()
		}
	}

	return DurableBatch{
		SegmentID: uint32(footer.SegmentID),
		CTime:     footer.CTime,
		MaxSeqID:  footer.MaxSeqID,
		Items:     items,
		Entries:   footer.Entries,
	}, nil
}

// SegmentManifest holds raw footer entries with tombstones merged.
// Used by compaction to avoid re-reading record headers from disk.
type SegmentManifest struct {
	SegmentID uint32
	MaxSeqID  uint64
	Entries   []record.FooterEntry
}

// Item converts the i-th footer entry to an Item.
func (m *SegmentManifest) Item(i int) Item {
	return footerEntryToItem(m.SegmentID, &m.Entries[i])
}

// readSegmentManifest reads a segment's .meta file and returns raw footer entries
// with tombstones applied (deleted entries have SetDeleted flag).
// Unlike readMetaFile, this preserves full FooterEntry data (LogicalSize,
// PhysicalSize, SeqID, Flags, KeyLen) needed for compaction output footers.
func (p *persistence) readSegmentManifest(segID uint32) (SegmentManifest, error) {
	// Flush any pending buffered writes for this segment before reading
	if v, ok := p.files.Load(segID); ok {
		mf := v.(*metaFile)
		mf.mu.Lock()
		if err := mf.w.Flush(); err != nil {
			mf.mu.Unlock()
			return SegmentManifest{}, fmt.Errorf("flush pending meta for segment %d: %w", segID, err)
		}
		mf.mu.Unlock()
	}

	path := p.metaPath(segID)
	f, err := os.Open(path)
	if err != nil {
		return SegmentManifest{}, err
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return SegmentManifest{}, err
	}
	fileSize := stat.Size()

	footerBlockSize, err := findFooterBlockSize(path)
	if err != nil {
		return SegmentManifest{}, err
	}

	footer, _, err := record.ReadFooterBlock(f, footerBlockSize, int64(segID))
	if err != nil {
		return SegmentManifest{}, fmt.Errorf("read footer from %s: %w", path, err)
	}

	// Read and apply tombstones
	if fileSize > footerBlockSize {
		tombstoneData := make([]byte, fileSize-footerBlockSize)
		if _, err := f.ReadAt(tombstoneData, footerBlockSize); err != nil {
			return SegmentManifest{}, fmt.Errorf("read tombstones from %s: %w", path, err)
		}

		tombstones := make(map[Key]struct{})
		pos := 0
		for pos < len(tombstoneData) {
			if pos+TombstoneHeaderSize > len(tombstoneData) {
				break
			}
			if tombstoneData[pos] != TombstoneMagic {
				break
			}
			count := int(binary.LittleEndian.Uint32(tombstoneData[pos+1:]))
			pos += TombstoneHeaderSize
			for j := 0; j < count && pos+TombstoneKeySize <= len(tombstoneData); j++ {
				var k Key
				k.Lo = binary.LittleEndian.Uint64(tombstoneData[pos:])
				k.Hi = binary.LittleEndian.Uint64(tombstoneData[pos+8:])
				tombstones[k] = struct{}{}
				pos += TombstoneKeySize
			}
		}

		for i := range footer.Entries {
			if _, deleted := tombstones[footer.Entries[i].Key]; deleted {
				footer.Entries[i].SetDeleted()
			}
		}
	}

	return SegmentManifest{
		SegmentID: uint32(footer.SegmentID),
		MaxSeqID:  footer.MaxSeqID,
		Entries:   footer.Entries,
	}, nil
}

// scanAll iterates over all segment files and invokes fn for each manifest.
// It is resilient to missing/corrupt .meta files by scanning .seg files directly.
//
// The scan process for each .seg file:
//  1. Try to read the corresponding .meta file (fast path)
//  2. If .meta is missing or corrupt, scan the .seg file record-by-record
//  3. Optionally rebuild .meta from scanned records for future fast startup
func (p *persistence) scanAll(fn ScanBatchFn) error {
	segmentsDir := filepath.Join(p.basePath, "segments")

	// Iterate over shard directories
	shardDirs, err := os.ReadDir(segmentsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No segments yet
		}
		return fmt.Errorf("read segments dir: %w", err)
	}

	for _, shardEntry := range shardDirs {
		if !shardEntry.IsDir() {
			continue
		}

		shardPath := filepath.Join(segmentsDir, shardEntry.Name())
		entries, err := os.ReadDir(shardPath)
		if err != nil {
			continue // Skip unreadable directories
		}

		for _, entry := range entries {
			// Look for .seg files (the actual data), not .meta files
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
				continue
			}

			var segID uint32
			if _, err := fmt.Sscanf(entry.Name(), "%d.seg", &segID); err != nil {
				continue // Skip non-conforming files
			}

			segPath := filepath.Join(shardPath, entry.Name())
			manifest, err := p.loadSegmentManifest(segID, segPath)
			if err != nil {
				return fmt.Errorf("load segment %d manifest: %w", segID, err)
			}

			if len(manifest.Items) == 0 {
				continue
			}

			if !fn(manifest) {
				return nil // Caller requested stop
			}
		}
	}

	// Also scan for orphan .meta files (where .seg is missing)
	// These should be cleaned up.
	for _, shardEntry := range shardDirs {
		if !shardEntry.IsDir() {
			continue
		}

		shardPath := filepath.Join(segmentsDir, shardEntry.Name())
		entries, _ := os.ReadDir(shardPath)

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".meta") {
				continue
			}

			var segID uint32
			if _, err := fmt.Sscanf(entry.Name(), "%d.meta", &segID); err != nil {
				continue
			}

			// Check if corresponding .seg exists
			segPath := filepath.Join(shardPath, fmt.Sprintf("%d.seg", segID))
			if _, err := os.Stat(segPath); os.IsNotExist(err) {
				metaPath := filepath.Join(shardPath, entry.Name())
				slog.Warn("removing orphan .meta file (no corresponding .seg)",
					"path", metaPath)
				_ = os.Remove(metaPath)
			}
		}
	}

	return nil
}

// loadSegmentManifest loads a segment's manifest, trying .meta first then falling back to scanning.
func (p *persistence) loadSegmentManifest(segID uint32, segPath string) (DurableBatch, error) {
	// Check if .meta file exists
	metaPath := p.metaPath(segID)
	metaExists := true
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		metaExists = false
	}

	// Try to read .meta file first (fast path)
	manifest, err := p.readMetaFile(segID)
	if err == nil && metaExists {
		// .meta exists and was read successfully - use it even if empty
		// (empty means all items were tombstoned)
		return manifest, nil
	}

	// .meta missing or corrupt - scan the .seg file directly
	slog.Info("recovering segment by scanning (meta unavailable)",
		"segment", segID, "meta_exists", metaExists, "meta_error", err)

	f, err := os.Open(segPath)
	if err != nil {
		return DurableBatch{}, fmt.Errorf("open segment file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return DurableBatch{}, fmt.Errorf("stat segment file: %w", err)
	}

	footer, err := record.ScanSegmentFile(f, stat.Size(), segID)
	if err != nil {
		return DurableBatch{}, fmt.Errorf("scan segment file: %w", err)
	}

	// Convert FooterEntries to Items
	items := make([]Item, 0, len(footer.Entries))
	for i := range footer.Entries {
		e := &footer.Entries[i]
		if e.IsDeleted() {
			continue // Skip tombstoned items
		}
		items = append(items, footerEntryToItem(segID, e))
	}

	// Rebuild .meta file for future fast startup
	if len(items) > 0 {
		if err := p.rebuildMetaFile(segPath, footer); err != nil {
			slog.Warn("failed to rebuild .meta file (non-fatal)",
				"segment", segID, "error", err)
		} else {
			slog.Info("rebuilt .meta file from segment scan",
				"segment", segID, "items", len(items))
		}
	}

	return DurableBatch{
		SegmentID: segID,
		CTime:     footer.CTime,
		MaxSeqID:  footer.MaxSeqID,
		Entries:   footer.Entries,
		Items:     items,
	}, nil
}

// rebuildMetaFile writes a .meta file from a scanned SegmentFooter.
func (p *persistence) rebuildMetaFile(segPath string, footer record.SegmentFooter) error {
	metaPath := segPath[:len(segPath)-4] + ".meta" // .seg -> .meta

	// Build the footer block
	buf := record.AppendFooterBlock(nil, footer)

	// Write atomically
	return sys.WriteFile(metaPath, buf, 0)
}

// scanSegment reads a single segment's .meta file.
func (p *persistence) scanSegment(segID uint32, fn ScanBatchFn) error {
	manifest, err := p.readMetaFile(segID)
	if err != nil {
		return err
	}

	if len(manifest.Items) > 0 {
		fn(manifest)
	}
	return nil
}

// scanRange scans all segments in [startSegID, endSegID] (inclusive).
// Used for gap detection during compaction validation.
func (p *persistence) scanRange(startSegID, endSegID uint32, fn ScanBatchFn) error {
	for segID := startSegID; segID <= endSegID; segID++ {
		manifest, err := p.readMetaFile(segID)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		if len(manifest.Items) == 0 {
			continue
		}

		if !fn(manifest) {
			return nil // Caller requested stop
		}
	}
	return nil
}

// compactTombstones rewrites the .meta file with tombstones collapsed into footer entries.
// If there are no tombstone batches (only baked-in deleted items), this is a no-op.
func (p *persistence) compactTombstones(segID uint32) error {
	// Flush any pending buffered writes for this segment
	if v, ok := p.files.Load(segID); ok {
		mf := v.(*metaFile)
		mf.mu.Lock()
		if err := mf.w.Flush(); err != nil {
			mf.mu.Unlock()
			return fmt.Errorf("flush pending meta for segment %d: %w", segID, err)
		}
		mf.mu.Unlock()
	}

	path := p.metaPath(segID)
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Find footer block size
	footerBlockSize, err := findFooterBlockSize(path)
	if err != nil {
		return err
	}

	// If file size equals footer block size, no tombstone batches exist
	if stat.Size() == footerBlockSize {
		return nil // Nothing to compact
	}

	// Read full manifest with merged tombstones
	manifest, err := p.readMetaFile(segID)
	if err != nil {
		return err
	}

	// Close file handle if open (we're about to replace the file)
	if v, ok := p.files.LoadAndDelete(segID); ok {
		mf := v.(*metaFile)
		mf.mu.Lock()
		_ = mf.w.Flush()
		_ = mf.f.Close()
		mf.mu.Unlock()
	}

	// Read original footer to rewrite with updated entries
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	footer, _, err := record.ReadFooterBlock(f, footerBlockSize, int64(segID))
	if err := f.Close(); err != nil {
		slog.Warn("close meta file during compaction", "segID", segID, "error", err)
	}
	if err != nil {
		return err
	}

	// Build tombstone set from merged manifest
	tombstoneKeys := make(map[Key]struct{})
	for i := range manifest.Items {
		if manifest.Items[i].IsDeleted() {
			tombstoneKeys[manifest.Items[i].Key] = struct{}{}
		}
	}

	// Update footer entries with deleted flags
	for i := range footer.Entries {
		if _, deleted := tombstoneKeys[footer.Entries[i].Key]; deleted {
			footer.Entries[i].SetDeleted()
		}
	}

	// Rewrite footer block without tombstone appendages
	buf := record.AppendFooterBlock(nil, footer)

	tmpPath := path + ".compact.tmp"
	if err := os.WriteFile(tmpPath, buf, 0o644); err != nil {
		return fmt.Errorf("write compacted meta: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename compacted meta: %w", err)
	}

	return nil
}

// dropSegment deletes a segment's .meta file.
// Used after compaction when segment data has been moved.
func (p *persistence) dropSegment(segID uint32) error {
	// Close and remove from cache if open
	if v, ok := p.files.LoadAndDelete(segID); ok {
		mf := v.(*metaFile)
		mf.mu.Lock()
		if err := mf.w.Flush(); err != nil {
			slog.Warn("flush meta file before drop", "segID", segID, "error", err)
		}
		if err := mf.f.Close(); err != nil {
			slog.Warn("close meta file before drop", "segID", segID, "error", err)
		}
		mf.mu.Unlock()
	}

	path := p.metaPath(segID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove meta file %s: %w", path, err)
	}
	return nil
}

// --- Segment Registry ---

// registerSegment adds a segment with its metadata and frozen Bloom filter.
// Called after segment flush completes successfully.
// The filter must be Freeze()'d before registration.
// Idempotent: re-registering updates the existing entry.
func (p *persistence) registerSegment(meta SegmentMetadata) {
	p.segments.Lock()
	defer p.segments.Unlock()

	// Check for existing entry (idempotent update)
	if existing, ok := p.segments.byID[meta.ID]; ok {
		// Adjust incremental counters: subtract old, add new
		p.segments.totalLiveBytes += meta.LiveBytes - existing.LiveBytes
		p.segments.totalLiveItems += int64(meta.LiveItemCount) - int64(existing.LiveItemCount)
		// Update in place - pointer in sorted slice points to same object
		*existing = meta
		return
	}

	// New entry - store by pointer for O(1) lookup and efficient sorting
	entry := new(SegmentMetadata)
	*entry = meta
	p.segments.byID[meta.ID] = entry
	p.segments.totalLiveBytes += meta.LiveBytes
	p.segments.totalLiveItems += int64(meta.LiveItemCount)

	// Append to sorted slice and re-sort (segments usually arrive in order,
	// so this is typically O(1) for already-sorted data with optimized sort)
	p.segments.sorted = append(p.segments.sorted, entry)
	sort.Slice(p.segments.sorted, func(i, j int) bool {
		return p.segments.sorted[i].ID < p.segments.sorted[j].ID
	})
}

// unregisterSegment removes a segment from the registry.
// Called after compaction merges segments into a new one.
func (p *persistence) unregisterSegment(segID uint32) {
	p.segments.Lock()
	defer p.segments.Unlock()

	if entry, ok := p.segments.byID[segID]; ok {
		p.segments.totalLiveBytes -= entry.LiveBytes
		p.segments.totalLiveItems -= int64(entry.LiveItemCount)
		delete(p.segments.byID, segID)
	}

	// Remove from sorted slice (binary search + shift)
	idx := sort.Search(len(p.segments.sorted), func(i int) bool {
		return p.segments.sorted[i].ID >= segID
	})
	if idx < len(p.segments.sorted) && p.segments.sorted[idx].ID == segID {
		p.segments.sorted = append(p.segments.sorted[:idx], p.segments.sorted[idx+1:]...)
	}

	delete(p.segments.pendingTombstone, segID)
}

// unregisterSegments removes multiple segments from the registry atomically.
// More efficient than multiple unregisterSegment calls when dropping merged segments.
func (p *persistence) unregisterSegments(segIDs []uint32) {
	if len(segIDs) == 0 {
		return
	}

	p.segments.Lock()
	defer p.segments.Unlock()

	// Build lookup set for O(1) membership checks
	toRemove := make(map[uint32]struct{}, len(segIDs))
	for _, id := range segIDs {
		toRemove[id] = struct{}{}
		if entry, ok := p.segments.byID[id]; ok {
			p.segments.totalLiveBytes -= entry.LiveBytes
			p.segments.totalLiveItems -= int64(entry.LiveItemCount)
			delete(p.segments.byID, id)
		}
		delete(p.segments.pendingTombstone, id)
	}

	// Filter sorted slice in place - preserves sorted order
	n := 0
	for _, entry := range p.segments.sorted {
		if _, remove := toRemove[entry.ID]; !remove {
			p.segments.sorted[n] = entry
			n++
		}
	}
	p.segments.sorted = p.segments.sorted[:n]
}

// updateSegmentOnDelete updates a segment's metadata after items are deleted.
// Called during eviction or explicit delete to track tombstone compaction candidates.
func (p *persistence) updateSegmentOnDelete(segID uint32, deletedCount int32, deletedBytes int64) {
	p.segments.Lock()
	defer p.segments.Unlock()

	entry, ok := p.segments.byID[segID]
	if !ok {
		return
	}

	wasAboveTombstoneThreshold := entry.TombstoneCount >= TombstoneCompactionThreshold

	entry.TombstoneCount += deletedCount
	entry.LiveItemCount -= deletedCount
	entry.LiveBytes -= deletedBytes
	p.segments.totalLiveItems -= int64(deletedCount)
	p.segments.totalLiveBytes -= deletedBytes

	if !wasAboveTombstoneThreshold && entry.TombstoneCount >= TombstoneCompactionThreshold {
		p.segments.pendingTombstone[segID] = struct{}{}
	}
}

// getTombstoneCompactionCandidates returns segment IDs that have crossed the
// tombstone threshold and have cooled past the given boundary.
//
// This is O(K) where K is the number of pending candidates, not O(N) where N is
// total segments. Candidates are NOT consumed on retrieval; call
// acknowledgeTombstoneCompaction() after successful compaction to remove them.
//
// Returns segment IDs sorted in ascending order for deterministic processing.
func (p *persistence) getTombstoneCompactionCandidates(maxEligibleID uint32) []uint32 {
	p.segments.RLock()
	defer p.segments.RUnlock()

	if len(p.segments.pendingTombstone) == 0 {
		return nil
	}

	var candidates []uint32
	for segID := range p.segments.pendingTombstone {
		if segID < maxEligibleID {
			candidates = append(candidates, segID)
		}
	}

	slices.Sort(candidates)

	return candidates
}

// acknowledgeTombstoneCompaction removes a segment from the pending tombstone
// compaction set after successful compaction.
func (p *persistence) acknowledgeTombstoneCompaction(segID uint32) {
	p.segments.Lock()
	defer p.segments.Unlock()
	delete(p.segments.pendingTombstone, segID)
}

// SparseSegment represents a drain candidate with its remaining live data.
type SparseSegment struct {
	ID        uint32
	LiveBytes int64
}

// getDrainCandidates returns all cooled segments (ID < maxEligibleID) sorted
// by LiveBytes ascending (least populated first). Used by pressure-driven drain
// to pick the cheapest segments to sacrifice.
func (p *persistence) getDrainCandidates(maxEligibleID uint32) []SparseSegment {
	p.segments.RLock()
	defer p.segments.RUnlock()

	var candidates []SparseSegment
	for segID, meta := range p.segments.byID {
		if segID >= maxEligibleID {
			continue
		}
		candidates = append(candidates, SparseSegment{
			ID:        segID,
			LiveBytes: meta.LiveBytes,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].LiveBytes < candidates[j].LiveBytes
	})

	return candidates
}

// getRewriteCandidates returns cooled segment IDs with waste ratio at or above
// the threshold, sorted ascending. Includes 100% dead segments (caller handles
// "all dead → just delete" separately).
func (p *persistence) getRewriteCandidates(maxEligibleID uint32, wasteThreshold float64) []uint32 {
	p.segments.RLock()
	defer p.segments.RUnlock()

	var candidates []uint32
	for segID, meta := range p.segments.byID {
		if segID >= maxEligibleID {
			continue
		}
		if meta.WasteRatio() >= wasteThreshold {
			candidates = append(candidates, segID)
		}
	}

	slices.Sort(candidates)
	return candidates
}

// getInMemoryManifest returns the cached manifest for a segment, if available.
// Returns false if the segment has no cached entries (e.g., test-registered segments).
func (p *persistence) getInMemoryManifest(segID uint32) (SegmentManifest, bool) {
	p.segments.RLock()
	defer p.segments.RUnlock()

	entry, ok := p.segments.byID[segID]
	if !ok || entry.Entries == nil {
		return SegmentManifest{}, false
	}

	return SegmentManifest{
		SegmentID: segID,
		Entries:   entry.Entries,
	}, true
}

// getSegmentCount returns the number of registered segments.
// Thread-safe for concurrent reads.
func (p *persistence) getSegmentCount() int {
	p.segments.RLock()
	defer p.segments.RUnlock()
	return len(p.segments.byID)
}

// getGlobalAvgBlobSize returns the average live blob size across all segments.
// Returns 0 if there are no live items. O(K) where K = segment count.
// Thread-safe for concurrent reads.
func (p *persistence) getGlobalAvgBlobSize() int64 {
	p.segments.RLock()
	defer p.segments.RUnlock()

	if p.segments.totalLiveItems == 0 {
		return 0
	}
	return p.segments.totalLiveBytes / p.segments.totalLiveItems
}

// getOldestSegmentID returns the ID of the oldest registered segment, or 0 if none.
// O(1) since the sorted slice is maintained in ascending ID order.
// Thread-safe for concurrent reads.
func (p *persistence) getOldestSegmentID() uint32 {
	p.segments.RLock()
	defer p.segments.RUnlock()

	if len(p.segments.sorted) == 0 {
		return 0
	}
	return p.segments.sorted[0].ID
}

// snapshotSegmentIDs returns a copy of all registered segment IDs, sorted ascending.
// Used for KeyIndex reconciliation to ensure Pebble matches the RAM index.
func (p *persistence) snapshotSegmentIDs() []uint32 {
	p.segments.RLock()
	defer p.segments.RUnlock()

	ids := make([]uint32, len(p.segments.sorted))
	for i, m := range p.segments.sorted {
		ids[i] = m.ID
	}
	return ids
}

// close closes all open .meta file handles and flushes buffers.
func (p *persistence) close() error {
	var errs []error

	p.files.Range(func(key, value any) bool {
		mf := value.(*metaFile)
		mf.mu.Lock()
		if err := mf.w.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush meta %d: %w", key.(uint32), err))
		}
		if err := mf.f.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close meta %d: %w", key.(uint32), err))
		}
		mf.mu.Unlock()
		return true
	})

	return errors.Join(errs...)
}
