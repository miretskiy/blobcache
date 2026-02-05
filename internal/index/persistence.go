package index

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// Persistence format constants for the unified .meta file.
//
// File layout:
//   [SegmentFooter block (page-aligned, written by WriteFooter)]
//   [Tombstone batch 1 (appended on delete)]
//   [Tombstone batch 2 (appended on delete)]
//   ...
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
}

// ScanBatchFn is the callback for scanning segment manifests.
type ScanBatchFn func(DurableBatch) bool

// TombstoneRecord provides information about a tombstoned item.
// Passed to the callback during tombstone compaction for optional hole punching.
type TombstoneRecord struct {
	KeyHash Key  // 128-bit hash of the deleted key
	Item    Item // The tombstoned item (has Offset, PhysicalLen for hole punching)
}

// TombstoneFn is called for each tombstone during compaction.
// Allows caller to perform I/O operations (e.g., hole punching) before metadata update.
type TombstoneFn func(TombstoneRecord)

// SegmentMetadata tracks per-segment state for compaction decisions and synchronization.
//
// Size: 32 bytes for optimal memory layout with xmap.
// Combined with xmap.Shard base (32 bytes), total = 64 bytes (1 cache line).
//
// Layout:
//   - TombstoneCount: 4 bytes (atomic.Int32)
//   - LiveItemCount:  4 bytes (atomic.Int32)
//   - PhysicalBytes:  8 bytes (int64)
//   - LogicalBytes:   8 bytes (int64)
//   - Padding:        8 bytes (explicit, for 32-byte total)
type SegmentMetadata struct {
	TombstoneCount atomic.Int32 // Incremented on Delete/Evict
	LiveItemCount  atomic.Int32 // Decremented on Delete/Evict
	PhysicalBytes  int64        // Actual disk usage (from stat.Blocks * 512)
	LogicalBytes   int64        // Sum of live item PhysicalLen
	_              [8]byte      // Padding to 32 bytes
}

// metaFile manages append-only tombstone operations for a segment's .meta file.
type metaFile struct {
	mu              sync.Mutex
	path            string
	w               *bufio.Writer
	f               *os.File
	footerBlockSize int64 // Size of the footer block (tombstones start after this)
}

// segmentSnapshot represents a sealed segment with its frozen Bloom filter.
// The filter is a "frozen snapshot" of the segment's physical content at creation time.
// It represents what keys were ever written to the segment, regardless of later deletions.
// This immutability is critical for correct tombstone dissolution decisions.
type segmentSnapshot struct {
	ID          uint32
	SegmentKeys *bloom.Filter // Frozen filter, ~29KB at 32k items / 3% FPR
}

// persistence manages all .meta files for durable segment metadata storage.
type persistence struct {
	basePath string
	shards   int // Number of directory shards for segment files

	// Open .meta file handles (keyed by segment ID).
	// Lazy-opened on first tombstone write for that segment.
	files sync.Map // map[uint32]*metaFile

	// Segment metadata with built-in sharded locking (via xmap).
	// Dual purpose:
	// 1. Track tombstone counts and sparseness for compaction selection
	// 2. Coordinate Delete (exclusive) and Compaction (shared) via xmap's RWMutex
	//
	// Locking protocol:
	// - Delete: Acquires shard.Lock() for one segment (write lock, exclusive)
	// - Compaction: Acquires shard.RLock() for multiple segments (read lock, shared)
	//
	// Type params: V=SegmentMetadata (value in map), E=Pad32 (padding for alignment)
	segmentMeta *xmap.Map[SegmentMetadata, xmap.Pad32]

	// Per-segment Bloom filters for tombstone dissolution decisions.
	// Used by HasOlderShadow() to determine if a key might exist in older segments.
	// Filters are "frozen snapshots" representing physical segment content at creation.
	snapshots struct {
		sync.RWMutex
		entries []segmentSnapshot // Sorted by ID ascending for binary search
	}
}

func newPersistence(basePath string, shards int) (*persistence, error) {
	return &persistence{
		basePath:    basePath,
		shards:      shards,
		segmentMeta: xmap.New[SegmentMetadata, xmap.Pad32](xmap.WithShardShift(8)), // 256 shards
	}, nil
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

	// Apply tombstones to items
	for i := range items {
		if _, deleted := tombstones[items[i].Key]; deleted {
			items[i].SetDeleted()
		}
	}

	return DurableBatch{
		SegmentID: uint32(footer.SegmentID),
		CTime:     footer.CTime,
		MaxSeqID:  footer.MaxSeqID,
		Items:     items,
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
				slog.Warn("failed to load segment manifest, skipping",
					"segment", segID, "path", segPath, "error", err)
				continue
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

// compactTombstones rewrites the .meta file with tombstones collapsed into footer entries.
// The onTombstone callback is invoked for each tombstone from the tombstone batches.
// If there are no tombstone batches (only baked-in deleted items), this is a no-op.
func (p *persistence) compactTombstones(segID uint32, onTombstone TombstoneFn) error {
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
		return nil // Nothing to compact - no tombstones appended
	}

	// Read full manifest with merged tombstones
	manifest, err := p.readMetaFile(segID)
	if err != nil {
		return err
	}

	// Invoke callback for each deleted item (these came from tombstone batches)
	for i := range manifest.Items {
		if manifest.Items[i].IsDeleted() && onTombstone != nil {
			onTombstone(TombstoneRecord{
				KeyHash: manifest.Items[i].Key,
				Item:    manifest.Items[i],
			})
		}
	}

	// Close file handle if open
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
	f.Close()
	if err != nil {
		return err
	}

	// Update footer entries with deleted flags
	tombstoneKeys := make(map[Key]struct{})
	for i := range manifest.Items {
		if manifest.Items[i].IsDeleted() {
			tombstoneKeys[manifest.Items[i].Key] = struct{}{}
		}
	}
	for i := range footer.Entries {
		if _, deleted := tombstoneKeys[footer.Entries[i].Key]; deleted {
			footer.Entries[i].SetDeleted()
		}
	}

	// Rewrite footer block without tombstone appendages
	// We need a pool-like buffer for alignment - use a simple allocation here
	physicalSize := record.SegmentFooterAlignedSize(len(footer.Entries))
	buf := make([]byte, physicalSize)
	data := record.AppendFooterBlock(buf, footer)

	tmpPath := path + ".compact.tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write compacted meta: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename compacted meta: %w", err)
	}

	return nil
}

// --- Segment Snapshot Registry ---

// registerSnapshot adds a segment's frozen Bloom filter to the registry.
// Called after segment flush completes successfully.
// The filter must be Freeze()'d before registration.
func (p *persistence) registerSnapshot(segID uint32, filter *bloom.Filter) {
	p.snapshots.Lock()
	defer p.snapshots.Unlock()

	entry := segmentSnapshot{ID: segID, SegmentKeys: filter}

	// Fast path: append at end (common case - segments created in order)
	if len(p.snapshots.entries) == 0 || p.snapshots.entries[len(p.snapshots.entries)-1].ID < segID {
		p.snapshots.entries = append(p.snapshots.entries, entry)
		return
	}

	// Binary search for insertion point (rare case: out-of-order registration)
	idx := sort.Search(len(p.snapshots.entries), func(i int) bool {
		return p.snapshots.entries[i].ID >= segID
	})

	// Check for duplicate (idempotent registration)
	if idx < len(p.snapshots.entries) && p.snapshots.entries[idx].ID == segID {
		p.snapshots.entries[idx].SegmentKeys = filter // Update existing
		return
	}

	// Insert at idx
	p.snapshots.entries = append(p.snapshots.entries, segmentSnapshot{})
	copy(p.snapshots.entries[idx+1:], p.snapshots.entries[idx:])
	p.snapshots.entries[idx] = entry
}

// unregisterSnapshot removes a segment from the registry.
// Called after compaction merges segments into a new one.
func (p *persistence) unregisterSnapshot(segID uint32) {
	p.snapshots.Lock()
	defer p.snapshots.Unlock()

	idx := sort.Search(len(p.snapshots.entries), func(i int) bool {
		return p.snapshots.entries[i].ID >= segID
	})

	if idx < len(p.snapshots.entries) && p.snapshots.entries[idx].ID == segID {
		p.snapshots.entries = append(p.snapshots.entries[:idx], p.snapshots.entries[idx+1:]...)
	}
}

// unregisterSnapshots removes multiple segments from the registry atomically.
// More efficient than multiple unregisterSnapshot calls when dropping merged segments.
func (p *persistence) unregisterSnapshots(segIDs []uint32) {
	if len(segIDs) == 0 {
		return
	}

	p.snapshots.Lock()
	defer p.snapshots.Unlock()

	// Build lookup set for O(1) membership checks
	toRemove := make(map[uint32]struct{}, len(segIDs))
	for _, id := range segIDs {
		toRemove[id] = struct{}{}
	}

	// Filter in place - preserves sorted order
	n := 0
	for _, entry := range p.snapshots.entries {
		if _, remove := toRemove[entry.ID]; !remove {
			p.snapshots.entries[n] = entry
			n++
		}
	}
	p.snapshots.entries = p.snapshots.entries[:n]
}

// hasOlderShadow checks if any segment with ID < floorID might contain the key.
// Returns true if any older segment's Bloom filter tests positive.
//
// This is the core tombstone dissolution query:
//   - If true: tombstone MUST be preserved (older version may exist)
//   - If false: tombstone can be safely dissolved (no older version)
//
// Thread-safe for concurrent reads.
func (p *persistence) hasOlderShadow(key Key, floorID uint32) bool {
	p.snapshots.RLock()
	defer p.snapshots.RUnlock()

	// Binary search for first segment >= floorID
	floorIdx := sort.Search(len(p.snapshots.entries), func(i int) bool {
		return p.snapshots.entries[i].ID >= floorID
	})

	// Check all segments before floorIdx (ID < floorID)
	for i := 0; i < floorIdx; i++ {
		if p.snapshots.entries[i].SegmentKeys.Test(key) {
			return true // Bloom says "maybe" - must preserve tombstone
		}
	}

	return false // No older shadow - safe to dissolve
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
