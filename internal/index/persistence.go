package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/internal/xmap"
	"go.mills.io/bitcask/v2"
)

// Persistence format constants.
const (
	// ItemSize is the serialized size of an Item (32 bytes).
	// Wire format: Key.Lo(8) + Key.Hi(8) + SegmentID(4) + Offset(4) + PhysicalLen(4) + Flags(4)
	ItemSize = 32

	// ManifestHeaderSize is the header before items.
	// Wire format: SegmentID(4) + CTime(8) + MaxSeqID(8)
	ManifestHeaderSize = 20

	// Key namespace prefixes for disjoint Bitcask keyspaces
	prefixRegular   byte = 0x00 // Regular segment data chunks
	prefixTombstone byte = 0xFF // Tombstone entries

	// ChunkType values (2 bytes in composite key)
	chunkTypeRegular uint16 = 0x0000
)

// maxChunkSize is the maximum size for a Bitcask value (default 256KB).
var maxChunkSize uint64 = 256 << 10

// DurableBatch holds lean Items for a batch in persistent storage.
// This is what gets serialized to Bitcask.
type DurableBatch struct {
	SegmentID uint32
	CTime     int64
	MaxSeqID  uint64 // Highest SeqID in this segment (WAL recovery checkpoint)
	Items     []Item

	// IndexKey is the bitcask key for this record (not serialized).
	// Populated when reading from bitcask, nil when creating new records.
	IndexKey []byte
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

type persistence struct {
	db *bitcask.Bitcask

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
}

func newPersistence(basePath string) (*persistence, error) {
	dbPath := filepath.Join(basePath, "db")
	db, err := bitcask.Open(dbPath, bitcask.WithMaxValueSize(maxChunkSize))
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	return &persistence{
		db:          db,
		segmentMeta: xmap.New[SegmentMetadata, xmap.Pad32](xmap.WithShardShift(8)), // 256 shards
	}, nil
}

// --- Serialization ---

// AppendItem appends an encoded Item to dst.
func AppendItem(dst []byte, item Item) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, item.Key.Lo)
	dst = binary.LittleEndian.AppendUint64(dst, item.Key.Hi)
	dst = binary.LittleEndian.AppendUint32(dst, item.SegmentID)
	dst = binary.LittleEndian.AppendUint32(dst, item.Offset)
	dst = binary.LittleEndian.AppendUint32(dst, item.PhysicalLen)
	dst = binary.LittleEndian.AppendUint32(dst, item.Flags)
	return dst
}

// DecodeItem decodes an Item from src.
func DecodeItem(src []byte) (Item, error) {
	if len(src) < ItemSize {
		return Item{}, errors.New("buffer too small for Item")
	}
	return Item{
		Key: Key{
			Lo: binary.LittleEndian.Uint64(src[0:8]),
			Hi: binary.LittleEndian.Uint64(src[8:16]),
		},
		SegmentID:   binary.LittleEndian.Uint32(src[16:20]),
		Offset:      binary.LittleEndian.Uint32(src[20:24]),
		PhysicalLen: binary.LittleEndian.Uint32(src[24:28]),
		Flags:       binary.LittleEndian.Uint32(src[28:32]),
	}, nil
}

// AppendBatch appends an encoded DurableBatch to dst.
func AppendBatch(dst []byte, m DurableBatch) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, m.SegmentID)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(m.CTime))
	dst = binary.LittleEndian.AppendUint64(dst, m.MaxSeqID)
	for i := range m.Items {
		dst = AppendItem(dst, m.Items[i])
	}
	return dst
}

// DecodeBatch decodes a DurableBatch from src.
func DecodeBatch(src []byte) (DurableBatch, error) {
	if len(src) < ManifestHeaderSize {
		return DurableBatch{}, errors.New("buffer too small for manifest header")
	}

	segmentID := binary.LittleEndian.Uint32(src[0:4])
	ctime := int64(binary.LittleEndian.Uint64(src[4:12]))
	maxSeqID := binary.LittleEndian.Uint64(src[12:20])

	itemsData := src[ManifestHeaderSize:]
	if len(itemsData)%ItemSize != 0 {
		return DurableBatch{}, errors.New("invalid manifest size")
	}

	numItems := len(itemsData) / ItemSize
	items := make([]Item, numItems)
	for i := 0; i < numItems; i++ {
		offset := i * ItemSize
		item, err := DecodeItem(itemsData[offset : offset+ItemSize])
		if err != nil {
			return DurableBatch{}, err
		}
		items[i] = item
	}

	return DurableBatch{
		SegmentID: segmentID,
		CTime:     ctime,
		MaxSeqID:  maxSeqID,
		Items:     items,
	}, nil
}

// --- Persistence Operations ---

// maxChunkIdx is the largest possible chunk index (uint16).
const maxChunkIdx = 0xFFFF

// makeKey creates a 9-byte BigEndian composite key: [Prefix:1][SegmentID:4][ChunkType:2][ChunkIdx:2].
// This structure supports:
// - 4B segments (4 billion)
// - 65K chunks per segment (far more than needed: 128MB / 256KB = 512 chunks max)
// - Separate namespaces for regular data vs tombstones via ChunkType
func (p *persistence) makeKey(segID uint32, chunkType uint16, chunkIdx uint16) []byte {
	key := make([]byte, 9)
	key[0] = prefixRegular // Regular data prefix
	binary.BigEndian.PutUint32(key[1:5], segID)
	binary.BigEndian.PutUint16(key[5:7], chunkType)
	binary.BigEndian.PutUint16(key[7:9], chunkIdx)
	return key
}

// makeRegularKey creates a key for regular segment data chunks.
func (p *persistence) makeRegularKey(segID uint32, chunkIdx uint16) []byte {
	return p.makeKey(segID, chunkTypeRegular, chunkIdx)
}

// makeTombstoneKey creates a key for a tombstone entry.
//
// Structure (two variants):
//   - Without user key (eviction): [0xFF][SegmentID:4][KeyHash.Lo:8][KeyHash.Hi:8]
//     Total: 21 bytes
//   - With user key (explicit Delete): [0xFF][SegmentID:4][KeyHash.Lo:8][KeyHash.Hi:8][UserKey:variable]
//     Total: 21 + len(userKey) bytes
//
// The user key is optional:
//   - Eviction: userKey is nil (no disk read needed, accepts hash collision risk in cache mode)
//   - Delete: userKey provided (enables collision detection in CAS mode)
//
// Benefits:
// - Separate namespace from regular keys (0xFF prefix)
// - Natural grouping by segment for range scans
// - Transient memory overhead (tombstones dropped during compaction)
func (p *persistence) makeTombstoneKey(segID uint32, keyHash Key, userKey []byte) []byte {
	keyLen := 21 + len(userKey)
	key := make([]byte, keyLen)

	key[0] = prefixTombstone
	binary.BigEndian.PutUint32(key[1:5], segID)
	binary.BigEndian.PutUint64(key[5:13], keyHash.Lo)
	binary.BigEndian.PutUint64(key[13:21], keyHash.Hi)

	if len(userKey) > 0 {
		copy(key[21:], userKey)
	}

	return key
}

func (p *persistence) writeBatch(segID uint32, items []Item, maxSeqID uint64) error {
	if len(items) == 0 {
		return nil
	}

	txn := p.db.Transaction()
	defer txn.Discard()

	currentManifest := DurableBatch{
		SegmentID: segID,
		CTime:     time.Now().Unix(),
		MaxSeqID:  maxSeqID,
	}

	var chunkIdx uint16
	currentSize := ManifestHeaderSize

	flush := func() error {
		if len(currentManifest.Items) == 0 {
			return nil
		}
		data := AppendBatch(nil, currentManifest)
		if err := txn.Put(p.makeRegularKey(segID, chunkIdx), data); err != nil {
			return err
		}
		currentManifest.Items = currentManifest.Items[:0]
		currentSize = ManifestHeaderSize
		chunkIdx++
		return nil
	}

	for i := range items {
		// Check if adding this item would exceed the limit
		if uint64(currentSize+ItemSize) > maxChunkSize && len(currentManifest.Items) > 0 {
			if err := flush(); err != nil {
				return err
			}
			currentManifest.CTime = time.Now().Unix()
		}

		currentManifest.Items = append(currentManifest.Items, items[i])
		currentSize += ItemSize
	}

	if err := flush(); err != nil {
		return err
	}
	return txn.Commit()
}

func (p *persistence) scanAll(fn ScanBatchFn) error {
	// Use transaction for consistent snapshot
	txn := p.db.Transaction()
	defer txn.Discard()

	// Load ALL tombstones first (across all segments)
	allTombstones := make(map[Key]struct{})

	// Use ForEach and filter by prefix (can't use Range with 0xFF+1 end bound)
	err := p.db.ForEach(func(key bitcask.Key) error {
		// Skip non-tombstone keys
		if len(key) == 0 || key[0] != prefixTombstone {
			return nil
		}
		// Decode Key hash from bytes [5:21]
		if len(key) >= 21 {
			k := Key{
				Lo: binary.BigEndian.Uint64(key[5:13]),
				Hi: binary.BigEndian.Uint64(key[13:21]),
			}
			allTombstones[k] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Scan regular data, merging tombstones
	return p.db.ForEach(func(key bitcask.Key) error {
		// Skip tombstone keys (already processed)
		if len(key) > 0 && key[0] == prefixTombstone {
			return nil
		}

		buf, err := p.db.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get chunk %v: %w", key, err)
		}

		manifest, err := DecodeBatch(buf)
		if err != nil {
			return fmt.Errorf("failed to decode chunk %v: %w", key, err)
		}

		// Merge tombstones into items
		for i := range manifest.Items {
			if _, isTombstone := allTombstones[manifest.Items[i].Key]; isTombstone {
				manifest.Items[i].SetDeleted()
			}
		}

		manifest.IndexKey = key
		if !fn(manifest) {
			return nil
		}
		return nil
	})
}

func (p *persistence) scanSegment(segID uint32, fn ScanBatchFn) error {
	// Use explicit transaction to get consistent snapshot of both tombstones and data
	txn := p.db.Transaction()
	defer txn.Discard()

	// 1. Load tombstones first (consistent view within transaction)
	tombstones := make(map[Key]struct{})
	tombstoneStart := make([]byte, 5)
	tombstoneStart[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombstoneStart[1:5], segID)

	tombstoneEnd := make([]byte, 5)
	tombstoneEnd[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombstoneEnd[1:5], segID+1)

	err := txn.Range(tombstoneStart, tombstoneEnd, func(key bitcask.Key) error {
		if len(key) >= 21 {
			k := Key{
				Lo: binary.BigEndian.Uint64(key[5:13]),
				Hi: binary.BigEndian.Uint64(key[13:21]),
			}
			tombstones[k] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 2. Scan regular data chunks (same transaction = consistent view)
	start := p.makeRegularKey(segID, 0)
	end := p.makeRegularKey(segID, maxChunkIdx)

	return txn.Range(start, end, func(key bitcask.Key) error {
		buf, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get chunk %v: %w", key, err)
		}

		manifest, err := DecodeBatch(buf)
		if err != nil {
			return fmt.Errorf("failed to decode chunk %v: %w", key, err)
		}

		// Merge tombstones into items
		for i := range manifest.Items {
			if _, isTombstone := tombstones[manifest.Items[i].Key]; isTombstone {
				manifest.Items[i].SetDeleted()
			}
		}

		manifest.IndexKey = key
		if !fn(manifest) {
			return nil
		}
		return nil
	})
}

// scanRange scans all chunks across multiple segments [startSegID, endSegID] (inclusive).
// Used for gap detection during compaction validation.
func (p *persistence) scanRange(startSegID, endSegID uint32, fn ScanBatchFn) error {
	start := p.makeRegularKey(startSegID, 0)
	end := p.makeRegularKey(endSegID, maxChunkIdx)

	return p.db.Range(start, end, func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
}

// dropSegment deletes all Bitcask entries for a segment.
// Used after compaction when segment data has been moved to a new segment.
func (p *persistence) dropSegment(segID uint32) error {
	start := p.makeRegularKey(segID, 0)
	end := p.makeRegularKey(segID, maxChunkIdx)

	txn := p.db.Transaction()
	defer txn.Discard()

	err := p.db.Range(start, end, func(key bitcask.Key) error {
		return txn.Delete(key)
	})
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (p *persistence) loadAndInvoke(key bitcask.Key, fn ScanBatchFn) error {
	buf, err := p.db.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get chunk %v: %w", key, err)
	}

	manifest, err := DecodeBatch(buf)
	if err != nil {
		return fmt.Errorf("failed to decode chunk %v: %w", key, err)
	}

	manifest.IndexKey = key
	if !fn(manifest) {
		// Stop requested by caller
		return nil
	}
	return nil
}

// --- Tombstone Operations ---

// tombstone writes a single tombstone to the incremental log.
// Tombstones are stored in a separate namespace (0xFF prefix) with full user key.
//
// This is a simple synchronous write (no transaction coordination needed).
// Returns immediately - tombstones are merged during scanSegment().
func (p *persistence) tombstone(segID uint32, keyHash Key, userKey []byte) error {
	key := p.makeTombstoneKey(segID, keyHash, userKey)

	// Value: timestamp for observability (when was this deleted?)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(time.Now().Unix()))

	return p.db.Put(key, value)
}

// tombstoneBatch writes multiple tombstones in a single transaction.
// Used by eviction where we delete many items at once - batching avoids
// acquiring the Bitcask lock and cloning the radix tree N times.
func (p *persistence) tombstoneBatch(items []Item) error {
	if len(items) == 0 {
		return nil
	}

	txn := p.db.Transaction()
	defer txn.Discard()

	// Value: timestamp for observability (when was this deleted?)
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], uint64(time.Now().Unix()))

	for _, item := range items {
		// Eviction tombstones have no user key (nil)
		key := p.makeTombstoneKey(item.SegmentID, item.Key, nil)
		if err := txn.Put(key, value[:]); err != nil {
			return err
		}
	}
	return txn.Commit()
}

// compactTombstones merges the tombstone incremental log into the segment manifest.
// This is a metadata cleanup operation that also enables space reclamation.
//
// The onTombstone callback is invoked for each tombstone with its associated item.
// Caller can use this to hole punch the blob (idempotent operation):
// - CAS mode deletes: First hole punch happens here
// - Cache mode deletes/eviction: Already hole-punched, this is a no-op
//
// All operations in a single transaction for atomicity.
func (p *persistence) compactTombstones(segID uint32, onTombstone TombstoneFn) error {
	txn := p.db.Transaction()
	defer txn.Discard()

	// 1. Load tombstones within transaction
	tombstones := make(map[Key]bool)
	tombStart := make([]byte, 5)
	tombStart[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombStart[1:5], segID)

	tombEnd := make([]byte, 5)
	tombEnd[0] = prefixTombstone
	binary.BigEndian.PutUint32(tombEnd[1:5], segID+1)

	err := txn.Range(tombStart, tombEnd, func(key bitcask.Key) error {
		if len(key) >= 21 {
			k := Key{
				Lo: binary.BigEndian.Uint64(key[5:13]),
				Hi: binary.BigEndian.Uint64(key[13:21]),
			}
			tombstones[k] = true
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(tombstones) == 0 {
		return nil // Nothing to compact
	}

	// 2. Read segment manifest
	var allItems []Item
	var cTime int64
	var maxSeqID uint64
	var originalKeys [][]byte

	dataStart := p.makeRegularKey(segID, 0)
	dataEnd := p.makeRegularKey(segID, maxChunkIdx)

	err = txn.Range(dataStart, dataEnd, func(key bitcask.Key) error {
		buf, err := txn.Get(key)
		if err != nil {
			return err
		}

		manifest, err := DecodeBatch(buf)
		if err != nil {
			return err
		}

		originalKeys = append(originalKeys, []byte(key))
		if cTime == 0 {
			cTime = manifest.CTime
		}
		if manifest.MaxSeqID > maxSeqID {
			maxSeqID = manifest.MaxSeqID
		}

		allItems = append(allItems, manifest.Items...)
		return nil
	})
	if err != nil {
		return err
	}

	// 3. Invoke callback for each tombstone (caller can hole punch)
	if onTombstone != nil {
		for i := range allItems {
			if tombstones[allItems[i].Key] && !allItems[i].IsDeleted() {
				onTombstone(TombstoneRecord{
					KeyHash: allItems[i].Key,
					Item:    allItems[i],
				})
			}
		}
	}

	// 4. Mark items as deleted
	for i := range allItems {
		if tombstones[allItems[i].Key] {
			allItems[i].SetDeleted()
		}
	}

	// 5. Delete old manifest keys
	for _, k := range originalKeys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}

	// 6. Write updated manifest with tombstones collapsed
	newManifest := DurableBatch{
		SegmentID: segID,
		CTime:     cTime,
		MaxSeqID:  maxSeqID,
	}

	var chunkIdx uint16
	currentSize := ManifestHeaderSize

	for i := range allItems {
		if uint64(currentSize+ItemSize) > maxChunkSize && len(newManifest.Items) > 0 {
			data := AppendBatch(nil, newManifest)
			if err := txn.Put(p.makeRegularKey(segID, chunkIdx), data); err != nil {
				return err
			}
			chunkIdx++
			newManifest.Items = newManifest.Items[:0]
			currentSize = ManifestHeaderSize
		}

		newManifest.Items = append(newManifest.Items, allItems[i])
		currentSize += ItemSize

		if i == len(allItems)-1 {
			data := AppendBatch(nil, newManifest)
			if err := txn.Put(p.makeRegularKey(segID, chunkIdx), data); err != nil {
				return err
			}
		}
	}

	// 7. Drop tombstone log entries
	err = p.db.Range(tombStart, tombEnd, func(key bitcask.Key) error {
		return txn.Delete(key)
	})
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (p *persistence) close() error {
	return p.db.Close()
}
