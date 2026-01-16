package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"time"

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
)

// maxChunkSize is the maximum size for a Bitcask value (default 256KB).
var maxChunkSize uint64 = 256 << 10

// testingSetMaxChunkSize sets a custom max chunk size for testing.
// Usage: defer testingSetMaxChunkSize(123)()
func testingSetMaxChunkSize(size uint64) func() {
	old := maxChunkSize
	maxChunkSize = size
	return func() {
		maxChunkSize = old
	}
}

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

type persistence struct {
	db *bitcask.Bitcask
}

func newPersistence(basePath string) (*persistence, error) {
	dbPath := filepath.Join(basePath, "db")
	db, err := bitcask.Open(dbPath, bitcask.WithMaxValueSize(maxChunkSize))
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	return &persistence{db: db}, nil
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

func (p *persistence) DeleteRecordsFromSegment(segID uint32, keys map[Key]struct{}) error {
	var liveItems []Item
	var originalKeys [][]byte
	var cTime int64

	// 1. Flatten and Filter: Collect only what is still alive
	err := p.scanSegment(segID, func(m DurableBatch) bool {
		originalKeys = append(originalKeys, m.IndexKey)
		if cTime == 0 {
			cTime = m.CTime
		}

		for _, item := range m.Items {
			// If it's NOT in our deletion set AND wasn't already deleted, keep it
			if _, deleted := keys[item.Key]; !deleted && !item.IsDeleted() {
				liveItems = append(liveItems, item)
			}
		}
		return true
	})

	if err != nil || len(originalKeys) == 0 {
		return err
	}

	txn := p.db.Transaction()
	defer txn.Discard()

	// 2. Wipe: Clear the old positional keys
	for _, k := range originalKeys {
		_ = txn.Delete(k)
	}

	// 3. Re-Pack: Write back only the live items into the minimum number of chunks
	currentManifest := DurableBatch{
		SegmentID: segID,
		CTime:     cTime,
	}

	// Handle the 'empty segment' case: write one empty tombstone at index 0
	if len(liveItems) == 0 {
		data := AppendBatch(nil, currentManifest)
		return txn.Put(p.makeKey(segID, 0), data)
	}

	var chunkIdx uint32
	currentSize := ManifestHeaderSize

	for i := 0; i < len(liveItems); i++ {
		// Check if adding this item would exceed the limit (and we have items to flush)
		if uint64(currentSize+ItemSize) > maxChunkSize && len(currentManifest.Items) > 0 {
			data := AppendBatch(nil, currentManifest)
			if err := txn.Put(p.makeKey(segID, chunkIdx), data); err != nil {
				return err
			}
			chunkIdx++
			currentManifest.Items = currentManifest.Items[:0]
			currentSize = ManifestHeaderSize
		}

		currentManifest.Items = append(currentManifest.Items, liveItems[i])
		currentSize += ItemSize

		// Flush on last item
		if i == len(liveItems)-1 {
			data := AppendBatch(nil, currentManifest)
			if err := txn.Put(p.makeKey(segID, chunkIdx), data); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}

// makeKey creates a 12-byte BigEndian composite key: [SegmentID (4)][Sequence (8)].
func (p *persistence) makeKey(segID uint32, chunkIdx uint32) []byte {
	key := make([]byte, 12)
	binary.BigEndian.PutUint32(key[0:4], segID)
	binary.BigEndian.PutUint64(key[4:12], uint64(chunkIdx))
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

	var chunkIdx uint32
	currentSize := ManifestHeaderSize

	flush := func() error {
		if len(currentManifest.Items) == 0 {
			return nil
		}
		data := AppendBatch(nil, currentManifest)
		if err := txn.Put(p.makeKey(segID, chunkIdx), data); err != nil {
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
	return p.db.ForEach(func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
}

func (p *persistence) scanSegment(segID uint32, fn ScanBatchFn) error {
	// Start key: Segment with chunk 0
	start := p.makeKey(segID, 0)

	// End key: Same segment, largest possible chunk index
	end := p.makeKey(segID, 0xFFFFFFFF)

	return p.db.Range(start, end, func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
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

func (p *persistence) delete(keys ...[]byte) error {
	txn := p.db.Transaction()
	defer txn.Discard()
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return txn.Commit()
}

func (p *persistence) close() error {
	return p.db.Close()
}
