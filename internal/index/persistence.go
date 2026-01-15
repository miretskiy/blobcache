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
	// ItemSize is the serialized size of a lean Item (24 bytes).
	// Wire format: Hash(8) + SegmentID(4) + Offset(4) + PhysicalLen(4) + Flags(4)
	ItemSize = 24

	// ManifestHeaderSize is the header before items.
	// Wire format: SegmentID(4) + CTime(8)
	ManifestHeaderSize = 12
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

// SegmentManifest holds lean Items for a segment in persistent storage.
// This is what gets serialized to Bitcask.
type SegmentManifest struct {
	SegmentID uint32
	CTime     int64
	Items     []Item

	// IndexKey is the bitcask key for this record (not serialized).
	// Populated when reading from bitcask, nil when creating new records.
	IndexKey []byte
}

// ScanManifestFn is the callback for scanning segment manifests.
type ScanManifestFn func(SegmentManifest) bool

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
	dst = binary.LittleEndian.AppendUint64(dst, item.Hash)
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
		Hash:        binary.LittleEndian.Uint64(src[0:8]),
		SegmentID:   binary.LittleEndian.Uint32(src[8:12]),
		Offset:      binary.LittleEndian.Uint32(src[12:16]),
		PhysicalLen: binary.LittleEndian.Uint32(src[16:20]),
		Flags:       binary.LittleEndian.Uint32(src[20:24]),
	}, nil
}

// AppendManifest appends an encoded SegmentManifest to dst.
func AppendManifest(dst []byte, m SegmentManifest) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, m.SegmentID)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(m.CTime))
	for i := range m.Items {
		dst = AppendItem(dst, m.Items[i])
	}
	return dst
}

// DecodeManifest decodes a SegmentManifest from src.
func DecodeManifest(src []byte) (SegmentManifest, error) {
	if len(src) < ManifestHeaderSize {
		return SegmentManifest{}, errors.New("buffer too small for manifest header")
	}

	segmentID := binary.LittleEndian.Uint32(src[0:4])
	ctime := int64(binary.LittleEndian.Uint64(src[4:12]))

	itemsData := src[ManifestHeaderSize:]
	if len(itemsData)%ItemSize != 0 {
		return SegmentManifest{}, errors.New("invalid manifest size")
	}

	numItems := len(itemsData) / ItemSize
	items := make([]Item, numItems)
	for i := 0; i < numItems; i++ {
		offset := i * ItemSize
		item, err := DecodeItem(itemsData[offset : offset+ItemSize])
		if err != nil {
			return SegmentManifest{}, err
		}
		items[i] = item
	}

	return SegmentManifest{
		SegmentID: segmentID,
		CTime:     ctime,
		Items:     items,
	}, nil
}

// --- Persistence Operations ---

func (p *persistence) DeleteRecordsFromSegment(segID uint32, hashes map[uint64]struct{}) error {
	var liveItems []Item
	var originalKeys [][]byte
	var cTime int64

	// 1. Flatten and Filter: Collect only what is still alive
	err := p.scanSegment(segID, func(m SegmentManifest) bool {
		originalKeys = append(originalKeys, m.IndexKey)
		if cTime == 0 {
			cTime = m.CTime
		}

		for _, item := range m.Items {
			// If it's NOT in our deletion set AND wasn't already deleted, keep it
			if _, deleted := hashes[item.Hash]; !deleted && !item.IsDeleted() {
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
	currentManifest := SegmentManifest{
		SegmentID: segID,
		CTime:     cTime,
	}

	// Handle the 'empty segment' case: write one empty tombstone at index 0
	if len(liveItems) == 0 {
		data := AppendManifest(nil, currentManifest)
		return txn.Put(p.makeKey(segID, 0), data)
	}

	var chunkIdx uint32
	currentSize := ManifestHeaderSize

	for i := 0; i < len(liveItems); i++ {
		// Check if adding this item would exceed the limit (and we have items to flush)
		if uint64(currentSize+ItemSize) > maxChunkSize && len(currentManifest.Items) > 0 {
			data := AppendManifest(nil, currentManifest)
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
			data := AppendManifest(nil, currentManifest)
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

func (p *persistence) writeBatch(segID uint32, items []Item) error {
	if len(items) == 0 {
		return nil
	}

	txn := p.db.Transaction()
	defer txn.Discard()

	currentManifest := SegmentManifest{
		SegmentID: segID,
		CTime:     time.Now().Unix(),
	}

	var chunkIdx uint32
	currentSize := ManifestHeaderSize

	flush := func() error {
		if len(currentManifest.Items) == 0 {
			return nil
		}
		data := AppendManifest(nil, currentManifest)
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

func (p *persistence) scanAll(fn ScanManifestFn) error {
	return p.db.ForEach(func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
}

func (p *persistence) scanSegment(segID uint32, fn ScanManifestFn) error {
	// Start key: Segment with chunk 0
	start := p.makeKey(segID, 0)

	// End key: Same segment, largest possible chunk index
	end := p.makeKey(segID, 0xFFFFFFFF)

	return p.db.Range(start, end, func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
}

func (p *persistence) loadAndInvoke(key bitcask.Key, fn ScanManifestFn) error {
	buf, err := p.db.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get chunk %v: %w", key, err)
	}

	manifest, err := DecodeManifest(buf)
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
