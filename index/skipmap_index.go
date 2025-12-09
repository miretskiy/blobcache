package index

import (
	"context"
	"unsafe"

	"github.com/zhangyunhao116/skipmap"
)

// SkipmapIndex implements a fast in-memory index using skipmap
// Optionally backed by Bitcask for durability
type SkipmapIndex struct {
	// In-memory skipmap for fast lookups
	data *skipmap.StringMap[*Record]

	// Optional persistent backing store
	bitcask *BitcaskIndex
}

// NewSkipmapIndex creates a pure in-memory skipmap index (no persistence)
func NewSkipmapIndex() *SkipmapIndex {
	return &SkipmapIndex{
		data: skipmap.NewString[*Record](),
	}
}

// NewDurableSkipmapIndex creates a skipmap index backed by Bitcask
// Reads are served from in-memory skipmap, writes go to both
func NewDurableSkipmapIndex(basePath string) (*SkipmapIndex, error) {
	// Open Bitcask
	bitcask, err := NewBitcaskIndex(basePath)
	if err != nil {
		return nil, err
	}

	// Create skipmap
	data := skipmap.NewString[*Record]()

	// Load all entries from Bitcask into skipmap
	ctx := context.Background()
	if err := bitcask.Range(ctx, func(record Record) error {
		keyStr := unsafe.String(unsafe.SliceData(record.Key), len(record.Key))
		recordCopy := record // Copy to heap
		data.Store(keyStr, &recordCopy)
		return nil
	}); err != nil {
		bitcask.Close()
		return nil, err
	}

	return &SkipmapIndex{
		data:    data,
		bitcask: bitcask,
	}, nil
}

// Put inserts or updates a record
func (idx *SkipmapIndex) Put(ctx context.Context, key []byte, record *Record) error {
	// Set record key if not already set
	if record.Key == nil {
		record.Key = key
	}
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	idx.data.Store(keyStr, record)

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.Put(ctx, key, record)
	}
	return nil
}

// Get retrieves an entry
func (idx *SkipmapIndex) Get(ctx context.Context, key []byte, entry *Record) error {
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	stored, ok := idx.data.Load(keyStr)
	if !ok {
		return ErrNotFound
	}

	// Copy data to caller's entry
	*entry = *stored
	return nil
}

// Delete removes an entry
func (idx *SkipmapIndex) Delete(ctx context.Context, key []byte) error {
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	idx.data.Delete(keyStr)

	// Delete from Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.Delete(ctx, key)
	}
	return nil
}

// Close closes the Bitcask backing store if present
func (idx *SkipmapIndex) Close() error {
	if idx.bitcask != nil {
		return idx.bitcask.Close()
	}
	return nil
}

// Scan iterates over all records in the index
func (idx *SkipmapIndex) Range(ctx context.Context, fn func(Record) error) error {
	var err error
	idx.data.Range(func(key string, record *Record) bool {
		err = fn(*record)
		return err == nil // Continue if no error
	})
	return err
}

// PutBatch inserts multiple records
func (idx *SkipmapIndex) PutBatch(ctx context.Context, records []Record) error {
	// Store in skipmap
	for _, rec := range records {
		entry := &Record{
			Key:       rec.Key,
			SegmentID: rec.SegmentID,
			Pos:       rec.Pos,
			Size:      rec.Size,
			CTime:     rec.CTime,
			Checksum:  rec.Checksum,
		}
		keyStr := unsafe.String(unsafe.SliceData(rec.Key), len(rec.Key))
		idx.data.Store(keyStr, entry)
	}

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.PutBatch(ctx, records)
	}
	return nil
}
