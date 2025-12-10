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
	data *skipmap.StringMap[Value]

	// Optional persistent backing store
	bitcask *BitcaskIndex
}

// NewSkipmapIndex creates a pure in-memory skipmap index (no persistence)
func NewSkipmapIndex() *SkipmapIndex {
	return &SkipmapIndex{
		data: skipmap.NewString[Value](),
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
	data := skipmap.NewString[Value]()

	// Load all entries from Bitcask into skipmap
	ctx := context.Background()
	if err := bitcask.Range(ctx, func(kv KeyValue) bool {
		keyStr := unsafe.String(unsafe.SliceData(kv.Key), len(kv.Key))
		data.Store(keyStr, kv.Val)
		return true
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
func (idx *SkipmapIndex) Put(ctx context.Context, key Key, record Value) error {
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	idx.data.Store(keyStr, record)

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.Put(ctx, key, record)
	}
	return nil
}

// Get retrieves an entry
func (idx *SkipmapIndex) Get(ctx context.Context, key Key, val *Value) error {
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	stored, ok := idx.data.Load(keyStr)
	if !ok {
		return ErrNotFound
	}

	// Copy data to caller's entry
	*val = stored
	return nil
}

// Delete removes an entry
func (idx *SkipmapIndex) Delete(ctx context.Context, key Key) error {
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

// Range iterates over all records in the index
func (idx *SkipmapIndex) Range(ctx context.Context, fn func(KeyValue) bool) error {
	var err error
	idx.data.Range(func(keyStr string, record Value) bool {
		key := unsafe.Slice(unsafe.StringData(keyStr), len(keyStr))
		return fn(KeyValue{Key: key, Val: record})
	})
	return err
}

// PutBatch inserts multiple records
func (idx *SkipmapIndex) PutBatch(ctx context.Context, records []KeyValue) error {
	// Store in skipmap
	for _, rec := range records {
		keyStr := unsafe.String(unsafe.SliceData(rec.Key), len(rec.Key))
		idx.data.Store(keyStr, rec.Val)
	}

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.PutBatch(ctx, records)
	}
	return nil
}
