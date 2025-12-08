package index

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/miretskiy/blobcache/base"
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
	keys, err := bitcask.GetAllKeys(ctx)
	if err != nil {
		bitcask.Close()
		return nil, err
	}

	for _, keyBytes := range keys {
		var entry Record
		k := base.NewKey(keyBytes, 256) // Reconstruct Key (shard count doesn't matter for Get)
		if err := bitcask.Get(ctx, k, &entry); err != nil {
			continue // Skip errors
		}

		keyStr := unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
		entryCopy := entry // Copy to heap
		data.Store(keyStr, &entryCopy)
	}

	return &SkipmapIndex{
		data:    data,
		bitcask: bitcask,
	}, nil
}

// Put inserts or updates a record
func (idx *SkipmapIndex) Put(ctx context.Context, key base.Key, record *Record) error {
	// Set record key if not already set
	if record.Key == nil {
		record.Key = key.Raw()
	}
	keyStr := unsafe.String(unsafe.SliceData(key.Raw()), len(key.Raw()))
	idx.data.Store(keyStr, record)

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.Put(ctx, key, record)
	}
	return nil
}

// Get retrieves an entry
func (idx *SkipmapIndex) Get(ctx context.Context, key base.Key, entry *Record) error {
	keyStr := unsafe.String(unsafe.SliceData(key.Raw()), len(key.Raw()))
	stored, ok := idx.data.Load(keyStr)
	if !ok {
		return ErrNotFound
	}

	// Copy data to caller's entry
	*entry = *stored
	return nil
}

// Delete removes an entry
func (idx *SkipmapIndex) Delete(ctx context.Context, key base.Key) error {
	keyStr := unsafe.String(unsafe.SliceData(key.Raw()), len(key.Raw()))
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

// TotalSizeOnDisk returns total size of all entries
func (idx *SkipmapIndex) TotalSizeOnDisk(ctx context.Context) (int64, error) {
	var total atomic.Int64
	idx.data.Range(func(key string, entry *Record) bool {
		total.Add(int64(entry.Size))
		return true
	})
	return total.Load(), nil
}

// GetAllKeys returns all keys for bloom filter reconstruction
func (idx *SkipmapIndex) GetAllKeys(ctx context.Context) ([][]byte, error) {
	var keys [][]byte
	idx.data.Range(func(key string, entry *Record) bool {
		keyCopy := make([]byte, len(entry.Key))
		copy(keyCopy, entry.Key)
		keys = append(keys, keyCopy)
		return true
	})
	return keys, nil
}

// GetOldestRecords returns iterator over N oldest records by ctime
func (idx *SkipmapIndex) GetOldestRecords(ctx context.Context, limit int) RecordIterator {
	// Collect all entries and sort by ctime
	var entries []*Record
	idx.data.Range(func(key string, entry *Record) bool {
		entries = append(entries, entry)
		return true
	})

	// Sort by ctime ascending (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CTime < entries[j].CTime
	})

	// Limit results
	if len(entries) > limit {
		entries = entries[:limit]
	}

	return &SkipmapRecordIterator{entries: entries, idx: -1}
}

// SkipmapRecordIterator iterates over in-memory sorted records
type SkipmapRecordIterator struct {
	entries []*Record
	idx     int
	err     error
}

func (it *SkipmapRecordIterator) Next() bool {
	it.idx++
	return it.idx < len(it.entries)
}

func (it *SkipmapRecordIterator) Record() (Record, error) {
	if it.idx >= len(it.entries) {
		return Record{}, fmt.Errorf("iterator exhausted")
	}
	return *it.entries[it.idx], nil
}

func (it *SkipmapRecordIterator) Err() error {
	return it.err
}

func (it *SkipmapRecordIterator) Close() error {
	return nil
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
