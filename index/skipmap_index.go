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
	data *skipmap.StringMap[*Entry]

	// Optional persistent backing store
	bitcask *BitcaskIndex
}

// NewSkipmapIndex creates a pure in-memory skipmap index (no persistence)
func NewSkipmapIndex() *SkipmapIndex {
	return &SkipmapIndex{
		data: skipmap.NewString[*Entry](),
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
	data := skipmap.NewString[*Entry]()

	// Load all entries from Bitcask into skipmap
	ctx := context.Background()
	keys, err := bitcask.GetAllKeys(ctx)
	if err != nil {
		bitcask.Close()
		return nil, err
	}

	for _, keyBytes := range keys {
		var entry Entry
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

// Put inserts or updates an entry
func (idx *SkipmapIndex) Put(ctx context.Context, key base.Key, size int, ctime int64) error {
	entry := &Entry{
		Key:       key.Raw(),
		SegmentID: 0,
		Pos:       0,
		Size:      size,
		CTime:     ctime,
	}
	keyStr := unsafe.String(unsafe.SliceData(key.Raw()), len(key.Raw()))
	idx.data.Store(keyStr, entry)

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.Put(ctx, key, size, ctime)
	}
	return nil
}

// Get retrieves an entry
func (idx *SkipmapIndex) Get(ctx context.Context, key base.Key, entry *Entry) error {
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
	idx.data.Range(func(key string, entry *Entry) bool {
		total.Add(int64(entry.Size))
		return true
	})
	return total.Load(), nil
}

// GetAllKeys returns all keys for bloom filter reconstruction
func (idx *SkipmapIndex) GetAllKeys(ctx context.Context) ([][]byte, error) {
	var keys [][]byte
	idx.data.Range(func(key string, entry *Entry) bool {
		keyCopy := make([]byte, len(entry.Key))
		copy(keyCopy, entry.Key)
		keys = append(keys, keyCopy)
		return true
	})
	return keys, nil
}

// GetOldestEntries returns iterator over N oldest entries by ctime
func (idx *SkipmapIndex) GetOldestEntries(ctx context.Context, limit int) EntryIteratorInterface {
	// Collect all entries and sort by ctime
	var entries []*Entry
	idx.data.Range(func(key string, entry *Entry) bool {
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

	return &SkipmapEntryIterator{entries: entries, idx: -1}
}

// SkipmapEntryIterator iterates over in-memory sorted entries
type SkipmapEntryIterator struct {
	entries []*Entry
	idx     int
	err     error
}

func (it *SkipmapEntryIterator) Next() bool {
	it.idx++
	return it.idx < len(it.entries)
}

func (it *SkipmapEntryIterator) Entry() (Entry, error) {
	if it.idx >= len(it.entries) {
		return Entry{}, fmt.Errorf("iterator exhausted")
	}
	return *it.entries[it.idx], nil
}

func (it *SkipmapEntryIterator) Err() error {
	return it.err
}

func (it *SkipmapEntryIterator) Close() error {
	return nil
}

// PutBatch inserts multiple records
func (idx *SkipmapIndex) PutBatch(ctx context.Context, records []Record) error {
	// Store in skipmap
	for _, rec := range records {
		entry := &Entry{
			Key:       rec.Key.Raw(),
			SegmentID: rec.SegmentID,
			Pos:       rec.Pos,
			Size:      rec.Size,
			CTime:     rec.CTime,
		}
		keyStr := unsafe.String(unsafe.SliceData(rec.Key.Raw()), len(rec.Key.Raw()))
		idx.data.Store(keyStr, entry)
	}

	// Write to Bitcask if durable
	if idx.bitcask != nil {
		return idx.bitcask.PutBatch(ctx, records)
	}
	return nil
}
