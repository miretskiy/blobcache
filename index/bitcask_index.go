package index

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"go.mills.io/bitcask/v2"

	"github.com/miretskiy/blobcache/base"
)

// BitcaskIndex implements Index using Bitcask for metadata storage
type BitcaskIndex struct {
	db *bitcask.Bitcask
}

// NewBitcaskIndex creates a new Bitcask-based index
func NewBitcaskIndex(basePath string) (*BitcaskIndex, error) {
	dbPath := filepath.Join(basePath, "bitcask-index")

	db, err := bitcask.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	return &BitcaskIndex{db: db}, nil
}

// encodeValue encodes segment_id, pos, size, and ctime into bytes
func encodeValue(segmentID int64, pos int64, size int, ctime int64) []byte {
	buf := make([]byte, 28) // 8+8+4+8 bytes
	binary.LittleEndian.PutUint64(buf[0:8], uint64(segmentID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(pos))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(size))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(ctime))
	return buf
}

// decodeValue decodes segment_id, pos, size, and ctime from bytes into entry
func decodeValue(buf []byte, entry *Entry) {
	entry.SegmentID = int64(binary.LittleEndian.Uint64(buf[0:8]))
	entry.Pos = int64(binary.LittleEndian.Uint64(buf[8:16]))
	entry.Size = int(binary.LittleEndian.Uint32(buf[16:20]))
	entry.CTime = int64(binary.LittleEndian.Uint64(buf[20:28]))
}

// Put inserts or updates an entry (for compatibility, uses segment_id=0, pos=0)
func (idx *BitcaskIndex) Put(ctx context.Context, key base.Key, size int, ctime int64) error {
	value := encodeValue(0, 0, size, ctime)
	return idx.db.Put(key.Raw(), value)
}

// Get retrieves an entry (caller provides Entry to avoid allocation)
func (idx *BitcaskIndex) Get(ctx context.Context, key base.Key, entry *Entry) error {
	value, err := idx.db.Get(key.Raw())
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	entry.Key = key.Raw()
	decodeValue(value, entry)
	return nil
}

// Delete removes an entry (writes tombstone in Bitcask)
func (idx *BitcaskIndex) Delete(ctx context.Context, key base.Key) error {
	return idx.db.Delete(key.Raw())
}

// Close closes the index
func (idx *BitcaskIndex) Close() error {
	return idx.db.Close()
}

// TotalSizeOnDisk returns the total size of all entries
func (idx *BitcaskIndex) TotalSizeOnDisk(ctx context.Context) (int64, error) {
	var total int64
	err := idx.db.Scan(nil, func(key bitcask.Key) error {
		value, err := idx.db.Get(key)
		if err != nil {
			return err
		}
		var entry Entry
		decodeValue(value, &entry)
		total += int64(entry.Size)
		return nil
	})
	return total, err
}

// GetAllKeys returns all keys for bloom filter reconstruction
func (idx *BitcaskIndex) GetAllKeys(ctx context.Context) ([][]byte, error) {
	var keys [][]byte
	err := idx.db.Scan(nil, func(key bitcask.Key) error {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keys = append(keys, keyCopy)
		return nil
	})
	return keys, err
}

// entryWithTime holds entry metadata for sorting
type entryWithTime struct {
	key   []byte
	size  int
	ctime int64
}

// GetOldestEntries returns iterator over N oldest entries by ctime for eviction
func (idx *BitcaskIndex) GetOldestEntries(ctx context.Context, limit int) EntryIteratorInterface {
	// Bitcask doesn't support ordered iteration, so scan all and sort in memory
	var entries []entryWithTime

	err := idx.db.Scan(nil, func(key bitcask.Key) error {
		value, err := idx.db.Get(key)
		if err != nil {
			return err
		}
		var entry Entry
		decodeValue(value, &entry)
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		entries = append(entries, entryWithTime{key: keyCopy, size: entry.Size, ctime: entry.CTime})
		return nil
	})
	if err != nil {
		return &BitcaskEntryIterator{err: err}
	}

	// Sort by ctime ascending (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ctime < entries[j].ctime
	})

	// Limit results
	if len(entries) > limit {
		entries = entries[:limit]
	}

	// Return iterator
	return &BitcaskEntryIterator{entries: entries, idx: -1}
}

// BitcaskEntryIterator implements iteration over sorted in-memory entries
type BitcaskEntryIterator struct {
	entries []entryWithTime
	idx     int
	err     error
}

func (it *BitcaskEntryIterator) Next() bool {
	it.idx++
	return it.idx < len(it.entries)
}

func (it *BitcaskEntryIterator) Entry() (Entry, error) {
	if it.idx >= len(it.entries) {
		return Entry{}, fmt.Errorf("iterator exhausted")
	}
	e := it.entries[it.idx]
	return Entry{
		Key:   e.key,
		Size:  e.size,
		CTime: e.ctime,
	}, nil
}

func (it *BitcaskEntryIterator) Err() error {
	return it.err
}

func (it *BitcaskEntryIterator) Close() error {
	return nil
}

// PutBatch inserts multiple records
func (idx *BitcaskIndex) PutBatch(ctx context.Context, records []Record) error {
	for _, rec := range records {
		value := encodeValue(rec.SegmentID, rec.Pos, rec.Size, rec.CTime)
		if err := idx.db.Put(rec.Key.Raw(), value); err != nil {
			return err
		}
	}
	return nil
}
