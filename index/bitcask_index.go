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

// Flags for Record encoding
const (
	flagHasChecksum byte = 1 << 0
)

// encodeValue encodes Record fields into bytes
// Format: segmentID(8) + pos(8) + size(4) + ctime(8) + flags(1) + [checksum(4) if flagHasChecksum]
func encodeValue(record *Record) []byte {
	baseSize := 29 // 8+8+4+8+1 bytes
	var buf []byte

	if record.HasChecksum {
		buf = make([]byte, baseSize+4) // Add 4 bytes for checksum
		binary.LittleEndian.PutUint32(buf[29:33], record.Checksum)
		buf[28] = flagHasChecksum
	} else {
		buf = make([]byte, baseSize)
		buf[28] = 0 // No flags
	}

	binary.LittleEndian.PutUint64(buf[0:8], uint64(record.SegmentID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(record.Pos))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(record.Size))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(record.CTime))

	return buf
}

// decodeValue decodes bytes into Record
func decodeValue(buf []byte, record *Record) {
	record.SegmentID = int64(binary.LittleEndian.Uint64(buf[0:8]))
	record.Pos = int64(binary.LittleEndian.Uint64(buf[8:16]))
	record.Size = int(binary.LittleEndian.Uint32(buf[16:20]))
	record.CTime = int64(binary.LittleEndian.Uint64(buf[20:28]))

	// Backwards compatibility: old format was 28 bytes without flags
	if len(buf) >= 29 {
		flags := buf[28]
		record.HasChecksum = (flags & flagHasChecksum) != 0

		if record.HasChecksum && len(buf) >= 33 {
			record.Checksum = binary.LittleEndian.Uint32(buf[29:33])
		}
	} else {
		// Old format - no checksum
		record.HasChecksum = false
		record.Checksum = 0
	}
}

// Put inserts or updates a record
func (idx *BitcaskIndex) Put(ctx context.Context, key base.Key, record *Record) error {
	// Set record key if not already set
	if record.Key == nil {
		record.Key = key.Raw()
	}
	value := encodeValue(record)
	return idx.db.Put(key.Raw(), value)
}

// Get retrieves an entry (caller provides Record to avoid allocation)
func (idx *BitcaskIndex) Get(ctx context.Context, key base.Key, entry *Record) error {
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
		var entry Record
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

// GetOldestRecords returns iterator over N oldest records by ctime for eviction
func (idx *BitcaskIndex) GetOldestRecords(ctx context.Context, limit int) RecordIterator {
	// Bitcask doesn't support ordered iteration, so scan all and sort in memory
	var entries []Record

	err := idx.db.Scan(nil, func(key bitcask.Key) error {
		value, err := idx.db.Get(key)
		if err != nil {
			return err
		}
		var entry Record
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		entry.Key = keyCopy
		decodeValue(value, &entry)
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return &BitcaskRecordIterator{err: err}
	}

	// Sort by ctime ascending (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CTime < entries[j].CTime
	})

	// Limit results
	if len(entries) > limit {
		entries = entries[:limit]
	}

	// Return iterator
	return &BitcaskRecordIterator{entries: entries, idx: -1}
}

// BitcaskRecordIterator implements iteration over sorted in-memory records
type BitcaskRecordIterator struct {
	entries []Record
	idx     int
	err     error
}

func (it *BitcaskRecordIterator) Next() bool {
	it.idx++
	return it.idx < len(it.entries)
}

func (it *BitcaskRecordIterator) Record() (Record, error) {
	if it.idx >= len(it.entries) {
		return Record{}, fmt.Errorf("iterator exhausted")
	}
	return it.entries[it.idx], nil
}

func (it *BitcaskRecordIterator) Err() error {
	return it.err
}

func (it *BitcaskRecordIterator) Close() error {
	return nil
}

// PutBatch inserts multiple records
func (idx *BitcaskIndex) PutBatch(ctx context.Context, records []Record) error {
	for _, rec := range records {
		value := encodeValue(&rec)
		if err := idx.db.Put(rec.Key, value); err != nil {
			return err
		}
	}
	return nil
}
