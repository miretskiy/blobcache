package index

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"go.mills.io/bitcask/v2"
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

// Flags for Value encoding
const (
	flagHasChecksum byte = 1 << 0
)

// encodeValue encodes Value fields into bytes
// Format: segmentID(8) + pos(8) + size(4) + ctime(8) + flags(1) + [checksum(4) if flagHasChecksum]
func encodeValue(record Value) []byte {
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

// decodeValue decodes bytes into Value
func decodeValue(buf []byte, val *Value) {
	val.SegmentID = int64(binary.LittleEndian.Uint64(buf[0:8]))
	val.Pos = int64(binary.LittleEndian.Uint64(buf[8:16]))
	val.Size = int(binary.LittleEndian.Uint32(buf[16:20]))
	val.CTime = int64(binary.LittleEndian.Uint64(buf[20:28]))

	// Backwards compatibility: old format was 28 bytes without flags
	if len(buf) >= 29 {
		flags := buf[28]
		val.HasChecksum = (flags & flagHasChecksum) != 0

		if val.HasChecksum && len(buf) >= 33 {
			val.Checksum = binary.LittleEndian.Uint32(buf[29:33])
		}
	} else {
		// Old format - no checksum
		val.HasChecksum = false
		val.Checksum = 0
	}
}

// Put inserts or updates a record
func (idx *BitcaskIndex) Put(ctx context.Context, key Key, val Value) error {
	value := encodeValue(val)
	return idx.db.Put(bitcask.Key(key), value)
}

// Get retrieves an entry (caller provides Value to avoid allocation)
func (idx *BitcaskIndex) Get(ctx context.Context, key Key, val *Value) error {
	value, err := idx.db.Get(bitcask.Key(key))
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	decodeValue(value, val)
	return nil
}

// Delete removes an entry (writes tombstone in Bitcask)
func (idx *BitcaskIndex) Delete(ctx context.Context, key Key) error {
	return idx.db.Delete(bitcask.Key(key))
}

// Close closes the index
func (idx *BitcaskIndex) Close() error {
	return idx.db.Close()
}

// Range iterates over all records in the index
func (idx *BitcaskIndex) Range(ctx context.Context, fn func(KeyValue) bool) error {
	return idx.db.ForEach(func(k bitcask.Key) error {
		v, err := idx.db.Get(k)
		if err != nil {
			return err
		}
		var value Value
		decodeValue(v, &value)
		if !fn(KeyValue{Key: Key(k), Val: value}) {
			return bitcask.ErrStopIteration
		}
		return nil
	})
}

// PutBatch inserts multiple records
func (idx *BitcaskIndex) PutBatch(ctx context.Context, records []KeyValue) error {
	for _, rec := range records {
		value := encodeValue(rec.Val)
		if err := idx.db.Put(bitcask.Key(rec.Key), value); err != nil {
			return err
		}
	}
	return nil
}
