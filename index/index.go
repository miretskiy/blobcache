package index

import (
	"context"
	"errors"

	"github.com/miretskiy/blobcache/base"
)

// Record represents a cached blob's metadata
type Record struct {
	Key         []byte // Raw key bytes
	SegmentID   int64  // Segment ID (0 for per-blob mode)
	Pos         int64  // Position within segment (0 for per-blob mode)
	Size        int
	CTime       int64  // Creation time
	Checksum    uint32 // CRC32 checksum of the blob data
	HasChecksum bool   // True if checksum field contains valid data
}

// Indexer defines the index operations
// TODO: Cleanup context usage - currently inconsistent (some methods use it, memtable doesn't)
type Indexer interface {
	Put(ctx context.Context, key base.Key, record *Record) error
	Get(ctx context.Context, key base.Key, record *Record) error
	Delete(ctx context.Context, key base.Key) error
	Close() error
	PutBatch(ctx context.Context, records []Record) error
	Scan(ctx context.Context, fn func(Record) error) error
}

// KeyValue holds a key with metadata for bulk insert
type KeyValue struct {
	Key  base.Key
	Size int
}

// Common errors
var (
	ErrNotFound = errors.New("key not found")
)

// RecordIterator provides iteration over records
type RecordIterator interface {
	Next() bool
	Record() (Record, error)
	Err() error
	Close() error
}
