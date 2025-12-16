package index

import (
	"errors"
	"time"

	"github.com/miretskiy/blobcache/metadata"
)

// Key represents the index key.
type Key uint64

// Value represents a cached blob's metadata
type Value struct {
	Pos      int64 // Position within segment (0 for per-blob mode)
	Size     int64
	Checksum uint64 // CRC32 checksum of the blob data; metadata.InvalidChecksum if not set.

	// Information associated with the segment this value/blob belongs to.
	CTime     time.Time // Creation time
	SegmentID int64     // Segment ID (0 for per-blob mode)
}

// KeyValue represents key and value.
type KeyValue struct {
	Key Key
	Val Value
}

// KeyValueFn is the callback for various index scan operations.
type KeyValueFn func(KeyValue) bool

// ScanSegmentFn is the callback for scanning segment records.
type ScanSegmentFn func(metadata.SegmentRecord) bool

//// Indexer defines the index operations
//type Indexer interface {
//	Put(ctx context.Context, key Key, val Value) error
//	Get(ctx context.Context, key Key, val *Value) error
//	Delete(ctx context.Context, key Key) error
//	Close() error
//	PutBatch(ctx context.Context, kvs []KeyValue) error
//	Range(ctx context.Context, start, end Key, fn KeyValueFn) error
//}

// Common errors
var (
	ErrNotFound = errors.New("key not found")
)
