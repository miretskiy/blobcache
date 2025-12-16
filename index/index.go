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
	Pos      int64  // Byte offset within segment file
	Size     int64  // Blob size in bytes
	Checksum uint64 // CRC32 checksum of the blob data; metadata.InvalidChecksum if not set.

	// Segment metadata
	ctime     time.Time // Creation time (unexported - use CTime() accessor or TestingSetCTime())
	SegmentID int64     // Unique segment file identifier
}

// CTime returns the creation time of this value
func (v Value) CTime() time.Time {
	return v.ctime
}

// TestingSetCTime sets the creation time (for testing only)
func (v *Value) TestingSetCTime(t time.Time) {
	v.ctime = t
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
