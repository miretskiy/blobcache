package index

import (
	"context"
	"errors"
)

// Key represents the index key.
type Key []byte

// Value represents a cached blob's metadata
type Value struct {
	SegmentID   int64 // Segment ID (0 for per-blob mode)
	Pos         int64 // Position within segment (0 for per-blob mode)
	Size        int
	CTime       int64  // Creation time
	Checksum    uint32 // CRC32 checksum of the blob data
	HasChecksum bool   // True if checksum field contains valid data
}

// KeyValue represents key and value.
type KeyValue struct {
	Key Key
	Val Value
}

// Indexer defines the index operations
type Indexer interface {
	Put(ctx context.Context, key Key, val Value) error
	Get(ctx context.Context, key Key, val *Value) error
	Delete(ctx context.Context, key Key) error
	Close() error
	PutBatch(ctx context.Context, kvs []KeyValue) error
	Range(ctx context.Context, fn func(KeyValue) bool) error
}

// Common errors
var (
	ErrNotFound = errors.New("key not found")
)
