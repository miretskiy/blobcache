package index

import (
	"context"
	"errors"
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
type Indexer interface {
	Put(ctx context.Context, key []byte, record *Record) error
	Get(ctx context.Context, key []byte, record *Record) error
	Delete(ctx context.Context, key []byte) error
	Close() error
	PutBatch(ctx context.Context, records []Record) error
	Range(ctx context.Context, fn func(Record) error) error
}

// Common errors
var (
	ErrNotFound = errors.New("key not found")
)
