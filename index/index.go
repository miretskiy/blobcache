package index

import (
	"errors"
	"sync/atomic"

	"github.com/miretskiy/blobcache/metadata"
)

// Key represents the index key.
type Key uint64

// Value represents a cached blob's metadata
type Value struct {
	Pos       int64  // Byte offset within segment file
	Size      int64  // Blob size in bytes
	Checksum  uint64 // CRC32 checksum of the blob data; metadata.InvalidChecksum if not set.
	SegmentID int64  // Unique segment file identifier

	// Sieve eviction algorithm fields
	visited atomic.Bool // visited bit for Sieve (atomic for lock-free Get path)
	next    *Value      // next in FIFO queue (newer)
	prev    *Value      // Previous in FIFO queue (older)
}

// MarkVisited sets the visited bit (for cache hit tracking)
func (v *Value) MarkVisited(visited bool) {
	v.visited.Store(visited)
}

// KeyValue represents key and value.
type KeyValue struct {
	Key Key
	Val *Value
}

// KeyValueFn is the callback for various index scan operations.
type KeyValueFn func(KeyValue) bool

// ScanSegmentFn is the callback for scanning segment records.
type ScanSegmentFn func(metadata.SegmentRecord) bool

// SortOrder specifies iteration order for ForEachBlobSorted
type SortOrder int

const (
	SortByInsertionOrder     SortOrder = iota // Oldest to newest
	SortByInsertionOrderDesc                  // Newest to oldest
)

var (
	ErrNotFound = errors.New("key not found")
)
