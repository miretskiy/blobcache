package base

import (
	"github.com/cespare/xxhash/v2"
)

// Key represents a cache key with precomputed hash, shard ID, and file ID
type Key struct {
	raw     []byte
	hash    uint64
	shardID int
	fileID  uint64
}

// NewKey creates a Key with precomputed values from hash
func NewKey(raw []byte, numShards int) Key {
	hash := xxhash.Sum64(raw)

	return Key{
		raw:     raw,
		hash:    hash,
		shardID: int(hash % uint64(numShards)),
		fileID:  hash,
	}
}

// Raw returns the raw key bytes
func (k Key) Raw() []byte {
	return k.raw
}

// Hash returns the full hash value
func (k Key) Hash() uint64 {
	return k.hash
}

// ShardID returns the precomputed shard ID
func (k Key) ShardID() int {
	return k.shardID
}

// FileID returns the deterministic file ID
func (k Key) FileID() uint64 {
	return k.fileID
}

// String returns the key as a string
func (k Key) String() string {
	return string(k.raw)
}
