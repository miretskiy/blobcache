package blobcache

import (
	"github.com/cespare/xxhash/v2"
)

// Key represents a cache key with precomputed shard ID
type Key struct {
	raw     []byte
	shardID int
}

// NewKey creates a Key with precomputed shard ID
func NewKey(raw []byte, numShards int) Key {
	shardID := int(xxhash.Sum64(raw) % uint64(numShards))
	return Key{
		raw:     raw,
		shardID: shardID,
	}
}

// Raw returns the raw key bytes
func (k Key) Raw() []byte {
	return k.raw
}

// ShardID returns the precomputed shard ID
func (k Key) ShardID() int {
	return k.shardID
}

// String returns the key as a string
func (k Key) String() string {
	return string(k.raw)
}
