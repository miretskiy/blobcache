package blobcache

import (
	"testing"
)

func TestNewKey(t *testing.T) {
	key := NewKey([]byte("test-key"), 256)

	if string(key.Raw()) != "test-key" {
		t.Errorf("Raw() = %s, want test-key", key.Raw())
	}

	if key.ShardID() < 0 || key.ShardID() >= 256 {
		t.Errorf("ShardID() = %d, want 0-255", key.ShardID())
	}

	if key.FileID() == 0 {
		t.Error("FileID() = 0, want non-zero hash")
	}

	if key.Hash() == 0 {
		t.Error("Hash() = 0, want non-zero")
	}

	// FileID should equal hash
	if key.FileID() != key.Hash() {
		t.Errorf("FileID() = %d, Hash() = %d, want equal", key.FileID(), key.Hash())
	}
}

func TestKey_Deterministic(t *testing.T) {
	// Same key should produce same hash/shard/fileID
	key1 := NewKey([]byte("my-key"), 256)
	key2 := NewKey([]byte("my-key"), 256)

	if key1.Hash() != key2.Hash() {
		t.Error("Hash not deterministic")
	}

	if key1.ShardID() != key2.ShardID() {
		t.Error("ShardID not deterministic")
	}

	if key1.FileID() != key2.FileID() {
		t.Error("FileID not deterministic")
	}
}

func TestKey_ShardDistribution(t *testing.T) {
	// Test that keys distribute across shards
	numShards := 256
	shardCounts := make(map[int]int)

	for i := 0; i < 10000; i++ {
		key := NewKey([]byte(string(rune(i))), numShards)
		shardCounts[key.ShardID()]++
	}

	// Each shard should have roughly 10000/256 = 39 keys
	// Check for reasonable distribution (at least 20, at most 60)
	for shard := 0; shard < numShards; shard++ {
		count := shardCounts[shard]
		if count < 20 || count > 60 {
			t.Errorf("Shard %d has %d keys, want 20-60 (poor distribution)", shard, count)
		}
	}
}

func TestKey_FileIDUnique(t *testing.T) {
	// Different keys should have different file IDs (with extremely high probability)
	seen := make(map[uint64]bool)

	for i := 0; i < 10000; i++ {
		key := NewKey([]byte(string(rune(i))), 256)
		if seen[key.FileID()] {
			t.Errorf("FileID collision at i=%d", i)
		}
		seen[key.FileID()] = true
	}
}
