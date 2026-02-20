package blobcache

// TestingKnobs provides hooks for testing error paths and injecting behavior.
// Pass via WithTestingKnobs option or set directly on MemTable.Knobs in tests.
type TestingKnobs struct {
	// InjectWriteErr is called during flush before writing to segment.
	InjectWriteErr func() error

	// InjectIndexErr is called during flush after writing but before index update.
	InjectIndexErr func() error

	// InjectEvictErr is called during eviction before deleting segment file.
	InjectEvictErr func() error

	// SequenceVendor overrides the default sequence ID generation.
	// If set, NextSeq() calls are delegated to this interface.
	SequenceVendor SequenceVendor

	// OnReadCacheMiss is called when the read cache misses for a key.
	OnReadCacheMiss func(hashKey Key)

	// OnReadCacheHit is called when the read cache hits for a key.
	OnReadCacheHit func(hashKey Key)

	// OnReadCacheEvict is called when a read cache slab is evicted.
	OnReadCacheEvict func(totalItems int32, visitedCount int32)
}
