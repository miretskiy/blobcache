package blobcache

import (
	"fmt"

	"github.com/miretskiy/blobcache/internal/index"
)

// RecoverIndex opens a cache from disk, recovering from missing/corrupt .meta files
// by scanning segment files directly when needed.
//
// This is the disaster recovery entry point - it opens the cache without WAL,
// allowing inspection and data recovery. For normal operation, use New().
//
// The recovery process is automatic and resilient:
//   - If .meta file exists and is valid, use it (fast path)
//   - If .meta file is missing or corrupt, scan the .seg file record-by-record
//   - Rebuild .meta from scanned records for future fast startup
//   - Clean up orphan .meta files (where .seg is missing)
func RecoverIndex(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	log.Info("starting cache recovery", "path", path)

	shards := max(1, cfg.Shards)

	// OpenIndex handles all recovery automatically via scanAll
	idx, err := index.OpenIndex(path, shards, 100000, ReadSST)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	c := &Cache{
		config:    cfg,
		index:     idx,
		archivist: NewArchivist(cfg, idx, cfg.IOScheduler),
		segIDs:    newSegmentIDProvider(cfg.Path, cfg.Shards),
		stopCh:    make(chan struct{}),
	}
	c.librarian = NewLibrarian(cfg.MaxCachedSlabs, c)
	c.Knobs = cfg.knobs
	c.memTable = NewMemTable(c.config, c, c, c.librarian, nil, c.segIDs) // No WAL during recovery
	c.memTable.Knobs = c.Knobs

	// Build Bloom Filter synchronously
	log.Info("rebuilding bloom filter...")
	if err := c.rebuildBloom(); err != nil {
		return nil, fmt.Errorf("failed to build bloom filter: %w", err)
	}

	// Set starting size by scanning the index
	var totalSize int64
	var itemCount int
	c.index.ForEachBlob(func(e index.Item) bool {
		totalSize += int64(e.PhysicalLen)
		itemCount++
		return true
	})
	c.approxSize.Store(totalSize)

	log.Info("recovery complete", "items", itemCount, "total_mb", totalSize/(1024*1024))
	return c, nil
}
