package blobcache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
)

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	cfg      config
	index    *index.Index
	bloom    atomic.Pointer[bloom.Filter]
	memTable *MemTable // Optional async write buffer

	// Background workers
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New creates a Cache at the specified path with optional configuration
// Does NOT start background workers - call Start() to begin operations
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	// Ensure directory structure exists and validate configuration
	if err := checkOrInitialize(cfg); err != nil {
		return nil, fmt.Errorf("initialization failed: %w", err)
	}

	// Open DuckDB index
	idx, err := index.New(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	c := &Cache{
		cfg:    cfg,
		index:  idx,
		stopCh: make(chan struct{}),
	}

	// Load or rebuild bloom filter
	if err := c.loadBloom(context.Background()); err != nil {
		idx.Close()
		return nil, fmt.Errorf("failed to load bloom: %w", err)
	}

	// Create memtable if enabled
	c.memTable = c.newMemTable(cfg.MemTableCapacity)

	return c, nil
}

// Start begins background operations (eviction, bloom refresh, orphan cleanup)
// Returns a closer function for graceful shutdown
func (c *Cache) Start() (func() error, error) {
	// Start eviction worker (checks every 60 seconds)
	c.wg.Add(1)
	go c.evictionWorker()

	// Start bloom refresh worker (rebuilds periodically)
	c.wg.Add(1)
	go c.bloomRefreshWorker()

	// Start orphan cleanup worker (if enabled)
	if c.cfg.OrphanCleanupInterval > 0 {
		c.wg.Add(1)
		go c.orphanCleanupWorker()
	}

	// Return closer function
	return c.Close, nil
}

// Close gracefully shuts down all background workers and saves state
// Safe to call multiple times and safe to call even if Start() was never called
func (c *Cache) Close() error {
	// Signal workers to stop (idempotent)
	select {
	case <-c.stopCh:
		return nil // Already closed
	default:
		close(c.stopCh)
	}

	// Close memtable (does NOT drain - caller should call Drain() if needed)
	if c.memTable != nil {
		c.memTable.Close()
	}

	// Wait for all workers to finish
	c.wg.Wait()

	// Save bloom filter before closing
	if err := c.saveBloom(); err != nil {
		// Log warning but don't fail Close()
		fmt.Printf("Warning: failed to save bloom filter: %v\n", err)
	}

	// Close index (DuckDB)
	if c.index != nil {
		return c.index.Close()
	}

	return nil
}

// Drain waits for all pending memtable writes to complete
// No-op if memtable is disabled
func (c *Cache) Drain() {
	if c.memTable != nil {
		c.memTable.Drain()
	}
}

// checkOrInitialize ensures directory structure exists and validates configuration
// Uses empty .initialized marker file for fast idempotency check
// Verifies shard count by counting shard directories
func checkOrInitialize(cfg config) error {
	markerPath := filepath.Join(cfg.Path, ".initialized")

	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		// Initialized - verify shard count matches
		return verifyShards(cfg)
	}

	// Not initialized - create directory structure

	// Create base directories
	dirs := []string{
		filepath.Join(cfg.Path, "db"),
		filepath.Join(cfg.Path, "blobs"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create shard directories
	for i := 0; i < cfg.Shards; i++ {
		shardDir := filepath.Join(cfg.Path, "blobs", fmt.Sprintf("shard-%03d", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return fmt.Errorf("failed to create shard-%03d: %w", i, err)
		}
	}

	// Touch empty marker file
	if err := os.WriteFile(markerPath, []byte{}, 0644); err != nil {
		return fmt.Errorf("failed to write marker: %w", err)
	}

	return nil
}

// verifyShards checks that expected shard directories exist
// Warns on mismatch but doesn't fail (cache can still function, just inefficiently)
func verifyShards(cfg config) error {
	blobsDir := filepath.Join(cfg.Path, "blobs")

	// Check each expected shard directory exists
	missing := 0
	for i := 0; i < cfg.Shards; i++ {
		shardDir := filepath.Join(blobsDir, fmt.Sprintf("shard-%03d", i))
		if _, err := os.Stat(shardDir); os.IsNotExist(err) {
			missing++
		}
	}

	if missing > 0 {
		fmt.Printf("WARNING: %d shard directories missing (config wants %d). Cache may be inefficient.\n",
			missing, cfg.Shards)
		// Don't fail - cache can still operate
	}

	// Check for extra shards (wrong config)
	entries, _ := os.ReadDir(blobsDir)
	extra := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "shard-") {
			extra++
		}
	}

	if extra > cfg.Shards {
		fmt.Printf("WARNING: Found %d shard directories, config expects %d. Extra shards will not be used.\n",
			extra, cfg.Shards)
	}

	return nil
}

// loadBloom loads bloom filter from storage or rebuilds from index
func (c *Cache) loadBloom(ctx context.Context) error {
	// Try to load from file first
	bloomPath := filepath.Join(c.cfg.Path, "bloom.dat")
	data, err := os.ReadFile(bloomPath)

	if err == nil {
		// Load existing bloom filter
		filter, err := bloom.Deserialize(data)
		if err == nil {
			c.bloom.Store(filter)
			return nil
		}
		// Failed to deserialize - rebuild below
	}

	// Bloom missing or corrupt - rebuild from index
	return c.rebuildBloom(ctx)
}

// rebuildBloom scans all keys from index and builds new bloom filter
func (c *Cache) rebuildBloom(ctx context.Context) error {
	// Get all keys from index
	keys, err := c.index.GetAllKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to get keys from index: %w", err)
	}

	// Create new bloom filter with estimated size
	estimatedKeys := len(keys)
	if estimatedKeys < c.cfg.BloomEstimatedKeys {
		estimatedKeys = c.cfg.BloomEstimatedKeys
	}

	filter := bloom.New(uint(estimatedKeys), c.cfg.BloomFPRate)

	// Add all keys
	for _, key := range keys {
		filter.Add(key)
	}

	c.bloom.Store(filter)
	return nil
}

// saveBloom persists bloom filter to disk
func (c *Cache) saveBloom() error {
	filter := c.bloom.Load()
	if filter == nil {
		return nil
	}

	data, err := filter.Serialize()
	if err != nil {
		return err
	}

	bloomPath := filepath.Join(c.cfg.Path, "bloom.dat")
	return os.WriteFile(bloomPath, data, 0644)
}

// Background workers

// evictionWorker checks size and evicts old files when over limit
func (c *Cache) evictionWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.runEviction(context.Background())
		case <-c.stopCh:
			return
		}
	}
}

// runEviction evicts oldest entries if cache size exceeds limit
func (c *Cache) runEviction(ctx context.Context) error {
	// Check if eviction needed
	if c.cfg.MaxSize <= 0 {
		return nil // Eviction disabled
	}

	totalSize, err := c.index.TotalSizeOnDisk(ctx)
	if err != nil {
		return fmt.Errorf("failed to get total size: %w", err)
	}

	if totalSize <= c.cfg.MaxSize {
		return nil // Under limit
	}

	// Need to evict - calculate how much to remove with hysteresis
	// Hysteresis prevents thrashing at the boundary
	toEvictBytes := totalSize - c.cfg.MaxSize
	toEvictBytes += int64(float64(c.cfg.MaxSize) * c.cfg.EvictionHysteresis)

	evictedBytes := int64(0)
	evictedCount := 0

	// Evict in batches until target reached
	batchSize := 1000
	for evictedBytes < toEvictBytes {
		// Get next batch of oldest entries
		it := c.index.GetOldestEntries(ctx, batchSize)
		if it.Err() != nil {
			return fmt.Errorf("failed to get oldest entries: %w", it.Err())
		}

		batchEvicted := 0
		for it.Next() && evictedBytes < toEvictBytes {
			entry, err := it.Entry()
			if err != nil {
				it.Close()
				return fmt.Errorf("failed to get entry: %w", err)
			}

			// Delete blob file
			blobPath := filepath.Join(c.cfg.Path, "blobs",
				fmt.Sprintf("shard-%03d", entry.ShardID),
				fmt.Sprintf("%d.blob", entry.FileID))
			os.Remove(blobPath) // Best effort - ignore errors

			// Delete from index
			k := base.NewKey(entry.Key, c.cfg.Shards)
			if err := c.index.Delete(ctx, k); err != nil {
				// Log warning but continue
				fmt.Printf("Warning: failed to delete key from index: %v\n", err)
			}

			evictedBytes += int64(entry.Size)
			evictedCount++
			batchEvicted++
		}

		if err := it.Err(); err != nil {
			it.Close()
			return fmt.Errorf("iteration error: %w", err)
		}
		it.Close()

		// If batch was empty, we're done (no more entries)
		if batchEvicted == 0 {
			break
		}
	}

	fmt.Printf("Evicted %d entries (%d MB)\n", evictedCount, evictedBytes/(1024*1024))
	return nil
}

// bloomRefreshWorker periodically rebuilds bloom filter from index
func (c *Cache) bloomRefreshWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cfg.BloomRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.rebuildBloom(context.Background())
			c.saveBloom()
		case <-c.stopCh:
			return
		}
	}
}

// orphanCleanupWorker removes orphaned blob files
func (c *Cache) orphanCleanupWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cfg.OrphanCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// TODO: Implement orphan cleanup
		case <-c.stopCh:
			return
		}
	}
}

// Put stores a key-value pair
// If memtable enabled, queues for async write; otherwise writes synchronously
func (c *Cache) Put(ctx context.Context, key, value []byte) error {
	if c.memTable != nil {
		return c.memTable.Add(key, value)
	}
	return c.writeToDisk(ctx, key, value)
}

// writeToDisk writes key-value pair directly to index and blob file
func (c *Cache) writeToDisk(ctx context.Context, key, value []byte) error {
	k := base.NewKey(key, c.cfg.Shards)

	// TODO: Add checksum if cfg.Checksums enabled

	// 1. Update index first (if this fails, nothing written)
	now := time.Now().UnixNano()
	if err := c.index.Put(ctx, k, len(value), now, now); err != nil {
		return fmt.Errorf("index put failed: %w", err)
	}

	// 2. Update bloom filter (fast, lock-free)
	bloom := c.bloom.Load()
	bloom.Add(key)

	// 3. Write blob file (if this fails, orphan in index gets cleaned up eventually)
	blobPath := filepath.Join(c.cfg.Path, "blobs",
		fmt.Sprintf("shard-%03d", k.ShardID()),
		fmt.Sprintf("%d.blob", k.FileID()))

	if err := os.WriteFile(blobPath, value, 0644); err != nil {
		// File write failed but index updated - acceptable for cache
		return fmt.Errorf("blob write failed: %w", err)
	}

	return nil
}

// Get retrieves a value by key
func (c *Cache) Get(ctx context.Context, key []byte) ([]byte, error) {
	// 1. Check bloom filter (lock-free, <1 ns)
	bloom := c.bloom.Load()
	if !bloom.Test(key) {
		return nil, ErrNotFound
	}

	// 2. Read blob file directly (no index lookup needed!)
	k := base.NewKey(key, c.cfg.Shards)
	blobPath := filepath.Join(c.cfg.Path, "blobs",
		fmt.Sprintf("shard-%03d", k.ShardID()),
		fmt.Sprintf("%d.blob", k.FileID()))

	data, err := os.ReadFile(blobPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound // Bloom FP or file evicted
		}
		return nil, err
	}

	// TODO: Verify checksum if cfg.VerifyOnRead enabled

	return data, nil
}
