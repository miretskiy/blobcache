package blobcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
)

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	cfg        config
	index      index.Indexer
	blobReader BlobReader
	bloom      atomic.Pointer[bloom.Filter]
	memTable   *MemTable

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

	// Open index (DuckDB, Bitcask, or Skipmap based on config)
	idx, err := func() (index.Indexer, error) {
		if cfg.UseSkipmapIndex {
			// Durable skipmap backed by Bitcask for segments
			if cfg.SegmentSize > 0 {
				return index.NewDurableSkipmapIndex(path)
			}
			// Pure in-memory for per-blob mode
			return index.NewSkipmapIndex(), nil
		}
		if cfg.UseBitcaskIndex {
			return index.NewBitcaskIndex(path)
		}
		// DuckDB index doesn't support segments yet
		if cfg.SegmentSize > 0 {
			panic("Segment mode requires Bitcask or Skipmap index")
		}
		return index.NewDuckDBIndex(path)
	}()
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	// Create blob reader based on config
	var blobReader BlobReader
	if cfg.SegmentSize > 0 {
		blobReader = NewSegmentReader(path)
	} else {
		blobReader = NewBufferedReader(path, cfg.Shards)
	}

	c := &Cache{
		cfg:        cfg,
		index:      idx,
		blobReader: blobReader,
		stopCh:     make(chan struct{}),
	}

	// Load or rebuild bloom filter
	if err := c.loadBloom(context.Background()); err != nil {
		idx.Close()
		return nil, fmt.Errorf("failed to load bloom: %w", err)
	}

	c.memTable = c.newMemTable()

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
	c.memTable.Close()

	// Wait for all workers to finish
	c.wg.Wait()

	// Save bloom filter before closing
	if err := c.saveBloom(); err != nil {
		// Log warning but don't fail Close()
		fmt.Printf("Warning: failed to save bloom filter: %v\n", err)
	}

	// Close blob reader (releases cached file handles)
	if c.blobReader != nil {
		c.blobReader.Close()
	}

	// Close index
	if c.index != nil {
		return c.index.Close()
	}

	return nil
}

// Drain waits for all pending memtable writes to complete
// No-op if memtable is disabled
func (c *Cache) Drain() {
	c.memTable.Drain()
}

// checkOrInitialize ensures directory structure exists and validates configuration
// Uses empty .initialized marker file for fast idempotency check
func checkOrInitialize(cfg config) error {
	markerPath := filepath.Join(cfg.Path, ".initialized")

	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		// Already initialized
		return nil
	}

	// Not initialized - create directory structure

	// Create base directories (both blobs and segments, use based on config)
	dirs := []string{
		filepath.Join(cfg.Path, "db"),
		filepath.Join(cfg.Path, "blobs"),
		filepath.Join(cfg.Path, "segments"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create shard directories for blobs
	for i := 0; i < cfg.Shards; i++ {
		shardDir := filepath.Join(cfg.Path, "blobs", fmt.Sprintf("shard-%03d", i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return fmt.Errorf("failed to create shard-%03d: %w", i, err)
		}
	}

	// Touch empty marker file
	if err := os.WriteFile(markerPath, []byte{}, 0o644); err != nil {
		return fmt.Errorf("failed to write marker: %w", err)
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

	// Put all keys
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
	return os.WriteFile(bloomPath, data, 0o644)
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

			// Compute file path from key
			k := base.NewKey(entry.Key, c.cfg.Shards)
			shardDir := fmt.Sprintf("shard-%03d", k.ShardID())
			blobFile := fmt.Sprintf("%d.blob", k.FileID())
			blobPath := filepath.Join(c.cfg.Path, "blobs", shardDir, blobFile)

			// Delete blob file
			if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
				fmt.Printf("Warning: failed to delete blob %s: %v\n", blobPath, err)
			}

			// Delete from index
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

// Put stores a key-value pair (makes copies for safety)
func (c *Cache) Put(key, value []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.memTable.Put(keyCopy, valueCopy)
}

// UnsafePut stores key-value without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePut(key, value []byte) {
	c.memTable.Put(key, value)
}

// Get retrieves a value by key as a Reader
// Returns (reader, true) if found, (nil, false) if not found
// For file-backed readers, caller should close when done
func (c *Cache) Get(key []byte) (io.Reader, bool) {
	// 1. Check bloom filter (lock-free, <1 ns)
	bloom := c.bloom.Load()
	if !bloom.Test(key) {
		return nil, false
	}

	// 2. Check memtable (recent writes)
	if value, found := c.memTable.Get(key); found {
		return bytes.NewReader(value), true
	}

	// 3. Read from disk using BlobReader
	k := base.NewKey(key, c.cfg.Shards)

	// For segment mode, need index lookup
	var entry index.Entry
	if c.cfg.SegmentSize > 0 {
		if err := c.index.Get(context.Background(), k, &entry); err != nil {
			return nil, false
		}
	}

	return c.blobReader.Get(k, &entry)
}
