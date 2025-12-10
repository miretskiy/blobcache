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

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
)

// Key is a cache key providing type safety (strong type, not alias)
type Key []byte

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	cfg             config
	index           index.Indexer
	blobReader      BlobReader
	bloom           atomic.Pointer[bloom.Filter]
	memTable        *MemTable
	storageStrategy StorageStrategy

	// Size tracking for reactive eviction
	approxSize      atomic.Int64 // Approximate total size (updated during flush/eviction)
	evictionRunning atomic.Bool  // Prevents concurrent evictions

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

	// Open index (always DurableSkipmap: skipmap backed by Bitcask)
	idx, err := index.NewDurableSkipmapIndex(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	// Create blob reader based on config (readers hold index reference)
	var blobReader BlobReader
	if cfg.SegmentSize > 0 {
		blobReader = NewSegmentReader(path, idx, cfg.Resilience.ChecksumHash, cfg.Resilience.VerifyOnRead)
	} else {
		blobReader = NewBufferedReader(path, cfg.Shards, idx, cfg.Resilience.ChecksumHash, cfg.Resilience.VerifyOnRead)
	}

	// Create storage strategy for eviction
	var strategy StorageStrategy
	if cfg.SegmentSize > 0 {
		strategy = NewSegmentStorage(path, idx)
	} else {
		strategy = NewBlobStorage(path, cfg.Shards, idx)
	}

	c := &Cache{
		cfg:             cfg,
		index:           idx,
		blobReader:      blobReader,
		storageStrategy: strategy,
		stopCh:          make(chan struct{}),
	}

	// Load or rebuild bloom filter
	if err := c.loadBloom(context.Background()); err != nil {
		idx.Close()
		return nil, fmt.Errorf("failed to load bloom: %w", err)
	}

	// Initialize approximate size for reactive eviction
	var totalSize int64
	idx.Range(context.Background(), func(rec index.Record) error {
		totalSize += int64(rec.Size)
		return nil
	})
	c.approxSize.Store(totalSize)

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
	// Create new bloom filter
	filter := bloom.New(uint(c.cfg.BloomEstimatedKeys), c.cfg.BloomFPRate)

	// Arrange for the current bloom filter to capture any additions
	// while we work on rebuilding (only if bloom already exists)
	var stopRecording func()
	var consumeRecording func(bloom.HashConsumer)
	if oldFilter := c.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
		defer stopRecording()
	}

	// Range index and add all keys to bloom
	if err := c.index.Range(ctx, func(rec index.Record) error {
		filter.Add(rec.Key)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to scan index: %w", err)
	}

	// Replay recorded additions (if any)
	if consumeRecording != nil {
		consumeRecording(filter.AddHash)
	}

	c.bloom.Store(filter)

	// Defensive: catch any additions between consume and swap
	if consumeRecording != nil {
		consumeRecording(filter.AddHash)
	}

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

	// Prevent concurrent evictions (multiple flush workers could trigger)
	if !c.evictionRunning.CompareAndSwap(false, true) {
		return nil // Another eviction already running
	}
	defer c.evictionRunning.Store(false)

	// Compute total size by scanning index
	var totalSize int64
	c.index.Range(ctx, func(rec index.Record) error {
		totalSize += int64(rec.Size)
		return nil
	})

	if totalSize <= c.cfg.MaxSize {
		return nil // Under limit
	}

	// Calculate how much to remove with hysteresis
	toEvictBytes := totalSize - c.cfg.MaxSize
	toEvictBytes += int64(float64(c.cfg.MaxSize) * c.cfg.EvictionHysteresis)

	// Delegate to storage strategy
	evictedBytes, err := c.storageStrategy.Evict(ctx, toEvictBytes)
	if err != nil {
		return err
	}

	// Update approximate size tracking
	c.approxSize.Add(-evictedBytes)

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
			_ = c.rebuildBloom(context.Background())
			_ = c.saveBloom()
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
func (c *Cache) Put(key Key, value []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.memTable.Put(keyCopy, valueCopy)
}

// UnsafePut stores key-value without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePut(key Key, value []byte) {
	c.memTable.Put(key, value)
}

// PutChecksummed stores key-value with an explicit checksum (makes copies for safety)
// The checksum will be validated if WithVerifyOnRead is enabled
func (c *Cache) PutChecksummed(key Key, value []byte, checksum uint32) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.memTable.PutChecksummed(keyCopy, valueCopy, checksum)
}

// UnsafePutChecksummed stores key-value with checksum without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePutChecksummed(key Key, value []byte, checksum uint32) {
	c.memTable.PutChecksummed(key, value, checksum)
}

// Get retrieves a value by key as a Reader
// Returns (reader, true) if found, (nil, false) if not found
// Checksums are verified internally if WithVerifyOnRead is enabled
func (c *Cache) Get(key Key) (io.Reader, bool) {
	// 1. Check bloom filter (lock-free)
	bloom := c.bloom.Load()
	if !bloom.Test(key) {
		return nil, false
	}

	// 2. Check memtable (recent writes)
	if value, found := c.memTable.Get(key); found {
		return bytes.NewReader(value), true
	}

	// 3. Read from disk using BlobReader
	// Reader handles index lookup and checksum verification internally
	return c.blobReader.Get(key)
}
