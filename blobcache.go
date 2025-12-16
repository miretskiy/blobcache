package blobcache

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

// Key is a cache key providing type safety (strong type, not alias)
type Key = index.Key

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	config
	index    *index.Index
	storage  *Storage
	bloom    atomic.Pointer[bloom.Filter]
	memTable *MemTable

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
	idx, err := checkOrInitialize(cfg)
	if err != nil {
		return nil, fmt.Errorf("initialization failed: %w", err)
	}

	// Create new bloom filter and figure out how much data on disk from segment records.
	var totalSize int64
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	if err := idx.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			filter.Add(rec.Hash)
			totalSize += rec.Size
		}
		return true
	}); err != nil {
		return nil, err
	}

	c := &Cache{
		config:  cfg,
		index:   idx,
		storage: NewStorage(cfg, idx),
		stopCh:  make(chan struct{}),
	}
	c.bloom.Store(filter)
	c.approxSize.Store(totalSize)
	c.memTable = c.newMemTable(c.config, c.storage)

	return c, nil
}

// Start begins background operations (eviction, bloom refresh, orphan cleanup)
// Returns a closer function for graceful shutdown
func (c *Cache) Start() (func(), error) {
	// Start eviction worker (checks every 60 seconds)
	c.wg.Add(1)
	go c.evictionWorker()

	// Return closer function
	return c.Close, nil
}

// Close gracefully shuts down all background workers and saves state
// Safe to call multiple times and safe to call even if Start() was never called
func (c *Cache) Close() {
	// Signal workers to stop (idempotent)
	select {
	case <-c.stopCh:
		return
	default:
		close(c.stopCh)
	}

	// Close memtable (does NOT drain - caller should call Drain() if needed)
	c.memTable.Close()
	c.wg.Wait()
	c.storage.Close()
	c.index.Close()
}

// Drain waits for all pending memtable writes to complete
// No-op if memtable is disabled
func (c *Cache) Drain() {
	c.memTable.Drain()
}

// checkOrInitialize ensures directory structure exists and validates configuration
// Uses empty .initialized marker file for fast idempotency check
func checkOrInitialize(cfg config) (*index.Index, error) {
	markerPath := filepath.Join(cfg.Path, ".initialized")

	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		// Already initialized
		return index.NewIndex(cfg.Path)
	}

	// Not initialized - create directory structure
	// Create shard directories for blobs
	for i := 0; i < max(1, cfg.Shards); i++ {
		shardDir := filepath.Join(cfg.Path, "segments", fmt.Sprintf("%04d", i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create %04d: %w", i, err)
		}
	}

	idx, err := index.NewIndex(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}
	// Touch empty marker file
	if err := os.WriteFile(markerPath, []byte{}, 0o644); err != nil {
		return nil, fmt.Errorf("failed to write marker: %w", err)
	}

	return idx, nil
}

// rebuildBloom scans all keys from index and builds new bloom filter
func (c *Cache) rebuildBloom() error {
	// Create new bloom filter
	filter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)

	// Arrange for the current bloom filter to capture any additions
	// while we work on rebuilding (only if bloom already exists)
	var stopRecording func()
	var consumeRecording func(bloom.HashConsumer)
	if oldFilter := c.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
		defer stopRecording()
	}

	if err := c.index.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			filter.Add(rec.Hash)
		}
		return true
	}); err != nil {
		return err
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

// Background workers

// evictionWorker checks size and evicts old files when over limit
func (c *Cache) evictionWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.MaxSize > 0 {
				c.runEviction(c.MaxSize)
			}
		case <-c.stopCh:
			return
		}
	}
}

// runEviction evicts oldest entries if cache size exceeds limit
func (c *Cache) runEviction(maxCacheSize int64) error {
	// Prevent concurrent evictions (multiple flush workers could trigger)
	if !c.evictionRunning.CompareAndSwap(false, true) {
		return nil // Another eviction already running
	}
	defer c.evictionRunning.Store(false)

	// Compute total size by scanning segment recrods.
	var totalSize int64
	var segments []metadata.SegmentRecord
	if err := c.index.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			totalSize += rec.Size
		}
		segments = append(segments, segment)
		return true
	}); err != nil {
		return err
	}

	if totalSize <= maxCacheSize {
		return nil // Under limit
	}

	// Calculate how much to remove no need for (hysteresis since we remove
	// entire segment)
	toEvictBytes := totalSize - maxCacheSize

	// Sort by ctime (oldest first)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].CTime.Before(segments[j].CTime)
	})

	// Evict oldest until target reached
	evictedBytes := int64(0)
	evictedCount := 0

	for _, segment := range segments {
		if evictedBytes >= toEvictBytes {
			break
		}

		// Delete blob file using centralized path generation
		for _, rec := range segment.Records {
			evictedBytes += rec.Size
		}
		evictedCount += len(segment.Records)

		p := getSegmentPath(c.Path, c.Shards, segment.SegmentID)
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: failed to remove segment %s: %v\n", p, err)
		}
		if err := c.index.DeleteSegment(segment); err != nil {
			return err
		}

		if c.onSegmentEvicted != nil {
			c.onSegmentEvicted(segment.SegmentID)
		}
	}

	fmt.Printf("Evicted %d blobs (%d MB)\n", evictedCount, evictedBytes/(1024*1024))

	// Rebuild bloom filter after eviction to remove stale entries
	if evictedCount > 0 {
		if err := c.rebuildBloom(); err != nil {
			return fmt.Errorf("failed to rebuild bloom after eviction: %w", err)
		}
	}

	return nil
}

// Put stores a key-value pair (makes copies for safety)
func (c *Cache) Put(key []byte, value []byte) {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.UnsafePut(key, valueCopy)
}

// UnsafePut stores key-value without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePut(key []byte, value []byte) {
	h := c.KeyHasher(key)
	c.memTable.Put(Key(h), value)
	c.bloom.Load().Add(h)
}

// PutChecksummed stores key-value with an explicit checksum (makes copies for safety)
// The checksum will be validated if WithVerifyOnRead is enabled
func (c *Cache) PutChecksummed(key []byte, value []byte, checksum uint32) {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.UnsafePutChecksummed(key, valueCopy, checksum)
}

// UnsafePutChecksummed stores key-value with checksum without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePutChecksummed(key []byte, value []byte, checksum uint32) {
	h := c.KeyHasher(key)
	c.memTable.PutChecksummed(Key(h), value, checksum)
	c.bloom.Load().Add(h)
}

type batcher interface {
	PutBatch([]index.KeyValue) error
}

func (c *Cache) PutBatch(kvs []index.KeyValue) error {
	if err := c.index.PutBatch(kvs); err != nil {
		return err
	}

	// Phase 3: Update size tracking and trigger reactive eviction if needed
	var addedBytes int64
	for _, rec := range kvs {
		addedBytes += rec.Val.Size
	}
	newSize := c.approxSize.Add(addedBytes)

	// Trigger eviction if over limit (reactive)
	if c.MaxSize > 0 && newSize > c.MaxSize {
		if err := c.runEviction(c.MaxSize); err != nil {
			fmt.Printf("Warning: reactive eviction failed: %v\n", err)
		}
	}
	return nil
}

// Get retrieves a value by key as a Reader
// Returns (reader, true) if found, (nil, false) if not found
// IncludeChecksums are verified internally if WithVerifyOnRead is enabled
func (c *Cache) Get(key []byte) (io.Reader, bool) {
	// 1. Check bloom filter (lock-free)
	h := c.KeyHasher(key)
	bloom := c.bloom.Load()
	if !bloom.Test(h) {
		return nil, false
	}

	// 2. Check memtable (recent writes)
	if value, found := c.memTable.Get(Key(h)); found {
		return bytes.NewReader(value), true
	}

	// 3. Read from disk using BlobReader
	// Reader handles index lookup and checksum verification internally
	return c.storage.Get(Key(h))
}
