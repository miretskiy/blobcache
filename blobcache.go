package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

type Key = uint64

const (
	// evictionHysteresis is the target fraction of MaxSize to evict to.
	evictionHysteresis = 0.93
)

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	config
	index   *index.Index
	storage *Storage
	bloom   struct {
		atomic.Pointer[bloom.Filter]
		hits        atomic.Uint64             // Bloom filter said "yes"
		ghosts      atomic.Uint64             // Bloom said yes, but index said no.
		deletions   atomic.Int64              // Track cumulative deletions since last rebuild
		lastRebuild atomic.Pointer[time.Time] // When the last rebuild happened.
	}
	
	// --- ARCHITECTURE COMPONENTS ---
	memTable  *MemTable  // The Write Engine (Producer)
	librarian *Librarian // The Read Cache (Consumer)
	
	// LogicalSize tracking for reactive eviction
	approxSize      atomic.Int64 // Approximate total size (updated during flush/eviction)
	evictionRunning atomic.Bool  // Prevents concurrent evictions
	
	// Background error tracking
	bgError atomic.Pointer[error] // First background error (nil = healthy)
	
	// Background workers
	evictionTrigger chan struct{} // Capacity 1: trigger eviction, blocks when eviction running
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// ErrorRporter interface allows memtable to check/set degraded state
// without direct dependency on Cache
type ErrorRporter interface {
	IsDegraded() bool
	ReportError(error)
}

func (c *Cache) IsDegraded() bool {
	return c.bgError.Load() != nil
}

func (c *Cache) ReportError(err error) {
	if c.bgError.CompareAndSwap(nil, &err) {
		log.Error("entering degraded mode (memory-only)", "error", err)
	}
}

// BGError returns any background error (nil if healthy)
func (c *Cache) BGError() error {
	if ptr := c.bgError.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// New creates a Cache at the specified path with optional configuration
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
	
	// Create new bloom filter and figure out how much data on disk from segment meta.
	var totalSize int64
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	if err := idx.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			if !rec.IsDeleted() {
				filter.Add(rec.Hash)
				totalSize += rec.LogicalSize
			}
		}
		return true
	}); err != nil {
		return nil, err
	}
	
	c := &Cache{
		config:          cfg,
		index:           idx,
		librarian:       NewLibrarian(cfg.MaxCachedSlabs),
		storage:         NewStorage(cfg, idx),
		evictionTrigger: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
	c.bloom.Store(filter)
	c.approxSize.Store(totalSize)
	c.memTable = NewMemTable(c.config, c, c, c.librarian)
	
	return c, nil
}

// Start begins background operations (eviction worker)
func (c *Cache) Start() {
	c.wg.Add(1)
	go c.evictionWorker()
}

// Close gracefully shuts down all background workers and saves state
func (c *Cache) Close() error {
	// Signal workers to stop (idempotent)
	select {
	case <-c.stopCh:
		return nil
	default:
		close(c.stopCh)
	}
	
	// 1. Close Write Path (Stops new slabs)
	c.memTable.Close()
	
	// 2. Close Read Path (Releases pinned memory)
	c.librarian.Close()
	
	c.wg.Wait()
	
	// Collect all close errors
	return errors.Join(
		c.storage.Close(),
		c.index.Close(),
	)
}

// Drain waits for all pending memtable writes to complete
func (c *Cache) Drain() {
	c.memTable.Drain()
}

// checkOrInitialize ensures directory structure exists and validates configuration
func checkOrInitialize(cfg config) (*index.Index, error) {
	markerPath := filepath.Join(cfg.Path, ".initialized")
	
	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		return index.NewIndex(cfg.Path)
	}
	
	// Not initialized - create directory structure
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

// --- UNIFIED LOOKUP PIPELINE ---

// search attempts to locate the blob in RAM (returning data) or Disk (returning r).
// It acts as the Single Source of Truth for Bloom metrics (Hits vs Ghosts).
func (c *Cache) search(key []byte) (data []byte, r io.Reader, rel Releaser, ok bool) {
	h := c.KeyHasher(key)

	// 1. Bloom Filter Gate
	if !c.bloom.Load().Test(h) {
		return nil, nil, Releaser{}, false
	}
	c.bloom.hits.Add(1) // Bloom said "yes"

	// 2. RAM Hit (Librarian)
	if ramData, releaser, found := c.librarian.Acquire(h); found {
		return ramData, nil, releaser, true
	}

	// 3. Disk Hit (Storage)
	entry, found := c.index.Get(h)
	if !found {
		// BLOOM GHOST: Bloom said yes, Index said no.
		c.bloom.ghosts.Add(1)
		return nil, nil, Releaser{}, false
	}

	diskReader, err := c.storage.ReadBlob(entry)
	if err != nil {
		c.handleStorageError(h, entry, err)
		return nil, nil, Releaser{}, false
	}

	return nil, diskReader, Releaser{}, true
}

// ZeroCopyView provides a unified reader for both RAM and Disk hits.
// If found is true, the caller MUST call the returned Releaser expediently.
func (c *Cache) ZeroCopyView(key []byte) (io.Reader, Releaser, bool) {
	data, r, rel, ok := c.search(key)
	if !ok {
		return nil, Releaser{}, false
	}
	
	if data != nil {
		// RAM Hit: Wrap raw bytes in a reader.
		return bytes.NewReader(data), rel, true
	}
	
	// Disk Hit: Reader is already set.
	return r, rel, true
}

func (c *Cache) Append(key []byte, dst []byte) ([]byte, bool) {
	data, r, rel, ok := c.search(key)
	if !ok {
		return dst, false
	}
	defer rel.Release()
	
	if data != nil {
		// Fast Path: Direct append (Zero Alloc)
		return append(dst, data...), true
	}
	
	// Slow Path: Disk Reader
	buf := bytes.NewBuffer(dst)
	buf.Reset()
	if _, err := io.Copy(buf, r); err != nil {
		return dst, false
	}
	return buf.Bytes(), true
}

// View provides scoped access to a value via an io.Reader.
func (c *Cache) View(key []byte, fn func(r io.Reader)) bool {
	data, r, rel, ok := c.search(key)
	if !ok {
		return false
	}
	defer rel.Release()
	
	if data != nil {
		fn(bytes.NewReader(data))
	} else {
		fn(r)
	}
	return true
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
	return c.Append(key, nil)
}

func (c *Cache) Put(key []byte, value []byte) {
	h := c.KeyHasher(key)
	c.memTable.Put(h, value)
	c.bloom.Load().Add(h)
}

func (c *Cache) PutChecksummed(key []byte, value []byte, checksum uint32) {
	h := c.KeyHasher(key)
	c.memTable.PutChecksummed(h, value, checksum)
	c.bloom.Load().Add(h)
}

// --- BATCH & MAINTENANCE ---

type Batcher interface {
	PutBatch(segID int64, records []metadata.BlobRecord) error
}

func (c *Cache) PutBatch(segID int64, records []metadata.BlobRecord) error {
	// Phase 1: Ingest into Index
	if err := c.index.IngestBatch(segID, records); err != nil {
		return err
	}
	
	// Phase 2: Update size tracking
	var addedBytes int64
	for _, rec := range records {
		addedBytes += rec.LogicalSize
	}
	newSize := c.approxSize.Add(addedBytes)
	
	// Phase 3: Trigger eviction if over limit
	if c.MaxSize > 0 && newSize > c.MaxSize && !c.IsDegraded() {
		c.triggerEviction()
	}
	return nil
}

func (c *Cache) triggerEviction() {
	select {
	case c.evictionTrigger <- struct{}{}:
	default:
	}
}

func (c *Cache) handleStorageError(h Key, e index.Entry, err error) {
	if IsTransientIOError(err) {
		log.Error("transient storage error (skipping)", "hash", h, "error", err)
		return
	}
	
	log.Warn("permanent storage failure: removing stale index entry", "hash", h, "error", err)
	
	err = c.index.DeleteBlobs(e)
	if err == nil {
		c.approxSize.Add(-e.LogicalSize)
		// Update Bloom metrics: This key is now a "Ghost"
		c.bloom.ghosts.Add(1)
		c.bloom.deletions.Add(1)
	} else {
		log.Warn("index update failure", "hash", h, "error", err)
	}
}

func (c *Cache) rebuildBloom() error {
	newFilter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)
	
	var stopRecording func()
	var consumeRecording func(bloom.HashConsumer)
	
	if oldFilter := c.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
	}
	
	err := c.index.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			if !rec.IsDeleted() {
				newFilter.AddHash(rec.Hash)
			}
		}
		return true
	})
	if err != nil {
		if stopRecording != nil {
			stopRecording()
		}
		return err
	}
	
	oldFilter := c.bloom.Swap(newFilter)
	
	if oldFilter != nil && stopRecording != nil {
		stopRecording()
		consumeRecording(newFilter.AddHash)
	}
	
	c.bloom.deletions.Store(0)
	now := time.Now()
	c.bloom.lastRebuild.Store(&now)
	
	return nil
}

func (c *Cache) maybeTriggerBloomRebuild() error {
	// 1. Cooldown Guard (e.g., 5 minutes)
	last := c.bloom.lastRebuild.Load()
	if last != nil && time.Since(*last) < 5*time.Minute {
		return nil
	}
	
	shouldRebuild := false
	
	// 2. Proactive: Cumulative Staleness check
	staleCount := c.bloom.deletions.Load()
	threshold := int64(float64(c.BloomEstimatedKeys) * 0.10)
	if staleCount > threshold {
		shouldRebuild = true
	}
	
	// 3. Reactive: Observed FPR check
	if !shouldRebuild {
		hits := c.bloom.hits.Load()
		ghosts := c.bloom.ghosts.Load()
		if hits > 2000 {
			observedFPR := float64(ghosts) / float64(hits)
			if observedFPR > (c.config.BloomFPRate * 5.0) {
				shouldRebuild = true
			}
		}
	}
	
	if shouldRebuild {
		return c.rebuildBloom()
	}
	return nil
}

// evictionWorker handles eviction requests and periodic compaction
func (c *Cache) evictionWorker() {
	defer c.wg.Done()
	
	compactionTicker := time.NewTicker(10 * time.Minute)
	defer compactionTicker.Stop()
	
	for {
		select {
		case <-c.evictionTrigger:
			// Eviction requested (triggered by PutBatch)
			if c.MaxSize > 0 && !c.IsDegraded() {
				if err := c.runEvictionSieve(c.MaxSize); err != nil {
					c.ReportError(err)
					return // Stop worker permanently
				}
			}
		
		case <-compactionTicker.C:
			// Periodic compaction
			if !c.IsDegraded() {
				if err := c.maybeCompactSegments(); err != nil {
					log.Warn("compaction failed", "error", err)
				}
			}
		
		case <-c.stopCh:
			return
		}
	}
}

// runEvictionSieve evicts blobs using Sieve algorithm until under size limit
func (c *Cache) runEvictionSieve(maxCacheSize int64) error {
	evictionStart := time.Now()
	
	if c.testingInjectEvictErr != nil {
		if err := c.testingInjectEvictErr(); err != nil {
			return err
		}
	}
	if !c.evictionRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer c.evictionRunning.Store(false)
	
	currentSize := c.approxSize.Load()
	if currentSize <= maxCacheSize {
		return nil
	}
	
	target := int64(float64(maxCacheSize) * evictionHysteresis)
	toEvictBytes := currentSize - target
	
	var (
		victims             []index.Entry
		evictedBytes        int64
		physicallyReclaimed int64
	)
	
	// 1. SELECTION PHASE
	for evictedBytes < toEvictBytes {
		victim, err := c.index.Evict()
		if err != nil {
			break
		}
		victims = append(victims, victim)
		evictedBytes += victim.LogicalSize
	}
	
	if len(victims) == 0 {
		return nil
	}
	
	// 2. COMMIT PHASE
	if err := c.index.DeleteBlobs(victims...); err != nil {
		return fmt.Errorf("eviction durability sync failed: %w", err)
	}
	
	// 3. RECLAMATION PHASE
	for _, v := range victims {
		reclaimed, _ := c.storage.HolePunchBlob(v.SegmentID, v.Pos, v.LogicalSize)
		physicallyReclaimed += reclaimed
	}
	
	// 4. METRICS & MAINTENANCE
	c.approxSize.Add(-evictedBytes)
	evictedCount := len(victims)
	
	log.Info("eviction completed",
		"duration", time.Since(evictionStart),
		"evicted_count", evictedCount,
		"evicted_mb", evictedBytes/(1024*1024),
		"reclaimed_mb", physicallyReclaimed/(1024*1024),
		"reclaim_pct", 100*float64(physicallyReclaimed)/float64(evictedBytes),
		"remaining_mb", c.approxSize.Load()/(1024*1024))
	
	c.bloom.deletions.Add(int64(evictedCount))
	
	if err := c.maybeTriggerBloomRebuild(); err != nil {
		log.Error("bloom rebuild failed", "error", err)
	}
	
	return nil
}

func (c *Cache) maybeCompactSegments() error {
	// Placeholder
	return nil
}
