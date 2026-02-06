package blobcache

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/wal"
	"github.com/zeebo/xxh3"
	"golang.org/x/time/rate"
)

// Key is the 128-bit hash of a blob key.
// Lo is used for bloom filter; full 128-bit for index to avoid collisions.
type Key = xxh3.Uint128

const (
	// evictionHysteresis is the target fraction of MaxSize to evict to.
	evictionHysteresis = 0.93
)

// segmentDeleteStats groups per-segment delete counts for batch metadata updates.
// Used during eviction to aggregate deletes before calling UpdateSegmentOnDelete.
type segmentDeleteStats struct {
	count int32
	bytes int64
}

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	config
	index     *index.DurableIndex
	archivist *Archivist
	wal       *wal.WAL // nil if WAL disabled
	segIDs    SegmentIDProvider
	hot       struct {
		bloom     atomic.Pointer[bloom.Filter]
		trustHash bool
		_         [64]byte // force next member to start on another cache line.
	}

	bloomStats struct {
		hits        atomic.Uint64             // Bloom filter said "yes"
		ghosts      atomic.Uint64             // Bloom said yes, but index said no.
		deletions   atomic.Int64              // Track cumulative deletions since last rebuild
		lastRebuild atomic.Pointer[time.Time] // When the last rebuild happened.
	}

	// --- ARCHITECTURE COMPONENTS ---
	memTable          *MemTable     // The Write Engine (Producer)
	librarian         *Librarian    // The Read Cache (Consumer)
	compactor         *Compactor    // Segment merge compaction
	compactionLimiter *rate.Limiter // Token bucket for compaction I/O throttling

	// Global monotonic sequence counter for operation ordering.
	// Initialized to time.Now().UnixNano() for continuity across restarts.
	// This ensures sequences are always increasing even after crashes,
	// without needing to scan WAL/segments for the last sequence.
	globalSeq atomic.Uint64

	// LogicalSize tracking for reactive eviction
	approxSize      atomic.Int64 // Approximate total size (updated during flush/eviction)
	evictionRunning atomic.Bool  // Prevents concurrent evictions

	// Oldest segment tracking for tombstone GC
	// Tombstones can only be safely dropped when compacting the tail (oldest) segment.
	// See CLAUDE.md "Deletion Model (Tombstones)" for rationale.
	oldestLiveSegmentID atomic.Uint32

	// Background error tracking
	bgError atomic.Pointer[error] // First background error (nil = healthy)

	// Background workers
	maintenanceTrigger chan struct{} // Capacity 1: trigger eviction + compaction cycle
	stopCh             chan struct{}
	wg                 sync.WaitGroup

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &TestingKnobs{...}
	Knobs *TestingKnobs

	// reclaimBuf is reused across eviction cycles to avoid heap allocations.
	// Holds coalesced hole ranges after merging adjacent victims.
	reclaimBuf []HoleRange

	// segmentDeleteBuf is reused across eviction cycles to group deletes by segment.
	// Cleared and reused to avoid map allocation every eviction cycle.
	segmentDeleteBuf map[uint32]segmentDeleteStats

	// ballast is a heap allocation that reduces GC frequency by keeping
	// the heap larger. Never accessed after initialization.
	ballast []byte
}

// ErrorReporter interface allows memtable to check/set degraded state
// without direct dependency on Cache
type ErrorReporter interface {
	IsDegraded() bool
	ReportError(error)
	ReportBlobError(key Key, errno base.BlobErrno)
}

// SequenceVendor provides monotonic sequence IDs for write ordering.
// Implement this interface to override the default sequence generation in tests.
type SequenceVendor interface {
	NextSeq() uint64
}

// SegmentIDProvider allocates unique segment IDs.
// Both MemTable (normal writes) and Compaction use this to avoid conflicts.
type SegmentIDProvider interface {
	NextSegmentID() uint32
	CurrentSegmentID() uint32
}

func (c *Cache) IsDegraded() bool {
	return c.bgError.Load() != nil
}

// IsTailSegment returns true if segID is the oldest live segment.
// Tombstones can only be safely dropped when compacting the tail segment;
// otherwise the Leapfrog Hazard can resurrect deleted keys.
func (c *Cache) IsTailSegment(segID uint32) bool {
	return segID == c.oldestLiveSegmentID.Load()
}

// OldestLiveSegmentID returns the oldest segment ID still in use.
// Returns 0 if no segments exist.
func (c *Cache) OldestLiveSegmentID() uint32 {
	return c.oldestLiveSegmentID.Load()
}

func (c *Cache) ReportError(err error) {
	if c.bgError.CompareAndSwap(nil, &err) {
		if c.DegradedMode == DegradedPanic {
			panic(fmt.Sprintf("blobcache: degraded mode triggered: %v\n\nStack trace:\n%s", err, debug.Stack()))
		}
		log.Error("entering degraded mode (memory-only)", "error", err)
	}
}

func (c *Cache) ReportBlobError(key Key, errno base.BlobErrno) {
	if errno == base.ErrNone {
		return
	}
	log.Warn("blob error reported", "key", key, "errno", errno)
	c.index.SetBlobErrno(key, errno)
}

// nextSeq atomically increments and returns the next sequence ID.
// This is THE source of truth for operation ordering.
func (c *Cache) nextSeq() uint64 {
	if c.Knobs != nil && c.Knobs.SequenceVendor != nil {
		return c.Knobs.SequenceVendor.NextSeq()
	}
	return c.globalSeq.Add(1)
}

// BGError returns any background error (nil if healthy)
func (c *Cache) BGError() error {
	if ptr := c.bgError.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// New creates a Cache at the specified path with optional configuration.
// Uses "crash-only" initialization: if WAL recovery is needed, we recover,
// flush to segments, close, and re-open cleanly. This ensures the cache
// always starts in a consistent state without "mixed mode" initialization.
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	// Iterative open with hard limit of 2 attempts:
	// - Attempt 1: If WAL exists, recover it, flush to segments, close, restart
	// - Attempt 2: Should be clean (no WAL). If WAL still exists, fail.
	const maxAttempts = 2
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c, recovered, err := open(cfg)
		if err != nil {
			return nil, err
		}

		if !recovered {
			// Clean open - no WAL recovery needed
			return c, nil
		}

		// WAL recovery happened - close and restart fresh
		log.Info("WAL recovery completed, restarting for clean state", "attempt", attempt)
		if closeErr := c.Close(); closeErr != nil {
			return nil, fmt.Errorf("close after recovery: %w", closeErr)
		}

		// Safety check: on second attempt, WAL should be gone
		if attempt == maxAttempts-1 {
			// Next iteration is the last - if we still have WAL files, that's a bug
			continue
		}
	}

	return nil, fmt.Errorf("WAL files still present after %d recovery attempts (bug?)", maxAttempts)
}

// open is the internal initialization function.
// Returns (cache, recovered, error) where recovered=true means WAL recovery happened.
func open(cfg config) (*Cache, bool, error) {
	// Ensure directory structure exists and validate configuration
	idx, err := checkOrInitialize(cfg)
	if err != nil {
		return nil, false, fmt.Errorf("initialization failed: %w", err)
	}

	// Create new bloom filter and figure out how much data on disk from segment meta.
	// Also track oldest segment ID for tombstone GC.
	var totalSize int64
	var oldestSegID uint32
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	if err := idx.ForEachSegment(func(m index.DurableBatch) bool {
		// Track oldest segment
		if oldestSegID == 0 || m.SegmentID < oldestSegID {
			oldestSegID = m.SegmentID
		}
		for _, item := range m.Items {
			if !item.IsDeleted() {
				filter.Add(item.Key)                 // Full 128-bit key
				totalSize += int64(item.PhysicalLen) // Track on-disk size
			}
		}
		return true
	}); err != nil {
		return nil, false, err
	}

	c := &Cache{
		config:             cfg,
		index:              idx,
		archivist:          NewArchivist(cfg, idx),
		segIDs:             newSegmentIDProvider(cfg.Path, cfg.Shards),
		maintenanceTrigger: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
	}

	// Initialize WAL if enabled
	if cfg.WAL.Enabled {
		w, walErr := c.initWAL()
		if walErr != nil {
			return nil, false, fmt.Errorf("WAL initialization failed: %w", walErr)
		}
		c.wal = w
	}

	// Initialize sequence counter with current nanosecond timestamp.
	// This guarantees monotonicity across process restarts:
	// - UnixNano gives ~292 years before overflow
	// - Even if we restart 1 second later, new sequences are guaranteed higher
	// - Clock skew is acceptable (we just need monotonicity within a run)
	c.globalSeq.Store(uint64(time.Now().UnixNano()))

	c.librarian = NewLibrarian(cfg.MaxCachedSlabs, c)
	c.hot.bloom.Store(filter)
	c.hot.trustHash = c.TrustHash
	c.approxSize.Store(totalSize)
	c.oldestLiveSegmentID.Store(oldestSegID)
	c.Knobs = cfg.knobs
	c.memTable = NewMemTable(c.config, c, c, c.librarian, c.wal, c.segIDs)
	c.memTable.Knobs = c.Knobs

	// Initialize compactor for segment merge operations
	ioFlags := sys.SyncNone
	if cfg.IO.DirectIOWrite {
		ioFlags |= sys.FlDirectIO
	}
	if cfg.IO.FDataSync {
		ioFlags |= sys.SyncData
	}
	c.compactor = NewCompactor(idx, c.segIDs, cfg.Path, cfg.Shards, ioFlags, int(cfg.WriteBufferSize), c.archivist.DropSegmentCache)

	// Initialize compaction rate limiter for I/O throttling.
	// Token bucket: refills at CompactionBandwidth bytes/sec, burst = 1 segment worth.
	if cfg.CompactionBandwidth > 0 {
		// Burst allows writing one full segment without waiting
		burst := int(cfg.WriteBufferSize * 2) // 2x buffer for safety
		if burst < 1 {
			burst = 1
		}
		c.compactionLimiter = rate.NewLimiter(rate.Limit(cfg.CompactionBandwidth), burst)
	}

	// Run WAL recovery after memtable is initialized
	var recovered bool
	if c.wal != nil {
		var recoveryErr error
		recovered, recoveryErr = c.runWALRecovery()
		if recoveryErr != nil {
			// Close what we've opened so far
			c.wal.Close()
			return nil, false, fmt.Errorf("WAL recovery failed: %w", recoveryErr)
		}
	}

	// Allocate heap ballast to reduce GC frequency (default: 1GB).
	if cfg.BallastSize > 0 {
		c.ballast = make([]byte, cfg.BallastSize)
	}

	return c, recovered, nil
}

// Start begins background operations (maintenance worker for eviction + compaction).
func (c *Cache) Start() {
	c.wg.Add(1)
	go c.maintenanceWorker()
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

	// 2. Close Read Path (Releases pinned slabs back to pool)
	c.librarian.Close()

	// 3. Release pool memory (must be after librarian returns slabs)
	c.memTable.ClosePools()

	c.wg.Wait()

	// Release compactor buffer
	if c.compactor != nil {
		c.compactor.Close()
	}

	// Collect all close errors (WAL may be nil if disabled)
	var walErr error
	if c.wal != nil {
		walErr = c.wal.Close()
	}

	return errors.Join(
		walErr,
		c.archivist.Close(),
		c.index.Close(),
	)
}

// Drain waits for all pending memtable writes to complete
func (c *Cache) Drain() {
	c.memTable.Drain()
}

// checkOrInitialize ensures directory structure exists and validates configuration
func checkOrInitialize(cfg config) (*index.DurableIndex, error) {
	markerPath := filepath.Join(cfg.Path, ".initialized")

	// Use BloomEstimatedKeys as index capacity hint
	capacityHint := cfg.BloomEstimatedKeys
	if capacityHint < 1024 {
		capacityHint = 1024
	}

	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		return index.OpenIndex(cfg.Path, cfg.Shards, capacityHint)
	}

	// Not initialized - create directory structure
	for i := 0; i < max(1, cfg.Shards); i++ {
		shardDir := filepath.Join(cfg.Path, "segments", fmt.Sprintf("%04d", i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create %04d: %w", i, err)
		}
	}

	idx, err := index.OpenIndex(cfg.Path, cfg.Shards, capacityHint)
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

// search attempts to locate the blob in RAM or Disk, returning raw bytes.
// For RAM hits, returns zero-copy slice into the slab buffer.
// For disk hits, reads the entire blob into memory.
// It acts as the Single Source of Truth for Bloom metrics (Hits vs Ghosts).
// Key verification happens on both paths to detect 128-bit hash collisions.
func (c *Cache) search(key []byte) (data []byte, rel Releaser, ok bool) {
	h := xxh3.Hash128(key)

	// 1. Bloom Filter Gate (full 128-bit key)
	if !c.hot.bloom.Load().Test(h) {
		return nil, Releaser{}, false
	}

	if h.Lo&127 == 0 {
		c.bloomStats.hits.Add(128)
	}

	// 2. RAM Hit (Librarian)
	if ramData, storedKey, releaser, found := c.librarian.Acquire(h); found {
		// Verify key to detect hash collision
		if !c.hot.trustHash && !bytes.Equal(storedKey, key) {
			releaser.Release()
			c.bloomStats.ghosts.Add(1) // Hash collision: bloom said yes, wrong key
			return nil, Releaser{}, false
		}
		return ramData, releaser, true
	}

	// 3. Disk Hit (Storage)
	entry, found := c.index.Get(h)
	if !found || entry.IsDeleted() {
		// BLOOM GHOST: Bloom said yes, Index said no (or item is deleted).
		c.bloomStats.ghosts.Add(1)
		return nil, Releaser{}, false
	}

	// 4. Check corruption flag
	if entry.HasError() {
		log.Debug("blob marked as corrupt", "hash", h, "errno", entry.Errno())
		return nil, Releaser{}, false
	}

	// 5. Read from disk (with key verification for collision detection)
	// Pass nil key when TrustHash enabled to skip verification
	verifyKey := key
	if c.TrustHash {
		verifyKey = nil
	}
	data, diskReleaser, err := c.archivist.ReadBlob(entry, verifyKey)
	if err != nil {
		c.handleStorageError(h, entry, err)
		return nil, Releaser{}, false
	}
	return data, diskReleaser, true
}

// ZeroCopyView provides a unified reader for both RAM and Disk hits.
// If found is true, the caller MUST call the returned Releaser expediently.
func (c *Cache) ZeroCopyView(key []byte) (io.Reader, Releaser, bool) {
	data, rel, ok := c.search(key)
	if !ok {
		return nil, Releaser{}, false
	}
	return bytes.NewReader(data), rel, true
}

func (c *Cache) Read(key []byte, dst []byte) ([]byte, bool) {
	data, rel, ok := c.search(key)
	if !ok {
		return dst, false
	}
	defer rel.Release()
	return append(dst, data...), true
}

// View provides scoped access to a value's raw bytes.
// The data slice is valid only for the duration of fn.
func (c *Cache) View(key []byte, fn func(data []byte)) bool {
	data, rel, ok := c.search(key)
	if !ok {
		return false
	}
	defer rel.Release()
	fn(data)
	return true
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
	return c.Read(key, nil)
}

// ErrEmptyKey is returned when Put or Delete is called with an empty key.
var ErrEmptyKey = errors.New("blobcache: empty key not allowed")

// Delete marks a blob as deleted (tombstone).
//
// Behavior depends on mode:
// - CAS Mode (WAL enabled): Writes tombstone to WAL, defers space reclamation to compaction
// - Cache Mode (no WAL): Immediately reclaims space via hole punching
//
// Returns nil if the key doesn't exist (idempotent delete).
// Returns nil if the key hash exists but stored key differs (hash collision - not our key).
func (c *Cache) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	// Verify the key exists and matches (handles hash collision detection).
	// search() checks both RAM and disk paths with key verification.
	_, rel, found := c.search(key)
	if !found {
		return nil // Not found or hash collision - idempotent
	}
	rel.Release()

	h := xxh3.Hash128(key)

	// Re-lookup to get index.Item (safe - search verified the key)
	item, found := c.index.Get(h)
	if !found || item.IsDeleted() {
		// Race: item was deleted between search and here
		return nil
	}

	// Invalidate Librarian cache to prevent serving stale data
	c.librarian.Invalidate(h)

	if c.wal != nil {
		return c.deleteInCASMode(key, h, item)
	}
	return c.deleteInCacheMode(key, h, item)
}

// deleteInCASMode handles deletion in CAS mode (WAL enabled).
// Writes tombstone to WAL for durability, defers space reclamation to compaction.
//
// Uses segment lock to coordinate with compaction (prevents concurrent segment drop).
func (c *Cache) deleteInCASMode(key []byte, h Key, item index.Item) error {
	segID := item.SegmentID

	// Acquire exclusive segment lock (blocks compaction of this segment)
	shard := c.index.SegmentLockShard(segID)
	shard.Lock()
	defer shard.Unlock()

	// TODO: Check if segment still exists (compaction may have dropped it while we waited)
	// If dropped, just remove orphaned RAM reference and return

	// Write tombstone to WAL for crash consistency
	rec := record.Record{
		Header: record.Header{
			Magic:        record.RecordMagic,
			Flags:        record.FlagDeleted | record.FlagInvalidCRC,
			SeqID:        c.nextSeq(),
			KeyLen:       uint16(len(key)),
			PhysicalSize: 0,
			LogicalSize:  0,
		},
		Key:   key,
		Value: nil,
	}
	if _, err := c.wal.Write(rec); err != nil {
		return fmt.Errorf("wal write delete: %w", err)
	}

	// Write tombstone to incremental log (with user key for collision detection)
	if err := c.index.Tombstone(segID, h, key); err != nil {
		return fmt.Errorf("write tombstone: %w", err)
	}

	// Update segment metadata (for compaction selection)
	c.index.UpdateSegmentOnDelete(segID, 1, int64(item.PhysicalLen))

	c.bloomStats.deletions.Add(1)
	return nil
}

// deleteInCacheMode handles deletion in Cache mode (no WAL).
// Immediately reclaims space via hole punching.
//
// Uses segment lock to coordinate with compaction (prevents concurrent segment drop).
func (c *Cache) deleteInCacheMode(key []byte, h Key, item index.Item) error {
	segID := item.SegmentID

	// Acquire exclusive segment lock (blocks compaction of this segment)
	shard := c.index.SegmentLockShard(segID)
	shard.Lock()
	defer shard.Unlock()

	// Immediate space reclamation via hole punch
	reclaimed, err := c.archivist.HolePunchBlob(segID, item.Offset, item.PhysicalLen)
	if err != nil {
		log.Warn("hole punch failed, space not reclaimed",
			"segment", segID, "offset", item.Offset,
			"size", item.PhysicalLen, "error", err)
	} else if reclaimed > 0 {
		log.Debug("reclaimed space via hole punch",
			"segment", segID, "bytes", reclaimed)
	}

	// Write tombstone to incremental log (with user key)
	if err := c.index.Tombstone(segID, h, key); err != nil {
		return fmt.Errorf("write tombstone: %w", err)
	}

	// Update segment metadata (for compaction selection)
	c.index.UpdateSegmentOnDelete(segID, 1, int64(item.PhysicalLen))

	c.bloomStats.deletions.Add(1)
	return nil
}

func (c *Cache) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	h := xxh3.Hash128(key)
	c.hot.bloom.Load().Add(h)
	c.putWithRetry(h, key, value, nil)
	return nil
}

func (c *Cache) PutChecksummed(key []byte, value []byte, checksum uint32) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	h := xxh3.Hash128(key)
	c.hot.bloom.Load().Add(h)
	c.putWithRetry(h, key, value, &checksum)
	return nil
}

// putWithRetry handles the zombie writer resurrection protocol.
// If a write is rejected (seqID too old due to slab rotation), we check
// if a newer version already exists. If so, we return success (idempotent).
// If not, we acquire a fresh seqID and retry.
func (c *Cache) putWithRetry(h Key, keyBytes, value []byte, checksum *uint32) {
	seqID := c.nextSeq()

	for {
		var err error
		if checksum != nil {
			err = c.memTable.PutChecksummed(seqID, h, keyBytes, value, *checksum)
		} else {
			err = c.memTable.Put(seqID, h, keyBytes, value)
		}

		if err == nil {
			return
		}

		if !errors.Is(err, errSequenceTooOld) {
			c.ReportError(err)
			return
		}

		// Zombie Investigation: Check if a version exists in the global index.
		// If it does, we "succeeded" (last write wins, our data is obsolete).
		// Note: SeqID is not stored in RAM index, so we just check existence.
		if _, found := c.index.Get(h); found {
			return
		}

		// Resurrection: Acquire fresh seqID and retry
		seqID = c.nextSeq()
	}
}

type Batcher interface {
	PutBatch(segID uint32, items []index.Item, maxSeqID uint64) error
}

// maintenanceSegmentInterval determines how often segment production triggers maintenance.
// Every N segments, an eviction + compaction check is triggered.
const maintenanceSegmentInterval = 10

func (c *Cache) PutBatch(segID uint32, items []index.Item, _ uint64) error {
	// Phase 1: Ingest into Index (also registers segment snapshot for tombstone dissolution)
	c.index.AddSegment(segID, items)

	// Phase 2: Update size tracking (using PhysicalLen = on-disk size)
	var addedBytes int64
	for _, item := range items {
		addedBytes += int64(item.PhysicalLen)
	}
	newSize := c.approxSize.Add(addedBytes)

	// Phase 3: Trigger maintenance (eviction + compaction) when needed
	// Triggers when: over size limit OR every N segments for compaction
	overSizeLimit := c.MaxSize > 0 && newSize > c.MaxSize
	segmentInterval := segID%maintenanceSegmentInterval == 0

	if (overSizeLimit || segmentInterval) && !c.IsDegraded() {
		c.triggerMaintenance()
	}

	return nil
}

// triggerMaintenance signals the background worker to run eviction + compaction.
// Non-blocking: if maintenance is already pending, this is a no-op.
func (c *Cache) triggerMaintenance() {
	select {
	case c.maintenanceTrigger <- struct{}{}:
	default:
	}
}

func (c *Cache) handleStorageError(h Key, e index.Item, err error) {
	// 1. Transient errors: Skip and retry later
	if sys.IsTransientIOError(err) {
		log.Error("transient storage error (skipping)", "hash", h, "error", err)
		return
	}

	// 2. Non-transient errors: Mark blob as corrupt
	// By definition, any non-transient error is permanent.
	// We mark the blob with errno but keep metadata for observability.
	// Let normal eviction handle cleanup if needed.
	errno := base.ToErrno(err)
	log.Warn("permanent blob error detected", "hash", h, "errno", errno, "error", err)
	c.ReportBlobError(h, errno)
}

func (c *Cache) rebuildBloom() error {
	newFilter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)

	var stopRecording func()
	var consumeRecording func(bloom.KeyConsumer)

	if oldFilter := c.hot.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
	}

	err := c.index.ForEachSegment(func(m index.DurableBatch) bool {
		for _, item := range m.Items {
			if !item.IsDeleted() {
				newFilter.AddHash(item.Key)
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

	oldFilter := c.hot.bloom.Swap(newFilter)

	if oldFilter != nil && stopRecording != nil {
		stopRecording()
		consumeRecording(newFilter.AddHash)
	}

	c.bloomStats.deletions.Store(0)
	now := time.Now()
	c.bloomStats.lastRebuild.Store(&now)

	return nil
}

func (c *Cache) maybeTriggerBloomRebuild() error {
	// 1. Cooldown Guard (e.g., 5 minutes)
	last := c.bloomStats.lastRebuild.Load()
	if last != nil && time.Since(*last) < 5*time.Minute {
		return nil
	}

	shouldRebuild := false

	// 2. Proactive: Cumulative Staleness check
	staleCount := c.bloomStats.deletions.Load()
	threshold := int64(float64(c.BloomEstimatedKeys) * 0.10)
	if staleCount > threshold {
		shouldRebuild = true
	}

	// 3. Reactive: Observed FPR check
	if !shouldRebuild {
		hits := c.bloomStats.hits.Load()
		ghosts := c.bloomStats.ghosts.Load()
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

// initWAL opens the WAL for writing.
// WAL files are named by the first SeqID written to them, so no pre-initialization needed.
func (c *Cache) initWAL() (*wal.WAL, error) {
	if c.WAL.Dir == "" {
		c.WAL.Dir = filepath.Join(c.Path, "wal")
	}
	return wal.Open(c.WAL.Config)
}

// cacheReplayer implements wal.Replayer, wrapping Cache for WAL recovery.
type cacheReplayer struct {
	cache *Cache
}

func (r *cacheReplayer) ReplayRecord(rec record.Record) error {
	h := xxh3.Hash128(rec.Key)

	if rec.IsDeleted() {
		// Replay delete: look up item and mark as tombstone
		item, found := r.cache.index.Get(h)
		if found && !item.IsDeleted() {
			// Hole punch to reclaim space (log errors but don't fail recovery)
			if _, err := r.cache.archivist.HolePunchBlob(item.SegmentID, item.Offset, item.PhysicalLen); err != nil {
				log.Warn("hole punch during delete replay failed", "key", h, "error", err)
			}
			// Mark as tombstone
			if err := r.cache.index.DeleteBlobs(item); err != nil {
				return fmt.Errorf("replay delete: %w", err)
			}
			r.cache.bloomStats.deletions.Add(1)
		}
		return nil
	}

	// Update bloom filter for puts
	r.cache.hot.bloom.Load().Add(h)

	// Replay the Put directly:
	// - Use original SeqID (not new sequence)
	// - Use original CRC (already verified during WAL read)
	// - Bypass compression (value is already in final form)
	// - Write record as-is to slab
	return r.cache.memTable.ReplayRecord(h, rec)
}

func (r *cacheReplayer) Flush() {
	r.cache.memTable.Flush()
}

func (r *cacheReplayer) Drain() {
	r.cache.memTable.Drain()
}

// runWALRecovery replays uncommitted WAL entries into the memtable.
// Returns (recovered, error) where recovered=true if any WAL files were replayed.
func (c *Cache) runWALRecovery() (bool, error) {
	// Compute checkpoint: max SeqID across all committed segments.
	// Any WAL file with firstSeqID <= checkpoint has already been flushed.
	var checkpoint uint64
	if err := c.index.ForEachSegment(func(m index.DurableBatch) bool {
		if m.MaxSeqID > checkpoint {
			checkpoint = m.MaxSeqID
		}
		return true
	}); err != nil {
		return false, fmt.Errorf("scan segments: %w", err)
	}

	// isCommitted returns true if the WAL file's data is already in a segment.
	// Since each WAL file contains a contiguous range [firstSeqID, maxSeqID],
	// if firstSeqID <= checkpoint, all its data was already flushed.
	isCommitted := func(firstSeqID uint64) bool {
		return firstSeqID <= checkpoint
	}

	replayer := &cacheReplayer{cache: c}
	recovered, err := c.wal.Recover(replayer, isCommitted)
	if err != nil {
		return false, err
	}
	return recovered, nil
}

// CoalesceVictims sorts victims by (SegmentID, Offset) and merges adjacent ranges
// into a single HoleRange. This reduces filesystem journal commits by ~98% when
// evicting thousands of blobs, turning "Swiss cheese" into "stripes".
//
// The dst slice is reused to avoid heap allocations. Pass c.reclaimBuf[:0] for
// zero-allocation coalescing across eviction cycles.
//
// Algorithm:
//  1. Sort victims in-place by SegmentID, then Offset
//  2. Walk through sorted victims, merging contiguous ranges
//  3. Two blobs are contiguous if: same SegmentID AND v[i].Offset + v[i].PhysicalLen == v[i+1].Offset
func CoalesceVictims(victims []index.Item, dst []HoleRange) []HoleRange {
	if len(victims) == 0 {
		return dst[:0]
	}

	// Sort by (SegmentID, Offset) - stable sort not needed since offsets are unique
	slices.SortFunc(victims, func(a, b index.Item) int {
		if c := cmp.Compare(a.SegmentID, b.SegmentID); c != 0 {
			return c
		}
		return cmp.Compare(a.Offset, b.Offset)
	})

	// Reset destination, keeping underlying capacity
	dst = dst[:0]

	// Start with first victim as current range
	currentSeg := victims[0].SegmentID
	currentOff := int64(victims[0].Offset)
	currentLen := int64(victims[0].PhysicalLen)

	for i := 1; i < len(victims); i++ {
		v := &victims[i]
		vEnd := currentOff + currentLen

		// Check if this victim is contiguous with current range
		if v.SegmentID == currentSeg && int64(v.Offset) == vEnd {
			// Merge: extend current range
			currentLen += int64(v.PhysicalLen)
		} else {
			// Emit current range and start new one
			dst = append(dst, HoleRange{
				SegmentID: currentSeg,
				Offset:    currentOff,
				Length:    currentLen,
			})
			currentSeg = v.SegmentID
			currentOff = int64(v.Offset)
			currentLen = int64(v.PhysicalLen)
		}
	}

	// Emit final range
	dst = append(dst, HoleRange{
		SegmentID: currentSeg,
		Offset:    currentOff,
		Length:    currentLen,
	})

	return dst
}

// maintenanceWorker handles eviction and compaction in a unified event-driven loop.
//
// Lifecycle (no timers):
//   - maintenanceTrigger: Fired by PutBatch when over size limit OR every N segments
//
// Each cycle: (1) evict if over size, (2) compact sparse segments.
// This ensures "Swiss cheese" holes from eviction are promptly defragmented,
// and segment production naturally triggers merge compaction.
func (c *Cache) maintenanceWorker() {
	defer c.wg.Done()

	for {
		select {
		case <-c.maintenanceTrigger:
			// Phase 1: Eviction (if needed)
			if c.MaxSize > 0 && c.approxSize.Load() > c.MaxSize && !c.IsDegraded() {
				if err := c.runEvictionSieve(c.MaxSize); err != nil {
					c.ReportError(err)
					return // Stop worker permanently
				}
			}

			// Phase 2: Compaction (always check after eviction or segment production)
			if !c.IsDegraded() {
				if err := c.maybeCompactSegments(); err != nil {
					c.ReportError(fmt.Errorf("compaction: %w", err))
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

	if c.Knobs != nil && c.Knobs.InjectEvictErr != nil {
		if err := c.Knobs.InjectEvictErr(); err != nil {
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
		victims             []index.Item
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
		evictedBytes += int64(victim.PhysicalLen)
	}

	if len(victims) == 0 {
		return nil
	}

	// 2. COMMIT PHASE
	if err := c.index.DeleteBlobs(victims...); err != nil {
		return fmt.Errorf("eviction durability sync failed: %w", err)
	}

	// Update segment metadata for compaction selection (group by segment).
	// Reuse buffer across eviction cycles to avoid allocation storm.
	if c.segmentDeleteBuf == nil {
		c.segmentDeleteBuf = make(map[uint32]segmentDeleteStats)
	}
	for _, v := range victims {
		sd := c.segmentDeleteBuf[v.SegmentID]
		sd.count++
		sd.bytes += int64(v.PhysicalLen)
		c.segmentDeleteBuf[v.SegmentID] = sd
	}
	for segID, sd := range c.segmentDeleteBuf {
		c.index.UpdateSegmentOnDelete(segID, sd.count, sd.bytes)
		delete(c.segmentDeleteBuf, segID) // Clear for reuse
	}

	// 3. RECLAMATION PHASE
	// Coalesce adjacent holes to reduce filesystem journal commits by ~98%.
	// This turns "Swiss cheese" (thousands of tiny holes) into "stripes"
	// (few large contiguous ranges), significantly reducing metadata overhead.
	c.reclaimBuf = CoalesceVictims(victims, c.reclaimBuf[:0])
	for _, r := range c.reclaimBuf {
		reclaimed, _ := c.archivist.HolePunchRange(context.Background(), r.SegmentID, r.Offset, r.Length)
		physicallyReclaimed += reclaimed
	}

	// 4. METRICS & MAINTENANCE
	c.approxSize.Add(-evictedBytes)
	evictedCount := len(victims)
	syscallCount := len(c.reclaimBuf)

	// Calculate batching efficiency: higher = more syscalls saved
	var batchEfficiency float64
	if syscallCount > 0 {
		batchEfficiency = float64(evictedCount) / float64(syscallCount)
	}

	log.Info("eviction completed",
		"duration", time.Since(evictionStart),
		"evicted_count", evictedCount,
		"evicted_mb", evictedBytes/(1024*1024),
		"reclaimed_mb", physicallyReclaimed/(1024*1024),
		"reclaim_pct", 100*float64(physicallyReclaimed)/float64(evictedBytes),
		"remaining_mb", c.approxSize.Load()/(1024*1024),
		"punch_syscalls", syscallCount,
		"batch_efficiency", fmt.Sprintf("%.1fx", batchEfficiency))

	c.bloomStats.deletions.Add(int64(evictedCount))

	if err := c.maybeTriggerBloomRebuild(); err != nil {
		log.Error("bloom rebuild failed", "error", err)
	}

	return nil
}

func (c *Cache) maybeCompactSegments() error {
	var errs []error

	// Phase 1: Tombstone compaction (metadata cleanup + hole punching)
	if err := c.maybeCompactTombstones(); err != nil {
		errs = append(errs, fmt.Errorf("tombstone compaction: %w", err))
	}

	// Phase 2: Merge compaction (combine sparse segments)
	if err := c.maybeMergeSegments(); err != nil {
		errs = append(errs, fmt.Errorf("merge compaction: %w", err))
	}

	return errors.Join(errs...)
}

// maybeMergeSegments identifies sparse segments and merges contiguous ranges.
// This reclaims space by combining multiple sparse segments into fewer dense segments.
//
// Uses the "Targeted Gravity" model:
//   - Dynamic minimum range size based on system sparseness (physical/logical ratio)
//   - Size-based accumulator targeting WriteBufferSize output
//   - Yielding between merges to protect foreground read throughput
func (c *Cache) maybeMergeSegments() error {
	// Calculate dynamic gravity based on system sparseness
	// physicalSize approximated by approxSize (tracks logical after hole punching)
	// For now, use a fixed gravity until we have proper physical size tracking
	logicalSize := c.approxSize.Load()
	physicalSize := logicalSize // TODO: Track actual physical size via stat.Blocks

	gravity := calculateDynamicGravity(physicalSize, logicalSize)
	// Target 1.5x WriteBufferSize (~96MB for default 64MB buffer) for efficient sequential I/O
	targetOutputSize := c.WriteBufferSize + c.WriteBufferSize/2

	// Select candidate ranges using sliding window accumulator
	candidates := c.selectSegmentsForMerge(targetOutputSize, gravity)
	if len(candidates) == 0 {
		return nil
	}

	log.Debug("merge compaction starting",
		"candidate_count", len(candidates),
		"gravity", gravity,
		"target_mb", targetOutputSize/(1024*1024))

	oldestSegID := c.OldestLiveSegmentID()

	for _, candidate := range candidates {
		segmentIDs := candidate.SegmentIDs

		// Determine if this is a tail compaction (includes oldest segment)
		// Safe to drop tombstones if we're compacting the oldest segment
		dropTombstones := len(segmentIDs) > 0 && segmentIDs[0] == oldestSegID

		result, err := c.compactor.Compact(segmentIDs, dropTombstones)
		if err != nil {
			// Fail fast: if one compaction fails, stop trying more.
			// Avoids accumulating repeated errors for the same underlying issue.
			return fmt.Errorf("compact segments %v: %w", segmentIDs, err)
		}

		// Populate Targeted Gravity metrics
		result.EstimatedInputMB = float64(candidate.EstimatedLiveBytes) / (1024 * 1024)
		result.ActualOutputMB = float64(result.WriteBytes) / (1024 * 1024)

		// Calculate derived I/O metrics
		var readAmp, readMBps, writeMBps float64
		var avgReadKB int64
		if result.WriteBytes > 0 {
			readAmp = float64(result.ReadBytes) / float64(result.WriteBytes)
		}
		if result.DurationMs > 0 {
			durSec := float64(result.DurationMs) / 1000.0
			readMBps = float64(result.ReadBytes) / durSec / (1024 * 1024)
			writeMBps = float64(result.WriteBytes) / durSec / (1024 * 1024)
		}
		if result.ReadOps > 0 {
			avgReadKB = result.ReadBytes / int64(result.ReadOps) / 1024
		}

		log.Info("segment merge completed",
			"old_segments", result.OldSegmentIDs,
			"new_segment", result.NewSegmentID,
			"items_compacted", result.ItemsCompacted,
			"stale_skipped", result.StaleSkipped,
			"tombstones_kept", result.TombstonesKept,
			"tombstones_dropped", result.TombstonesDropped,
			"tombstones_dissolved", result.TombstonesDissolved,
			"estimated_input_mb", fmt.Sprintf("%.1f", result.EstimatedInputMB),
			"actual_output_mb", fmt.Sprintf("%.1f", result.ActualOutputMB),
			"duration_ms", result.DurationMs,
			"read_ops", result.ReadOps,
			"write_ops", result.WriteOps,
			"read_amp", fmt.Sprintf("%.2f", readAmp),
			"read_mbps", fmt.Sprintf("%.1f", readMBps),
			"write_mbps", fmt.Sprintf("%.1f", writeMBps),
			"avg_read_kb", avgReadKB)

		// Token bucket rate limiting: throttle based on bytes written.
		// This protects foreground I/O (Archivist reads, MemTable writes) from
		// compaction saturating the I/O bus. The limiter refills at CompactionBandwidth
		// bytes/sec, so heavy compaction spreads out over time.
		if c.compactionLimiter != nil && result.WriteBytes > 0 {
			// WaitN blocks until we've earned tokens for the bytes we just wrote.
			// This is more precise than fixed sleep: large merges wait longer.
			tokens := int(result.WriteBytes)
			if tokens > c.compactionLimiter.Burst() {
				// If write exceeds burst, use burst size (limiter's max allowance)
				tokens = c.compactionLimiter.Burst()
			}
			_ = c.compactionLimiter.WaitN(context.Background(), tokens)
		}
	}

	// Recalculate oldest segment after dropping segments
	if err := c.recalculateOldestSegmentID(); err != nil {
		return fmt.Errorf("recalculate oldest segment: %w", err)
	}

	return nil
}

// maybeCompactTombstones identifies segments with many tombstones and compacts them.
// This collapses the tombstone incremental log into the segment manifest and
// reclaims space via hole punching.
func (c *Cache) maybeCompactTombstones() error {
	segments := c.selectSegmentsForTombstoneCompaction(DefaultTombstoneCompactionThreshold)
	if len(segments) == 0 {
		return nil
	}

	log.Debug("tombstone compaction starting", "segment_count", len(segments))

	for _, segID := range segments {
		// Acquire segment lock (shared with compaction, exclusive from Delete)
		shard := c.index.SegmentLockShard(segID)
		shard.Lock()
		err := c.compactSegmentTombstones(segID)
		shard.Unlock()

		if err != nil {
			// Fail fast on first error to avoid accumulating repeated errors
			return err
		}
	}

	return nil
}

// compactSegmentTombstones performs tombstone compaction on a single segment.
// This is the high-level operation that:
// 1. Hole punches tombstoned blobs (space reclamation)
// 2. Collapses tombstone log into segment manifest (metadata cleanup)
// 3. Updates SegmentMetadata counts
//
// The caller must hold the segment lock before calling this method.
func (c *Cache) compactSegmentTombstones(segID uint32) error {
	var punchedCount int
	var punchedBytes int64

	err := c.index.CompactTombstones(segID, func(tr index.TombstoneRecord) {
		// Hole punch (idempotent - no-op if already punched in cache mode/eviction)
		reclaimed, err := c.archivist.HolePunchBlob(segID, tr.Item.Offset, tr.Item.PhysicalLen)
		if err != nil {
			log.Warn("hole punch failed during tombstone compaction",
				"segment", segID, "key", tr.KeyHash, "error", err)
		} else if reclaimed > 0 {
			punchedCount++
			punchedBytes += reclaimed
		}
	})

	if err != nil {
		return fmt.Errorf("compact tombstones segment %d: %w", segID, err)
	}

	if punchedCount > 0 {
		log.Info("tombstone compaction completed",
			"segment", segID,
			"punched_count", punchedCount,
			"reclaimed_mb", punchedBytes/(1024*1024))
	}

	return nil
}
