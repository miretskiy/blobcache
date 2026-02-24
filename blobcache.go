package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"runtime/debug"
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
)

// Key is the 128-bit hash of a blob key.
// Lo is used for bloom filter; full 128-bit for index to avoid collisions.
type Key = xxh3.Uint128

const (
	// evictionHysteresis is the target fraction of MaxSize to evict to.
	evictionHysteresis = 0.93
)

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
		hits      atomic.Uint64 // Bloom filter said "yes"
		ghosts    atomic.Uint64 // Bloom said yes, but index said no.
		deletions atomic.Int64  // Track cumulative deletions since last rebuild
	}

	// --- ARCHITECTURE COMPONENTS ---
	memTable  *MemTable  // The Write Engine (Producer)
	librarian *Librarian // The Write-Path Read Cache (Consumer)
	readCache *ReadCache // The User-Space Read Cache (nil if disabled)
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
	maintenanceTrigger chan struct{} // Capacity 1: trigger eviction cycle
	stopCh             chan struct{}
	wg                 sync.WaitGroup

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &TestingKnobs{...}
	Knobs *TestingKnobs

	// keyIndex is the global Pebble-backed key index for ordered iteration.
	// Rebuildable from segment files — not a durability layer.
	// nil if not available (e.g., during recovery without path).
	keyIndex *KeyIndex

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
// Used by MemTable to allocate unique segment IDs for flushed slabs.
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

	// Auto-compute shard count if not explicitly set.
	// Target: ~1024 files per directory. Each segment produces up to 3 files
	// (.seg, .sst, .del), so shards ≈ totalFiles/1024, rounded up to power of 2.
	if cfg.Shards == 0 && cfg.MaxSize > 0 && cfg.WriteBufferSize > 0 {
		totalFiles := cfg.MaxSize / cfg.WriteBufferSize * 3
		shards := 1 << bits.Len(uint(max(1, totalFiles/1024)-1))
		cfg.Shards = min(shards, 256)
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

	// Create new bloom filter and compute total size from in-memory index.
	// Using ForEachBlob instead of ForEachSegment avoids disk scanning.
	var totalSize int64
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	idx.ForEachBlob(func(item index.Item) bool {
		if !item.IsDeleted() {
			filter.Add(item.Key)                 // Full 128-bit key
			totalSize += int64(item.PhysicalLen) // Track on-disk size
		}
		return true
	})

	// Get oldest segment from in-memory registry (O(1))
	oldestSegID := idx.GetOldestSegmentID()

	c := &Cache{
		config:             cfg,
		index:              idx,
		archivist:          NewArchivist(cfg, idx, cfg.IOScheduler),
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

	// Initialize read cache if enabled (before memtable — no dependency).
	if cfg.ReadCacheSlabs > 0 {
		rcSlabSize := cfg.ReadCacheSlabSize
		if rcSlabSize <= 0 {
			rcSlabSize = cfg.WriteBufferSize
		}
		rcMaxItemSize := cfg.ReadCacheMaxItemSize
		if rcMaxItemSize == 0 {
			rcMaxItemSize = rcSlabSize / 64 // Default: ensure ≥64 items per slab
		} else if rcMaxItemSize < 0 {
			rcMaxItemSize = 0 // -1 = no limit → pass 0 to ReadCache
		}
		rc := NewReadCache(
			rcSlabSize,
			cfg.ReadCacheSlabs,
			rcMaxItemSize,
			c,
		)
		c.archivist.readCache = rc
		c.readCache = rc // Keep for lifecycle: Close, Delete/Invalidate, Stats, Decay
	}

	// Open global Pebble key index (rebuildable — non-fatal if it fails).
	ki, kiErr := OpenKeyIndex(cfg.Path)
	if kiErr != nil {
		log.Warn("failed to open key index, iteration will be unavailable", "error", kiErr)
	}
	c.keyIndex = ki

	// Reconcile Pebble key index with RAM index — rebuild missing segments.
	if c.keyIndex != nil {
		c.reconcileKeyIndex()
	}

	c.memTable = NewMemTable(c.config, c, c, c.librarian, c.wal, c.segIDs)
	c.memTable.keyIndex = c.keyIndex
	c.memTable.Knobs = c.Knobs

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

// Start begins background operations (maintenance worker for eviction).
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

	// 3. Close Read Cache (releases mmap arenas back to its dedicated pool)
	if c.readCache != nil {
		c.readCache.Close()
	}

	// 4. Release pool memory (must be after librarian returns slabs)
	c.memTable.ClosePools()

	c.wg.Wait()

	// Collect all close errors (WAL may be nil if disabled)
	var walErr error
	if c.wal != nil {
		walErr = c.wal.Close()
	}

	var keyIndexErr error
	if c.keyIndex != nil {
		keyIndexErr = c.keyIndex.Close()
	}

	return errors.Join(
		walErr,
		c.archivist.Close(),
		c.index.Close(),
		keyIndexErr,
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
//
// The ReadCache (if enabled) is transparent inside Archivist.ReadBlob —
// search() does not interact with it directly.
func (c *Cache) search(key []byte) (data []byte, rel Releaser, ok bool) {
	h := xxh3.Hash128(key)

	// 1. Bloom Filter Gate (full 128-bit key)
	if !c.hot.bloom.Load().Test(h) {
		return nil, Releaser{}, false
	}

	if h.Lo&127 == 0 {
		c.bloomStats.hits.Add(128)
	}

	// 2. RAM Hit (Librarian — write-path cache)
	if ramData, storedKey, releaser, found := c.librarian.Acquire(h); found {
		// Verify key to detect hash collision
		if !c.hot.trustHash && !bytes.Equal(storedKey, key) {
			releaser.Release()
			c.bloomStats.ghosts.Add(1) // Hash collision: bloom said yes, wrong key
			return nil, Releaser{}, false
		}
		return ramData, releaser, true
	}

	// 3. Index lookup (O(1) sharded hash table)
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

	// 5. Archivist.ReadBlob (ReadCache is inside, transparent)
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

	// Invalidate caches to prevent serving stale data
	c.librarian.Invalidate(h)
	if c.readCache != nil {
		c.readCache.Invalidate(h)
	}

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

	// Remove from Pebble key index (best-effort — rebuildable).
	if c.keyIndex != nil {
		if err := c.keyIndex.DeleteByUserKey(key, h); err != nil {
			log.Warn("keyindex delete failed", "error", err)
		}
	}

	// Update segment metadata (for compaction selection)
	c.index.UpdateSegmentOnDelete(segID, 1, int64(item.PhysicalLen))

	c.bloomStats.deletions.Add(1)
	return nil
}

// deleteInCacheMode handles deletion in Cache mode (no WAL).
// Marks the item as dead in the index; physical space is reclaimed later
// by merge compaction (rewriting sparse segments into dense output).
//
// Uses segment lock to coordinate with compaction (prevents concurrent segment drop).
func (c *Cache) deleteInCacheMode(key []byte, h Key, item index.Item) error {
	segID := item.SegmentID

	// Acquire exclusive segment lock (blocks compaction of this segment)
	shard := c.index.SegmentLockShard(segID)
	shard.Lock()
	defer shard.Unlock()

	// Write tombstone to incremental log (with user key)
	if err := c.index.Tombstone(segID, h, key); err != nil {
		return fmt.Errorf("write tombstone: %w", err)
	}

	// Remove from Pebble key index (best-effort — rebuildable).
	if c.keyIndex != nil {
		if err := c.keyIndex.DeleteByUserKey(key, h); err != nil {
			log.Warn("keyindex delete failed", "error", err)
		}
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
	PutBatch(segID uint32, entries []record.FooterEntry, maxSeqID uint64) error
}

// maintenanceSegmentInterval determines how often segment production triggers maintenance.
// Every N segments, an eviction check is triggered.
const maintenanceSegmentInterval = 5

func (c *Cache) PutBatch(segID uint32, entries []record.FooterEntry, _ uint64) error {
	// Phase 1: Ingest into Index with entries for in-memory manifest caching
	c.index.AddSegmentFromEntries(segID, entries)

	// Phase 2: Update size tracking (using PhysicalLen = on-disk size)
	var addedBytes int64
	for i := range entries {
		if !entries[i].IsDeleted() {
			addedBytes += int64(record.HeaderSize) + int64(entries[i].KeyLen) + entries[i].PhysicalSize
		}
	}
	newSize := c.approxSize.Add(addedBytes)

	// Phase 3: Trigger maintenance (eviction) when needed
	overSizeLimit := c.MaxSize > 0 && newSize > c.MaxSize
	segmentInterval := segID%maintenanceSegmentInterval == 0

	if (overSizeLimit || segmentInterval) && !c.IsDegraded() {
		c.triggerMaintenance()
	}

	return nil
}

// triggerMaintenance signals the background worker to run eviction.
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

// reconcileKeyIndex ensures the Pebble key index is in sync with the RAM index.
// For each registered segment, checks for a sentinel in Pebble. If missing,
// scans the .seg file to extract user keys and adds them to Pebble.
func (c *Cache) reconcileKeyIndex() {
	segIDs := c.index.SnapshotSegmentIDs()
	var rebuilt int
	for _, segID := range segIDs {
		has, err := c.keyIndex.HasSentinel(segID)
		if err != nil {
			log.Warn("keyindex sentinel check failed", "segID", segID, "error", err)
			continue
		}
		if has {
			continue // Already in Pebble.
		}

		// Segment not in Pebble — rebuild from .seg file.
		segPath := getSegmentPath(c.Path, c.Shards, segID)
		entries, err := scanSegmentForUserKeys(segPath)
		if err != nil {
			log.Warn("keyindex rebuild scan failed", "segID", segID, "error", err)
			continue
		}
		if err := c.keyIndex.AddEntries(segID, entries); err != nil {
			log.Warn("keyindex rebuild add failed", "segID", segID, "error", err)
			continue
		}
		rebuilt++
	}
	if rebuilt > 0 {
		log.Info("keyindex reconciliation complete", "rebuilt", rebuilt, "total", len(segIDs))
	}
}

func (c *Cache) rebuildBloom() error {
	start := time.Now()
	prevGhosts := c.bloomStats.ghosts.Load()
	prevHits := c.bloomStats.hits.Load()
	prevDeletions := c.bloomStats.deletions.Load()

	newFilter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)

	var stopRecording func()
	var consumeRecording func(bloom.KeyConsumer)

	if oldFilter := c.hot.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
	}

	// Use in-memory index instead of disk scan (avoids "invalid file magic" races)
	var liveKeys int
	c.index.ForEachBlob(func(item index.Item) bool {
		if !item.IsDeleted() {
			newFilter.AddHash(item.Key)
			liveKeys++
		}
		return true
	})

	oldFilter := c.hot.bloom.Swap(newFilter)

	if oldFilter != nil && stopRecording != nil {
		stopRecording()
		consumeRecording(newFilter.AddHash)
	}

	// Reset all bloom stats for a clean observation window post-rebuild.
	c.bloomStats.deletions.Store(0)
	c.bloomStats.ghosts.Store(0)
	c.bloomStats.hits.Store(0)

	observedFPR := 0.0
	if prevHits > 0 {
		observedFPR = float64(prevGhosts) / float64(prevHits) * 100
	}
	log.Info("bloom filter rebuilt",
		"duration", time.Since(start),
		"live_keys", liveKeys,
		"prev_ghosts", prevGhosts,
		"prev_hits", prevHits,
		"prev_deletions", prevDeletions,
		"observed_fpr_pct", fmt.Sprintf("%.2f", observedFPR))

	return nil
}

func (c *Cache) maybeTriggerBloomRebuild() error {
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
		// Replay delete: look up item and mark as tombstone.
		// No hole punching — evicted items are logically dead.
		item, found := r.cache.index.Get(h)
		if found && !item.IsDeleted() {
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

// maintenanceWorker handles eviction and tombstone compaction in an event-driven loop.
//
// Event source: maintenanceTrigger fired by PutBatch when over size limit.
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

			// Phase 2: Segment compaction (WAL/durable mode only)
			if c.wal != nil && !c.IsDegraded() {
				if err := c.maybeRewriteSegments(); err != nil {
					c.ReportError(fmt.Errorf("segment compaction: %w", err))
					return
				}
			}

			// Phase 3: Segment drain (cache mode only)
			if c.wal == nil && !c.IsDegraded() {
				if err := c.maybeDrainSegments(); err != nil {
					c.ReportError(fmt.Errorf("segment drain: %w", err))
					return
				}
			}

		case <-c.stopCh:
			return
		}
	}
}

// runEvictionSieve evicts blobs using Sieve algorithm until under size limit.
//
// Each iteration: SIEVE selects cold items, commits tombstones, and updates
// segment metadata. Physical space is NOT reclaimed here — pressure-driven
// drain retires and deletes sparse segment files.
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
		evictedBytes int64
		evictedCount int
	)

	for evictedBytes < toEvictBytes {
		batch := c.index.Evict(toEvictBytes - evictedBytes)
		if len(batch) == 0 {
			break
		}

		if err := c.index.DeleteBlobs(batch...); err != nil {
			return fmt.Errorf("eviction durability sync failed: %w", err)
		}

		// Remove evicted items from Pebble key index (best-effort).
		// Uses reverse lookup: hash → user key.
		if c.keyIndex != nil {
			for _, v := range batch {
				if err := c.keyIndex.DeleteByHash(v.Key); err != nil {
					log.Warn("keyindex eviction delete failed", "error", err)
				}
			}
		}

		// Group by segment and update metadata.
		segStats := make(map[uint32]struct {
			count int32
			bytes int64
		})
		var batchBytes int64
		for _, v := range batch {
			s := segStats[v.SegmentID]
			s.count++
			s.bytes += int64(v.PhysicalLen)
			segStats[v.SegmentID] = s
			batchBytes += int64(v.PhysicalLen)
		}
		for segID, s := range segStats {
			c.index.UpdateSegmentOnDelete(segID, s.count, s.bytes)
		}

		evictedBytes += batchBytes
		evictedCount += len(batch)
	}

	if evictedCount == 0 {
		return nil
	}

	// METRICS & MAINTENANCE
	c.approxSize.Add(-evictedBytes)

	log.Info("eviction completed",
		"duration", time.Since(evictionStart),
		"evicted_count", evictedCount,
		"evicted_mb", evictedBytes/(1024*1024),
		"remaining_mb", c.approxSize.Load()/(1024*1024))

	c.bloomStats.deletions.Add(int64(evictedCount))

	if err := c.maybeTriggerBloomRebuild(); err != nil {
		log.Error("bloom rebuild failed", "error", err)
	}

	return nil
}

// maybeDrainSegments is pressure-driven disk reclamation for cache mode.
// Drain operates on a DIFFERENT pressure signal than SIEVE eviction:
//   - SIEVE manages live data size: triggers when approxSize > MaxSize
//   - Drain manages disk waste: triggers when (diskBytes - liveBytes) > MaxSize/2
//
// Hysteresis: triggers at high watermark (waste > MaxSize/2) but drains down to
// low watermark (waste < MaxSize/4). This prevents cascading re-triggers — each
// drained segment's live data is subtracted from approxSize, which would push
// waste back above a single threshold. Draining to a lower target absorbs this.
//
// Zero write amplification: no data is rewritten, segment files are simply deleted.
// Live items become cache misses — acceptable because they are in the sparsest
// segments (least live data sacrificed per segment deleted).
func (c *Cache) maybeDrainSegments() error {
	if c.MaxSize <= 0 {
		return nil // No size limit, no drain needed
	}

	// Estimate on-disk footprint: numSegments * WriteBufferSize.
	segCount := c.index.GetSegmentCount()
	estimatedDiskBytes := int64(segCount) * c.WriteBufferSize
	liveBytes := c.approxSize.Load()
	wasteBytes := estimatedDiskBytes - liveBytes

	// High watermark: trigger drain when waste > 50% of MaxSize.
	highWatermark := c.MaxSize / 2
	if wasteBytes <= highWatermark {
		return nil // Waste within budget
	}

	// Low watermark: drain down to 25% of MaxSize to prevent re-trigger cascade.
	lowWatermark := c.MaxSize / 4
	drainTarget := wasteBytes - lowWatermark

	// Cooling: only drain segments older than Librarian window.
	currentSegID := c.segIDs.CurrentSegmentID()
	coolingGap := uint32(c.MaxCachedSlabs + index.CoolingPeriodMargin)
	if currentSegID <= coolingGap {
		return nil
	}
	maxEligibleID := currentSegID - coolingGap

	candidates := c.index.GetDrainCandidates(maxEligibleID)
	if len(candidates) == 0 {
		return nil
	}

	var totalDrainedBytes int64
	var totalDrainedCount int
	var segmentsDrained int

	for _, seg := range candidates {
		if drainTarget <= 0 {
			break
		}

		// Exclusive segment lock: blocks concurrent reads and Delete() during drain.
		// File deletion must happen inside the lock to prevent TOCTOU races where
		// a reader has already resolved an Item but hasn't opened the file yet.
		shard := c.index.SegmentLockShard(seg.ID)
		shard.Lock()

		drainedBytes, drainedCount := c.index.DrainSegment(seg.ID)
		c.archivist.DropSegmentCache(seg.ID)

		if err := DeleteSegmentFiles(c.Path, c.Shards, seg.ID); err != nil {
			log.Warn("drain: delete segment file", "segID", seg.ID, "error", err)
		}

		// Remove all entries for this segment from Pebble key index.
		if c.keyIndex != nil {
			if err := c.keyIndex.DrainSegment(seg.ID); err != nil {
				log.Warn("keyindex drain failed", "segID", seg.ID, "error", err)
			}
		}

		shard.Unlock()

		totalDrainedBytes += drainedBytes
		totalDrainedCount += drainedCount
		segmentsDrained++
		drainTarget -= c.WriteBufferSize
	}

	if segmentsDrained > 0 {
		c.approxSize.Add(-totalDrainedBytes)
		c.bloomStats.deletions.Add(int64(totalDrainedCount))

		log.Info("segment drain completed",
			"segments", segmentsDrained,
			"drained_items", totalDrainedCount,
			"drained_mb", totalDrainedBytes/(1024*1024))

		if err := c.maybeTriggerBloomRebuild(); err != nil {
			log.Error("bloom rebuild failed after drain", "error", err)
		}
	}

	return nil
}
