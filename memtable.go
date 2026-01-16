package blobcache

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/miretskiy/blobcache/wal"
)

const (
	// numIndexShards is a power of 2 for fast modulo via bitmask.
	// 256 shards provides low contention (~2% collision with 32 concurrent writers).
	numIndexShards = 256
	indexShardMask = numIndexShards - 1
)

// errSequenceTooOld is returned when a write's seqID is older than maxSealedSeq.
// This happens when a slow writer awakens after slab rotation. The caller should:
// 1. Check the global index to see if a newer version exists
// 2. If newer exists, return success (idempotent - last write wins)
// 3. If older or missing, acquire a new seqID and retry
var errSequenceTooOld = errors.New("sequence ID too old: write belongs to sealed slab")

// MemTable is the Write Engine.
type MemTable struct {
	config
	Batcher
	ErrorReporter
	Knobs *TestingKnobs

	segmentID  atomic.Uint32
	slabPool   *MmapPool
	footerPool *MmapPool
	publisher  Publisher
	wal        *wal.WAL // nil if WAL disabled

	mu struct {
		sync.Mutex
		active      *ActiveSlab
		activeReady chan struct{}

		// maxSealedSeq is the highest SeqID in the last sealed slab.
		// Prevents "Time Travel" where a slow writer lands in a NEW slab after rotation,
		// hiding a newer write that's already in an OLD (sealed) slab.
		maxSealedSeq uint64
	}

	// Sharded locks for per-key consistency within active slab.
	// Prevents "Check-Then-Act" race where two threads updating the same key
	// both read stale state and the slower (older) write overwrites the faster (newer).
	// Usage: indexLocks[hash & indexShardMask].Lock()
	indexLocks [numIndexShards]sync.Mutex

	flushCh chan FlushTicket
	flushWg sync.WaitGroup // Tracks in-flight flush operations
	stopCh  chan struct{}
	wg      sync.WaitGroup // Tracks worker goroutines
}

func NewMemTable(cfg config, b Batcher, reporter ErrorReporter, pub Publisher, w *wal.WAL) *MemTable {
	poolCapacity := cfg.MaxCachedSlabs + cfg.MaxInflightSlabs + 2

	mt := &MemTable{
		config:        cfg,
		Batcher:       b,
		ErrorReporter: reporter,
		publisher:     pub,
		wal:           w, // nil if WAL disabled
		slabPool:      NewMmapPool("slab", cfg.WriteBufferSize, cfg.LargeWriteThreshold, poolCapacity),
		footerPool:    NewMmapPool("footer", 256<<10, 0, cfg.MaxInflightSlabs+1),
		flushCh:       make(chan FlushTicket, cfg.MaxInflightSlabs),
		stopCh:        make(chan struct{}),
	}

	mt.mu.active = mt.newActiveSlab(0)

	// Initialize segment ID from highest existing segment (workers will increment before use)
	mt.segmentID.Store(maxSegmentID(cfg.Path, cfg.Shards))

	mt.wg.Add(cfg.FlushConcurrency)
	for i := 0; i < cfg.FlushConcurrency; i++ {
		go mt.flushWorker()
	}

	return mt
}

// newActiveSlab allocates memory, initializes the struct, and publishes it.
// WARNING: Can block on slabPool.Acquire(). Do not call while holding mt.mu!
// Note: slabID is set on first write (it's the first SeqID written to this slab).
func (mt *MemTable) newActiveSlab(size int) *ActiveSlab {
	buf := func() *MmapBuffer {
		if size > 0 {
			return NewMmapBuffer(int64(size))
		}
		if mt.IsDegraded() {
			// If degraded, just safer to use unpooled.  Readers/Writers might
			// not have released their resources due to an error.
			return NewMmapBuffer(mt.WriteBufferSize)
		}
		return mt.slabPool.Acquire()
	}()

	as := &ActiveSlab{
		SharedSlab: SharedSlab{
			buf:   buf,
			index: xmap.New[SlabEntry, xmap.Pad32](xmap.WithShardShift(4)), // 16 shards for slab index
		},
		// slabID is 0 until first write sets it
		writesDone: newSignal(),
	}

	if mt.publisher != nil {
		mt.publisher.Publish(&as.SharedSlab)
	}

	return as
}

// maybeCompress applies the 1/8th heuristic compression if enabled.
// Returns a BufferHandle containing compressed data. If handle.IsZero(), no compression was applied.
// Release() MUST be called on the returned handle.
func (mt *MemTable) maybeCompress(src []byte) BufferHandle {
	cfg := mt.Compression

	// Skip compression if disabled or blob is too small (MinSize=0 means no minimum)
	if cfg.Codec == compression.CodexNone || (cfg.MinSize > 0 && int64(len(src)) < cfg.MinSize) {
		return BufferHandle{}
	}

	// 1/8th heuristic: dst buffer is 7/8 of src size (len - len>>3).
	// If compression can't achieve at least 12.5% savings, abort and store raw.
	maxDst := len(src) - len(src)>>3
	handle := AcquireBuffer(0, maxDst)

	result, err := compression.Compress(cfg.Codec, cfg.Level, handle.Bytes(), src)
	if compression.IsBufferTooSmall(err) || len(result) >= len(src) {
		// Compression didn't help (buffer too small or no savings), store raw
		handle.Release()
		return BufferHandle{}
	}

	// Compression succeeded - update handle to point to actual compressed data
	handle.buf = result
	return handle
}

// --- Write Logic ---

func (mt *MemTable) Put(seqID uint64, hash Key, keyBytes, value []byte) error {
	return mt.putWithChecksum(seqID, hash, keyBytes, value, nil)
}

func (mt *MemTable) PutChecksummed(seqID uint64, hash Key, keyBytes, value []byte, checksum uint32) error {
	return mt.putWithChecksum(seqID, hash, keyBytes, value, &checksum)
}

func (mt *MemTable) putWithChecksum(seqID uint64, hash Key, keyBytes, value []byte, checksum *uint32) error {
	if int64(len(value)) > mt.LargeWriteThreshold {
		return mt.putLarge(seqID, hash, keyBytes, value, checksum)
	}
	return mt.putActive(seqID, hash, keyBytes, value, checksum)
}

func (mt *MemTable) putLarge(seqID uint64, hash Key, keyBytes, value []byte, checksum *uint32) error {
	// 1. Compress in caller's goroutine (distributed compression)
	compressed := mt.maybeCompress(value)
	defer compressed.Release()

	// 2. Determine on-disk value bytes
	valueBytes := value
	codec := compression.CodexNone
	if !compressed.IsZero() {
		valueBytes = compressed.Bytes()
		codec = mt.Compression.Codec
	}

	// 3. Build record with key + value
	rec := record.NewRecord(seqID, keyBytes, valueBytes, int64(len(value)))
	rec.SetCompression(codec)

	// Override CRC if caller provided one (must be computed over key+value)
	if checksum != nil {
		rec.SetCRC(*checksum)
	}

	// 4. WAL Write BEFORE slab allocation (blocks until synced for durability)
	// If WAL fails, we haven't touched any state - clean early exit.
	// Skip WAL in degraded mode (degraded mode does everything except actual I/O).
	if mt.wal != nil && !mt.IsDegraded() {
		if err := mt.wal.Write(rec); err != nil {
			mt.ReportError(fmt.Errorf("wal write: %w", err)) // Enter degraded mode
			return err
		}
	}

	// 5. Allocate slab just for this record
	as := mt.newActiveSlab(rec.EncodedSize())

	// 6. Write record directly (zero-copy)
	buf, offset := as.Alloc(rec.EncodedSize())
	rec.EncodeTo(buf)

	// 7. Create SlabEntry for index lookup
	entry := SlabEntry{
		Header: rec.Header,
		Pos:    offset,
	}
	as.index.Put(hash, entry)
	as.slabID = seqID // Large writes have dedicated slab, seqID is the slabID
	as.currentMaxSeq = seqID

	if !mt.IsDegraded() {
		mt.sendToFlusher(as) // Acquires its own reference via PurchaseTicket
	}

	// Release "Active Writer" reference.
	as.buf.Unpin()
	return nil
}

func (mt *MemTable) putActive(seqID uint64, hash Key, keyBytes, value []byte, checksum *uint32) error {
	// 1. Compress before lock (parallel compression) - only on first call
	c := mt.maybeCompress(value)
	defer c.Release()
	return mt.putActiveCompressed(seqID, hash, keyBytes, value, checksum, c)
}

func (mt *MemTable) putActiveCompressed(
	seqID uint64, hash Key, keyBytes, value []byte, checksum *uint32, compressed BufferHandle,
) error {
	// 1. Build record BEFORE lock (pure computation, no lock needed)
	valueBytes := value
	codec := compression.CodexNone
	if !compressed.IsZero() {
		valueBytes = compressed.Bytes()
		codec = mt.Compression.Codec
	}

	rec := record.NewRecord(seqID, keyBytes, valueBytes, int64(len(value)))
	rec.SetCompression(codec)
	if checksum != nil {
		rec.SetCRC(*checksum)
	}

	// 2. WAL Write BEFORE slab allocation (blocks until synced for durability)
	// If WAL fails, we haven't touched the slab - clean early exit.
	// Skip WAL in degraded mode (degraded mode does everything except actual I/O).
	if mt.wal != nil && !mt.IsDegraded() {
		if err := mt.wal.Write(rec); err != nil {
			mt.ReportError(fmt.Errorf("wal write: %w", err)) // Enter degraded mode
			return err
		}
	}

	// 3. Write to slab (may retry on rotation)
	return mt.writeToSlab(seqID, hash, rec)
}

// writeToSlab handles slab allocation, index update, and pending write tracking.
// Separated from putActiveCompressed to keep WAL write outside the retry loop.
func (mt *MemTable) writeToSlab(seqID uint64, hash Key, rec record.Record) error {
	mt.mu.Lock()

	// 1. Lifecycle Guard: Reject writes older than already-sealed data.
	// This prevents "Time Travel" where a slow writer lands in a NEW slab
	// after rotation, hiding a newer write already in an OLD (sealed) slab.
	if seqID <= mt.mu.maxSealedSeq {
		mt.mu.Unlock()
		return errSequenceTooOld
	}

	// 2. Wait for Rotation (Backpressure)
	if mt.mu.activeReady != nil {
		wait := mt.mu.activeReady
		mt.mu.Unlock()
		<-wait
		return mt.writeToSlab(seqID, hash, rec) // Retry
	}

	active := mt.mu.active
	writeSize := rec.EncodedSize()

	// 3. Allocate space (under lock) - combines capacity check and reservation
	buf, wPos := active.Alloc(writeSize)
	if buf == nil {
		rotateUnlocked := mt.prepareRotationLocked()
		mt.mu.Unlock()
		rotateUnlocked()
		return mt.writeToSlab(seqID, hash, rec) // Retry
	}

	// 4. Track pending write (after successful allocation)
	active.pendingWrites.Add(1)

	// Set slabID on first write (used for WAL file naming/deletion)
	if active.slabID == 0 {
		active.slabID = seqID
	}

	// Track highest seqID in this slab for rotation handoff
	if seqID > active.currentMaxSeq {
		active.currentMaxSeq = seqID
	}

	mt.mu.Unlock()

	// 5. Write record directly to reserved region (zero-copy, outside lock)
	rec.EncodeTo(buf)

	// 6. Create SlabEntry for index lookup
	entry := SlabEntry{
		Header: rec.Header,
		Pos:    wPos,
	}

	// 7. Concurrency Guard: Prevent "Check-Then-Act" race.
	// Two threads updating the same key could both read stale state and
	// the slower (older) write could overwrite the faster (newer) one.
	shard := hash.Lo & indexShardMask
	mt.indexLocks[shard].Lock()
	if existing, ok := active.index.Get(hash); !ok || seqID > existing.SeqID {
		active.index.Put(hash, entry)
	}
	mt.indexLocks[shard].Unlock()

	// 8. Complete pending write tracking
	if active.pendingWrites.Add(-1) == 0 {
		if active.retired.Load() {
			active.writesDone.Close()
		}
	}
	return nil
}

// prepareRotationLocked sets up the rotation barrier and returns a closure
// that performs the allocation and switch-over outside the lock.
func (mt *MemTable) prepareRotationLocked() func() {
	old := mt.mu.active

	// 1. Setup Barrier (Block other writers)
	mt.mu.activeReady = make(chan struct{})

	// 2. Update Gatekeeper: Capture the highest seqID from the old slab.
	// This becomes the new threshold - any write with seqID <= this is rejected.
	if old.currentMaxSeq > mt.mu.maxSealedSeq {
		mt.mu.maxSealedSeq = old.currentMaxSeq
	}

	// 3. Retire Old Slab
	old.retired.Store(true)

	// 4. Detach Active (Set to nil so nobody touches it while we allocate)
	mt.mu.active = nil

	// Capture state for the unlocked closure
	waitCh := old.writesDone.ch
	hasPending := old.pendingWrites.Load() > 0
	shouldSend := !mt.IsDegraded()

	return func() {
		// A. Wait for pending writes on OLD slab
		if hasPending {
			<-waitCh
		}

		// B. Send OLD slab to flusher (acquires its own reference via PurchaseTicket)
		if shouldSend {
			mt.sendToFlusher(old)
		}

		// C. Release "Active Writer" reference.
		// The flusher has its own ref (via PurchaseTicket), and
		// the Librarian has its own ref (if enabled).
		old.buf.Unpin()

		// D. Allocate NEW slab (Blocking Operation)
		// This is done unlocked to prevent holding the mutex during allocation wait.
		newSlab := mt.newActiveSlab(0)

		// E. Rotate WAL file
		// At this point, all WAL writes for the old slab are complete
		// (we waited for pending writes above), so it's safe to close
		// the old WAL file. The next write will create a new WAL file
		// named by its SeqID.
		if mt.wal != nil {
			if err := mt.wal.Rotate(); err != nil {
				mt.ReportError(fmt.Errorf("wal rotate: %w", err))
			}
		}

		// F. Install NEW slab and Clear Barrier
		mt.mu.Lock()
		mt.mu.active = newSlab
		close(mt.mu.activeReady)
		mt.mu.activeReady = nil
		mt.mu.Unlock()
	}
}

// Flush triggers a rotation of the current slab (sends to flusher).
// Does not wait for the flush to complete - use Drain() for that.
// Implements wal.Replayer interface for recovery.
func (mt *MemTable) Flush() {
	if mt.IsDegraded() {
		return
	}

	mt.mu.Lock()
	if mt.mu.active != nil && mt.mu.active.wPos > 0 {
		rotateUnlocked := mt.prepareRotationLocked()
		mt.mu.Unlock()
		rotateUnlocked()
	} else {
		mt.mu.Unlock()
	}
}

// Drain triggers a flush and waits for ALL in-flight flushes to complete.
func (mt *MemTable) Drain() {
	mt.Flush()
	mt.flushWg.Wait()
}

func (mt *MemTable) sendToFlusher(as *ActiveSlab) {
	mt.flushWg.Add(1)
	ticket := as.PurchaseTicket()
	select {
	case mt.flushCh <- ticket:
	case <-mt.stopCh:
		ticket.Redeem()
		mt.flushWg.Done()
	}
}

func (mt *MemTable) flushWorker() {
	defer mt.wg.Done()
	writer, err := mt.openSegment()
	if err != nil {
		mt.ReportError(err)
		return
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			mt.ReportError(fmt.Errorf("writer close failed: %w", closeErr))
		}
	}()

	for {
		select {
		case ticket, ok := <-mt.flushCh:
			if !ok {
				return
			}
			as := ticket.Active
			nextWriter, flushErr := mt.processFlush(as, writer)
			if flushErr != nil {
				mt.ReportError(flushErr)
			}
			writer = nextWriter
			ticket.Redeem()
			mt.flushWg.Done()
		case <-mt.stopCh:
			return
		}
	}
}

func (mt *MemTable) processFlush(as *ActiveSlab, writer *SegmentWriter) (*SegmentWriter, error) {
	if mt.IsDegraded() {
		return writer, nil
	}

	if mt.Knobs != nil && mt.Knobs.InjectWriteErr != nil {
		if err := mt.Knobs.InjectWriteErr(); err != nil {
			return writer, err
		}
	}

	// 1. Collect entries and convert to record.Inode for WriteSlab
	var entries []record.Inode
	var maxSeqID uint64
	as.index.ForEach(func(key xmap.Key, e SlabEntry, _ *xmap.Pad32) bool {
		entries = append(entries, record.Inode{
			Key:          key, // Full 128-bit XXH3 hash
			Pos:          e.Pos,
			LogicalSize:  e.LogicalSize,
			PhysicalSize: e.PhysicalSize,
			SeqID:        e.SeqID,
			Flags:        e.Flags,
			KeyLen:       e.KeyLen, // From embedded record.Header
		})
		if e.SeqID > maxSeqID {
			maxSeqID = e.SeqID
		}
		return true
	})

	// 2. Sort by Physical Position (Offset) for linear I/O
	slices.SortFunc(entries, func(a, b record.Inode) int {
		return int(a.Pos - b.Pos)
	})

	// 3. Write Physical Slab
	alignedData := as.buf.AlignedBytes(as.wPos)
	absoluteEntries, err := writer.WriteSlab(alignedData, entries)
	if err != nil {
		return writer, fmt.Errorf("physical write failed: %w", err)
	}

	if mt.Knobs != nil && mt.Knobs.InjectIndexErr != nil {
		if err := mt.Knobs.InjectIndexErr(); err != nil {
			return writer, err
		}
	}

	// 4. Update Index (with maxSeqID for WAL recovery checkpoint)
	if err := mt.PutBatch(writer.id, absoluteEntries, maxSeqID); err != nil {
		return writer, fmt.Errorf("index update failed: %w", err)
	}

	// 5. Delete WAL file now that slab is durably flushed to segment
	// slabID is the first SeqID written to this slab (WAL file name)
	if mt.wal != nil && as.slabID != 0 {
		if err := mt.wal.DeleteFile(as.slabID); err != nil {
			// Log but don't fail - WAL file will be cleaned up on next recovery
			log.Warn("failed to delete WAL file", "slabID", as.slabID, "error", err)
		}
	}

	if writer.CurrentPos() >= mt.SegmentSize {
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close segment failed: %w", err)
		}
		return mt.openSegment()
	}
	return writer, nil
}

func (mt *MemTable) openSegment() (*SegmentWriter, error) {
	segmentID := mt.segmentID.Add(1)
	path := getSegmentPath(mt.Path, mt.Shards, segmentID)
	return NewSegmentWriter(
		segmentID, path,
		mt.SegmentSize, mt.footerPool, mt.IO.FDataSync, mt.IO.DirectIOWrite,
	)
}

func (mt *MemTable) Close() {
	select {
	case <-mt.stopCh:
	default:
		close(mt.stopCh)
	}
	mt.wg.Wait()
}

// ClosePools releases all pre-allocated mmap buffers.
// Must be called AFTER Librarian.Close() returns slabs to pools.
func (mt *MemTable) ClosePools() {
	mt.slabPool.Close()
	mt.footerPool.Close()
}

// ReplayRecord is called during WAL recovery to replay a record as-is.
// MUST only be called during initialization (no concurrent writers).
// Bypasses compression and CRC computation - record is written verbatim.
func (mt *MemTable) ReplayRecord(hash Key, rec record.Record) error {
	active := mt.mu.active
	writeSize := rec.EncodedSize()

	// Simple rotation if needed (no concurrency, no backpressure)
	buf, wPos := active.Alloc(writeSize)
	if buf == nil {
		mt.mu.active = mt.newActiveSlab(0)
		active = mt.mu.active
		buf, wPos = active.Alloc(writeSize)
	}

	// Set slabID on first write - enables WAL file deletion after flush
	if active.slabID == 0 {
		active.slabID = rec.SeqID
	}

	rec.EncodeTo(buf)

	if rec.SeqID > active.currentMaxSeq {
		active.currentMaxSeq = rec.SeqID
	}

	active.index.Put(hash, SlabEntry{Header: rec.Header, Pos: wPos})
	return nil
}
