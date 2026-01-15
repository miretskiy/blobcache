package blobcache

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/zhangyunhao116/skipmap"
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

	segmentID  atomic.Int64
	slabPool   *MmapPool
	footerPool *MmapPool
	publisher  Publisher

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

func NewMemTable(cfg config, b Batcher, reporter ErrorReporter, pub Publisher) *MemTable {
	poolCapacity := cfg.MaxCachedSlabs + cfg.MaxInflightSlabs + 2

	mt := &MemTable{
		config:        cfg,
		Batcher:       b,
		ErrorReporter: reporter,
		publisher:     pub,
		slabPool:      NewMmapPool("slab", cfg.WriteBufferSize, cfg.LargeWriteThreshold, poolCapacity),
		footerPool:    NewMmapPool("footer", 256<<10, 0, cfg.MaxInflightSlabs+1),
		flushCh:       make(chan FlushTicket, cfg.MaxInflightSlabs),
		stopCh:        make(chan struct{}),
	}
	mt.segmentID.Store(time.Now().UnixNano())

	mt.mu.active = mt.newActiveSlab(0)

	mt.wg.Add(cfg.FlushConcurrency)
	for i := 0; i < cfg.FlushConcurrency; i++ {
		go mt.flushWorker()
	}

	return mt
}

// newActiveSlab allocates memory, initializes the struct, and publishes it.
// WARNING: Can block on slabPool.Acquire(). Do not call while holding mt.mu!
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
			index: skipmap.NewUint64[SlabEntry](),
		},
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

func (mt *MemTable) Put(seqID uint64, key Key, value []byte) error {
	return mt.putWithChecksum(seqID, key, value, nil)
}

func (mt *MemTable) PutChecksummed(seqID uint64, key Key, value []byte, checksum uint32) error {
	return mt.putWithChecksum(seqID, key, value, &checksum)
}

func (mt *MemTable) putWithChecksum(seqID uint64, key Key, value []byte, checksum *uint32) error {
	if int64(len(value)) > mt.LargeWriteThreshold {
		return mt.putLarge(seqID, key, value, checksum)
	}
	return mt.putActive(seqID, key, value, checksum)
}

func (mt *MemTable) putLarge(seqID uint64, key Key, value []byte, checksum *uint32) error {
	// 1. Compress in caller's goroutine (distributed compression)
	compressed := mt.maybeCompress(value)
	defer compressed.Release()

	// 2. Bypass lock for large writes, allocate slab for header + data
	var as *ActiveSlab
	valueSize := len(value)
	if !compressed.IsZero() {
		valueSize = len(compressed.Bytes())
	}
	slabSize := record.HeaderSize + valueSize
	as = mt.newActiveSlab(slabSize)

	// 3. Build slab entry (embeds record.Header)
	entry := makeSlabEntry(seqID, 0, value, compressed.Bytes(), mt.Compression.Codec, mt.Resilience.ChecksumHasher, checksum)

	// 4. Write header + value
	as.buf.WriteAt(record.AppendHeader(nil, entry.Header), 0)
	if compressed.IsZero() {
		as.buf.WriteAt(value, int64(record.HeaderSize))
	} else {
		as.buf.WriteAt(compressed.Bytes(), int64(record.HeaderSize))
	}
	as.wPos = int64(slabSize)

	as.index.Store(key, entry)
	as.currentMaxSeq = seqID

	if !mt.IsDegraded() {
		mt.sendToFlusher(as) // Acquires its own reference via PurchaseTicket
	}

	// Release "Active Writer" reference.
	as.buf.Unpin()
	return nil
}

func (mt *MemTable) putActive(seqID uint64, key Key, value []byte, checksum *uint32) error {
	// 1. Compress before lock (parallel compression) - only on first call
	c := mt.maybeCompress(value)
	defer c.Release()
	return mt.putActiveCompressed(seqID, key, value, checksum, c)
}

func (mt *MemTable) putActiveCompressed(
	seqID uint64, key Key, value []byte, checksum *uint32, compressed BufferHandle,
) error {
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
		return mt.putActiveCompressed(seqID, key, value, checksum, compressed)
	}

	active := mt.mu.active
	valueSize := int64(len(value))
	if !compressed.IsZero() {
		valueSize = int64(len(compressed.Bytes()))
	}
	writeSize := int64(record.HeaderSize) + valueSize

	// 3. Check Capacity & Rotate
	if active.wPos+writeSize > int64(active.buf.Cap()) {
		rotateUnlocked := mt.prepareRotationLocked()
		mt.mu.Unlock()
		rotateUnlocked()
		return mt.putActiveCompressed(seqID, key, value, checksum, compressed)
	}

	// 4. Reservation
	active.pendingWrites.Add(1)
	wPos := active.wPos
	active.wPos += writeSize

	// Track highest seqID in this slab for rotation handoff
	if seqID > active.currentMaxSeq {
		active.currentMaxSeq = seqID
	}

	mt.mu.Unlock()

	// 5. Build slab entry and write header + value
	entry := makeSlabEntry(seqID, wPos, value, compressed.Bytes(), mt.Compression.Codec, mt.Resilience.ChecksumHasher, checksum)
	active.buf.WriteAt(record.AppendHeader(nil, entry.Header), wPos)
	if compressed.IsZero() {
		active.buf.WriteAt(value, wPos+int64(record.HeaderSize))
	} else {
		active.buf.WriteAt(compressed.Bytes(), wPos+int64(record.HeaderSize))
	}

	// 6. Concurrency Guard: Prevent "Check-Then-Act" race.
	// Two threads updating the same key could both read stale state and
	// the slower (older) write could overwrite the faster (newer) one.
	shard := key & indexShardMask
	mt.indexLocks[shard].Lock()
	if existing, ok := active.index.Load(key); !ok || seqID > existing.SeqID {
		active.index.Store(key, entry)
	}
	mt.indexLocks[shard].Unlock()

	// 7. Complete
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

		// E. Install NEW slab and Clear Barrier
		mt.mu.Lock()
		mt.mu.active = newSlab
		close(mt.mu.activeReady)
		mt.mu.activeReady = nil
		mt.mu.Unlock()
	}
}

func (mt *MemTable) Drain() {
	if mt.IsDegraded() {
		return
	}

	mt.mu.Lock()
	if mt.mu.active != nil && mt.mu.active.wPos > 0 {
		// Use the same rotation logic to flush the current buffer.
		rotateUnlocked := mt.prepareRotationLocked()
		mt.mu.Unlock()

		rotateUnlocked()
	} else {
		mt.mu.Unlock()
	}

	// Wait for ALL in-flight flushes to complete.
	mt.flushWg.Wait()
}

// makeSlabEntry creates a SlabEntry with embedded record.Header.
// The Header.Magic is set for disk writes; KeyLen is 0 (key bytes not stored yet).
func makeSlabEntry(
	seqID uint64, offset int64, original []byte, compressed []byte, codec compression.Codex,
	hasher Hasher, checksum *uint32,
) SlabEntry {
	// Determine sizes based on whether compression was applied
	logicalSize := int64(len(original))
	physicalSize := logicalSize
	if compressed != nil {
		physicalSize = int64(len(compressed))
	} else {
		codec = compression.CodexNone
	}

	entry := SlabEntry{
		Header: record.Header{
			Magic:        record.RecordMagic,
			Flags:        record.FlagInvalidCRC, // Will be cleared if checksum is set
			SeqID:        seqID,
			KeyLen:       0, // TODO: set when we start storing keys
			PhysicalSize: physicalSize,
			LogicalSize:  logicalSize,
		},
		Pos: offset,
	}

	// Set compression codec in flags
	entry.SetCompression(codec)

	// Checksum is computed on ORIGINAL data (before compression)
	// This allows verification after decompression
	if checksum != nil {
		entry.SetCRC(*checksum)
	} else if hasher != nil {
		h := hasher()
		h.Write(original)
		entry.SetCRC(h.Sum32())
	}
	return entry
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

	// 1. Collect entries and convert to record.FooterEntry for WriteSlab
	var entries []record.FooterEntry
	as.index.Range(func(hash uint64, e SlabEntry) bool {
		entries = append(entries, record.FooterEntry{
			Hash:         hash,
			Pos:          e.Pos,
			LogicalSize:  e.LogicalSize,
			PhysicalSize: e.PhysicalSize,
			SeqID:        e.SeqID,
			Flags:        e.Flags,
		})
		return true
	})

	// 2. Sort by Physical Position (Offset) for linear I/O
	slices.SortFunc(entries, func(a, b record.FooterEntry) int {
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

	// 4. Update Index
	if err := mt.PutBatch(writer.id, absoluteEntries); err != nil {
		return writer, fmt.Errorf("index update failed: %w", err)
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
	return NewSegmentWriter(
		segmentID, getSegmentPath(mt.Path, mt.Shards, segmentID),
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
