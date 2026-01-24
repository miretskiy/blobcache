package blobcache

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/miretskiy/blobcache/compression"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/wal"
	"github.com/miretskiy/blobcache/internal/xmap"
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

	segIDs     SegmentIDProvider
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

func NewMemTable(
	cfg config, b Batcher, reporter ErrorReporter, pub Publisher, w *wal.WAL,
	segIDs SegmentIDProvider,
) *MemTable {
	poolCapacity := cfg.MaxCachedSlabs + cfg.MaxInflightSlabs + 2

	mt := &MemTable{
		config:        cfg,
		Batcher:       b,
		ErrorReporter: reporter,
		publisher:     pub,
		wal:           w, // nil if WAL disabled
		segIDs:        segIDs,
		slabPool:      NewMmapPool("slab", cfg.WriteBufferSize, poolCapacity),
		footerPool:    NewMmapPool("footer", 256<<10, cfg.MaxInflightSlabs+1),
		flushCh:       make(chan FlushTicket, cfg.MaxInflightSlabs),
		stopCh:        make(chan struct{}),
	}

	mt.mu.active = mt.newActiveSlab(0)

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
		// Reserve first 8 bytes for file header (magic + version).
		// processFlush fills this in before writing, making every segment
		// a self-describing file that can be validated independently.
		wPos:       record.FileHeaderSize,
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

	// Compression succeeded - update handle's working buffer to the compressed data
	handle.SetBytes(result)
	return BufferHandle{h: handle.h}
}

// --- Write Logic ---

func (mt *MemTable) Put(seqID uint64, hash Key, keyBytes, value []byte) error {
	return mt.putActive(seqID, hash, keyBytes, value, nil)
}

func (mt *MemTable) PutChecksummed(
	seqID uint64, hash Key, keyBytes, value []byte, checksum uint32,
) error {
	return mt.putActive(seqID, hash, keyBytes, value, &checksum)
}

func (mt *MemTable) putActive(
	seqID uint64, hash Key, keyBytes, value []byte, checksum *uint32,
) error {
	// 1. Compress before lock (parallel compression) - only on first call
	c := mt.maybeCompress(value)
	defer c.Release()
	return mt.putActiveCompressed(seqID, hash, keyBytes, value, checksum, &c)
}

func (mt *MemTable) putActiveCompressed(
	seqID uint64, hash Key, keyBytes, value []byte, checksum *uint32, compressed *BufferHandle,
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

	// 2. Reserve space in slab, then WAL write, then fill (Reserve-First pattern)
	return mt.writeToSlab(seqID, hash, rec)
}

func (mt *MemTable) useWal() bool {
	return mt.wal != nil && !mt.IsDegraded()
}

// writeToSlab handles the Reserve-First write pattern: Reserve → WAL → Fill.
// This prevents the "Spillover Bug" where a record written to WAL file N could
// land in slab N+1 after rotation, causing data loss when WAL file N is deleted.
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
	xlWrite := int64(writeSize) > mt.WriteBufferSize

	var buf []byte
	var wPos int64
	var needRotation bool

	if xlWrite {
		// Check if XL accumulation would exceed threshold (2x buffer size).
		// Without this check, a workload of only XL writes would never rotate.
		if active.xlSize+int64(writeSize) <= 2*mt.WriteBufferSize {
			// Reserve position and increment xlSize under lock.
			// Actual buffer allocation happens after unlock to avoid mmap syscall under lock.
			wPos = active.AlignPosToPageBoundary()
			active.xlSize += int64(sys.PageAlign(record.FileHeaderSize + int64(writeSize)))
		} else {
			needRotation = true
		}
	} else {
		// 3. Allocate space (under lock) - combines capacity check and reservation
		buf, wPos = active.Alloc(writeSize)
		needRotation = buf == nil
	}

	// Rotation needed: either Alloc failed (normal) or XL threshold exceeded
	if needRotation {
		rotateUnlocked := mt.prepareRotationLocked()
		mt.mu.Unlock()
		rotateUnlocked()
		return mt.writeToSlab(seqID, hash, rec) // Retry
	}

	// 4. Track pending write (after successful reservation)
	active.pendingWrites.Add(1)

	// Track highest seqID in this slab for rotation handoff
	if seqID > active.currentMaxSeq {
		active.currentMaxSeq = seqID
	}

	mt.mu.Unlock()

	// 5. Allocate XL buffer outside lock (position already reserved).
	// File header space reserved so XLBuf can be written as standalone segment if needed.
	var xlBuf *MmapBuffer
	if xlWrite {
		xlBuf = NewMmapBuffer(record.FileHeaderSize + int64(rec.EncodedSize()))
		buf = xlBuf.raw[record.FileHeaderSize:]
	}

	// 5. WAL Write (AFTER reservation, BEFORE fill - Reserve-First pattern)
	// This prevents the "Spillover Bug" where a record written to WAL file N
	// lands in slab N+1 after rotation, causing data loss when WAL N is deleted.
	var walPos int64
	if mt.useWal() {
		result, err := mt.wal.Write(rec)
		if err != nil {
			active.pendingWrites.Add(-1)
			mt.ReportError(fmt.Errorf("wal write: %w", err))
			return err
		}
		walPos = result.Offset // Position in WAL file (becomes segment position after rename)
	}

	// 6. Write record directly to reserved region (zero-copy, outside lock)
	rec.EncodeTo(buf)

	// 7. Create SlabEntry for index lookup
	// Pos: slab buffer position (for Librarian reads before flush)
	// WalPos: WAL file position (for segment reads after WAL->segment rename)
	entry := SlabEntry{
		Header: rec.Header,
		Pos:    wPos,
		WalPos: walPos,
		XLBuf:  xlBuf,
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

		// B. Rotate WAL file and capture the fileID.
		// Must happen BEFORE sendToFlusher to avoid race with processFlush reading walFileID.
		// Writers are blocked on activeReady, so WAL writes are quiesced.
		if mt.wal != nil {
			closedFileID, err := mt.wal.EnqueueRotation()
			if err != nil {
				mt.ReportError(fmt.Errorf("wal rotate: %w", err))
			}
			old.walFileID = closedFileID
		}

		// C. Send OLD slab to flusher (acquires its own reference via PurchaseTicket)
		if shouldSend {
			mt.sendToFlusher(old)
		}

		// D. Release "Active Writer" reference.
		// The flusher has its own ref (via PurchaseTicket), and
		// the Librarian has its own ref (if enabled).
		old.buf.Unpin()

		// E. Allocate NEW slab (Blocking Operation)
		// This is done unlocked to prevent holding the mutex during allocation wait.
		newSlab := mt.newActiveSlab(0)

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

	for {
		select {
		case ticket, ok := <-mt.flushCh:
			if !ok {
				return
			}
			as := ticket.Active
			if err := mt.processFlush(as); err != nil {
				mt.ReportError(err)
			}
			ticket.Redeem()
			mt.flushWg.Done()
		case <-mt.stopCh:
			return
		}
	}
}

// processFlush finalizes a slab to a segment file.
// Dispatches to flushViaRename (WAL mode) or flushViaMerge (cache mode).
func (mt *MemTable) processFlush(as *ActiveSlab) error {
	if mt.IsDegraded() {
		return nil
	}
	if mt.Knobs != nil && mt.Knobs.InjectWriteErr != nil {
		if err := mt.Knobs.InjectWriteErr(); err != nil {
			return err
		}
	}

	useWalPos := mt.wal != nil && as.walFileID != 0
	if useWalPos {
		return mt.flushViaRename(as)
	}
	return mt.flushViaMerge(as)
}

// flushViaRename handles WAL mode: renames WAL file to segment, writes footer.
// Simple path with no XL buffer handling (WAL already contains all data).
func (mt *MemTable) flushViaRename(as *ActiveSlab) error {
	// 1. Collect entries using WalPos (actual position in WAL file)
	entries, maxSeqID := mt.collectEntries(as, true)

	// 2. Sort by position for linear I/O
	sortEntriesByPos(entries)

	// 3. Allocate segment ID
	segmentID := mt.segIDs.NextSegmentID()
	segmentPath := getSegmentPath(mt.Path, mt.Shards, segmentID)

	// 4. Rename WAL file to segment
	walPath := mt.wal.FilePath(as.walFileID)
	if err := os.Rename(walPath, segmentPath); err != nil {
		return fmt.Errorf("rename wal to segment: %w", err)
	}

	// 5. Finalize (index update + footer)
	return mt.finalizeFlush(segmentID, segmentPath, entries, maxSeqID)
}

// flushViaMerge handles cache mode: writes slab data with XL payloads interleaved.
// Complex path that merges XL buffers at page-aligned insertion points.
func (mt *MemTable) flushViaMerge(as *ActiveSlab) error {
	// 1. Collect entries using Pos (slab buffer position) and track XL writes
	entries, maxSeqID := mt.collectEntries(as, false)
	xlWrites, xlSeqIDs := collectXLWrites(as)
	defer releaseXLBuffers(xlWrites)

	// 2. Sort both by (Pos, SeqID) for linear I/O
	sortEntriesByPos(entries)
	sortXLWritesByPos(xlWrites)

	// 3. Adjust positions to account for XL buffer interleaving
	if len(xlWrites) > 0 {
		adjustFilePositionsForXLWrites(entries, xlWrites, xlSeqIDs)
	}

	// 4. Allocate segment ID and compute I/O flags
	segmentID := mt.segIDs.NextSegmentID()
	segmentPath := getSegmentPath(mt.Path, mt.Shards, segmentID)
	flags := mt.ioFlags()

	// 5. Write segment file (with or without XL interleaving)
	alignedData := as.buf.AlignedBytes(as.wPos)
	copy(alignedData[:record.FileHeaderSize], record.FileHeaderBytes[:])

	if len(xlWrites) > 0 {
		if err := writeSegmentWithXLPayloads(segmentPath, flags, alignedData, xlWrites); err != nil {
			return fmt.Errorf("writing segment with %d XL payloads: %w", len(xlWrites), err)
		}
	} else {
		if err := sys.WriteFile(segmentPath, alignedData, flags); err != nil {
			return fmt.Errorf("write segment: %w", err)
		}
	}

	// 6. Finalize (index update + footer)
	return mt.finalizeFlush(segmentID, segmentPath, entries, maxSeqID)
}

// collectEntries extracts footer entries from the slab index.
// If useWalPos is true, uses WalPos (for WAL mode); otherwise uses Pos (for cache mode).
func (mt *MemTable) collectEntries(as *ActiveSlab, useWalPos bool) ([]record.FooterEntry, uint64) {
	var entries []record.FooterEntry
	var maxSeqID uint64

	as.index.ForEach(func(key xmap.Key, e SlabEntry, _ *xmap.Pad32) bool {
		pos := e.Pos
		if useWalPos {
			pos = e.WalPos
		}
		entries = append(entries, record.FooterEntry{
			Key:          key,
			Pos:          pos,
			LogicalSize:  e.LogicalSize,
			PhysicalSize: e.PhysicalSize,
			SeqID:        e.SeqID,
			Flags:        e.Flags,
			KeyLen:       e.KeyLen,
		})
		if e.SeqID > maxSeqID {
			maxSeqID = e.SeqID
		}
		return true
	})

	return entries, maxSeqID
}

// collectXLWrites extracts XL buffer entries from the slab index.
func collectXLWrites(as *ActiveSlab) ([]SlabEntry, map[uint64]int) {
	var xlWrites []SlabEntry
	var xlSeqIDs map[uint64]int

	as.index.ForEach(func(_ xmap.Key, e SlabEntry, _ *xmap.Pad32) bool {
		if e.XLBuf != nil {
			if xlSeqIDs == nil {
				xlSeqIDs = make(map[uint64]int)
			}
			xlSeqIDs[e.SeqID] = len(xlWrites)
			xlWrites = append(xlWrites, e)
		}
		return true
	})

	return xlWrites, xlSeqIDs
}

func releaseXLBuffers(xlWrites []SlabEntry) {
	for _, xl := range xlWrites {
		xl.XLBuf.Unpin()
	}
}

func sortEntriesByPos(entries []record.FooterEntry) {
	slices.SortFunc(entries, func(a, b record.FooterEntry) int {
		if a.Pos != b.Pos {
			return int(a.Pos - b.Pos)
		}
		if a.SeqID < b.SeqID {
			return -1
		}
		if a.SeqID > b.SeqID {
			return 1
		}
		return 0
	})
}

func sortXLWritesByPos(xlWrites []SlabEntry) {
	slices.SortFunc(xlWrites, func(a, b SlabEntry) int {
		if a.Pos != b.Pos {
			return int(a.Pos - b.Pos)
		}
		if a.SeqID < b.SeqID {
			return -1
		}
		if a.SeqID > b.SeqID {
			return 1
		}
		return 0
	})
}

func (mt *MemTable) ioFlags() sys.OpenFlag {
	flags := sys.SyncNone
	if mt.IO.DirectIOWrite {
		flags |= sys.FlDirectIO
	}
	if mt.IO.FDataSync {
		flags |= sys.SyncData
	}
	return flags
}

// finalizeFlush updates the index and writes the footer file.
func (mt *MemTable) finalizeFlush(
	segmentID uint32, segmentPath string, entries []record.FooterEntry, maxSeqID uint64,
) error {
	if mt.Knobs != nil && mt.Knobs.InjectIndexErr != nil {
		if err := mt.Knobs.InjectIndexErr(); err != nil {
			return err
		}
	}

	// Build index items from footer entries
	items, err := footerEntriesToIndexItems(segmentID, entries)
	if err != nil {
		return err
	}

	// Update index
	if err := mt.PutBatch(segmentID, items, maxSeqID); err != nil {
		return fmt.Errorf("index update: %w", err)
	}

	// Write footer
	if err := WriteFooter(segmentID, entries, segmentPath, mt.footerPool, mt.ioFlags()); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}

	return nil
}

// writeSegmentWithXLPayloads writes alignedData into the segment, interleaving
// XL payloads at their insertion points. xlWrites must be sorted by (Pos, SeqID).
//
// XL buffers have FileHeaderSize bytes reserved at the start for potential file header.
// We write the full buffer (including the reserved bytes) because skipping them would
// break O_DIRECT alignment (the remaining data wouldn't start at a page boundary).
// The 8 bytes of "waste" per XL write is acceptable given the simplicity.
func writeSegmentWithXLPayloads(
	segmentPath string,
	flags sys.OpenFlag,
	alignedData []byte,
	xlWrites []SlabEntry,
) (retErr error) {
	var xlSize int64
	for _, w := range xlWrites {
		xlSize += int64(len(w.XLBuf.Bytes()))
	}
	f, err := sys.CreateAndAllocateFile(segmentPath, flags, int64(len(alignedData))+xlSize)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, sys.SyncFile(f, flags), f.Close())
	}()

	pos := 0
	for _, e := range xlWrites {
		if e.XLBuf == nil {
			return fmt.Errorf("unexpected nil XLBuf for xl write seqID=%d", e.SeqID)
		}
		// Write alignedData from current pos up to XL insertion point
		if int(e.Pos) > pos {
			buf := alignedData[pos:e.Pos]
			if _, err := sys.WriteAligned(buf, f, flags); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
		// Write full XL buffer (including reserved FileHeaderSize bytes for alignment)
		if _, err := sys.WriteAligned(e.XLBuf.Bytes(), f, flags); err != nil {
			return fmt.Errorf("xlwrite: %w", err)
		}
		pos = int(e.Pos)
	}
	// Write any remaining alignedData after last XL
	if len(alignedData[pos:]) > 0 {
		if _, err := sys.WriteAligned(alignedData[pos:], f, flags); err != nil {
			return fmt.Errorf("trailer write: %w", err)
		}
	}
	return nil
}

// adjustFilePositionsForXLWrites adjusts FooterEntry positions in cache mode to account
// for XL (extra large) buffers interleaved into the segment file.
//
// In cache mode, XL buffers are inserted at page-aligned positions, shifting all
// subsequent data. This function updates each entry's Pos to reflect its final
// file position after XL buffer insertion.
//
// XL buffer layout:
//   - Each XLBuf has FileHeaderSize (8 bytes) reserved at start
//   - Record data starts at offset FileHeaderSize within XLBuf
//   - Full XLBuf is written (including reserved bytes) for O_DIRECT page alignment
//
// Position calculation:
//   - XL record: Pos = original + cumulative XL sizes + FileHeaderSize
//   - Normal record: Pos = original + cumulative XL sizes
//
// Preconditions: entries and xlWrites must both be sorted by (Pos, SeqID).
func adjustFilePositionsForXLWrites(
	entries []record.FooterEntry,
	xlWrites []SlabEntry,
	xlSeqIDs map[uint64]int,
) {
	var cumulativeXLSize int64
	xlIdx := 0
	for i := range entries {
		entry := &entries[i]
		_, isXL := xlSeqIDs[entry.SeqID]

		if isXL {
			entry.Pos += cumulativeXLSize + record.FileHeaderSize
			cumulativeXLSize += int64(len(xlWrites[xlIdx].XLBuf.Bytes()))
			xlIdx++
		} else {
			entry.Pos += cumulativeXLSize
		}
	}
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

	rec.EncodeTo(buf)

	if rec.SeqID > active.currentMaxSeq {
		active.currentMaxSeq = rec.SeqID
	}

	active.index.Put(hash, SlabEntry{Header: rec.Header, Pos: wPos})
	return nil
}
