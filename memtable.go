package blobcache

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/zhangyunhao116/skipmap"
)

// memFile represents a single memtable with skipmap and size tracking
type memFile struct {
	buf     *MmapBuffer
	nextIdx atomic.Int64 // entries index reservation
	entries []metadata.BlobRecord
	data    *skipmap.Uint64Map[int] // key -> offset in entries
	done    chan struct{}           // Closed when flush completes
}

// MemTable provides async write buffering with in-memory read support
// Uses atomic memFile pointer for lock-free writes
type MemTable struct {
	config
	Batcher
	ErrorRporter // Callback to check/set degraded state

	// SegmentID sequence number.
	segmentID atomic.Int64

	// Set of memFiles currently being written or flushed.
	files struct {
		sync.RWMutex
		active   *memFile
		flushing []*memFile
		cond     *sync.Cond
	}

	seq        atomic.Int64
	slabPool   *MmapPool
	footerPool *MmapPool

	// Background flush workers
	flushCh chan *memFile
	stopCh  chan struct{}
	wg      sync.WaitGroup

	writeBufferSize int64
}

// NewMemTable creates a memtable with skipmap-based storage
func NewMemTable(cfg config, b Batcher, reporter ErrorRporter) *MemTable {
	mt := &MemTable{
		config:          cfg,
		Batcher:         b,
		ErrorRporter:    reporter,
		slabPool:        NewMmapPool(cfg.MaxInflightBatches, cfg.WriteBufferSize, cfg.LargeWriteThreshold),
		footerPool:      NewMmapPool(cfg.MaxInflightBatches, 256<<10, 0),
		flushCh:         make(chan *memFile, cfg.MaxInflightBatches),
		stopCh:          make(chan struct{}),
		writeBufferSize: cfg.WriteBufferSize,
	}
	mt.segmentID.Store(time.Now().UnixNano())

	// Initialize active memfile
	mt.files.cond = sync.NewCond(&mt.files)
	mt.files.active = mt.newMemFile()

	// Start I/O workers for flushing memfiles
	mt.wg.Add(cfg.FlushConcurrency)
	for i := 0; i < cfg.FlushConcurrency; i++ {
		go mt.flushWorker()
	}

	return mt
}

func (mt *MemTable) newMemFile() *memFile {
	// We assume a "minimum respectable blob size" for a 1GB table.
	// If we assume 8KB blobs, that's 128k entries.
	// RAM cost: 3MB.
	// Clamp it to 4K entries on the low end and 1M on the high end.
	capacity := min(1<<20, max(4096, mt.writeBufferSize/8192))

	return &memFile{
		buf:     mt.slabPool.Acquire(),
		entries: make([]metadata.BlobRecord, capacity),
		data:    skipmap.NewUint64[int](),
		done:    make(chan struct{}),
	}
}

// Put stores key-value in memtable (checksum will be computed during flush)
// Caller must ensure key and value are not modified after this call
func (mt *MemTable) Put(key Key, value []byte) {
	mt.putWithChecksum(key, value, nil)
}

// PutChecksummed stores key-value with an explicit checksum
// Caller must ensure key and value are not modified after this call
func (mt *MemTable) PutChecksummed(key Key, value []byte, checksum uint32) {
	mt.putWithChecksum(key, value, &checksum)
}

func makeEntry(
	key Key, offset int64, val []byte, hasher Hasher, checksum *uint32,
) metadata.BlobRecord {
	entry := metadata.BlobRecord{
		Hash:  uint64(key),
		Pos:   offset,
		Size:  int64(len(val)),
		Flags: metadata.InvalidChecksum,
	}
	if checksum != nil {
		// User provided explicit checksum
		entry.Flags = uint64(*checksum)
	} else if hasher != nil {
		h := hasher()
		h.Write(val)
		entry.Flags = uint64(h.Sum32())
	}
	return entry
}

// putWithChecksum is the internal implementation for Put and PutChecksummed
// checksum may be nil (will be computed during flush if ChecksumHasher configured)
func (mt *MemTable) putWithChecksum(key Key, value []byte, checksum *uint32) {
	if int64(len(value)) > mt.LargeWriteThreshold {
		mt.putLargeWithChecksum(key, value, checksum)
		return
	}

	// RETRY LOOP: Ensures the blob is never lost.
	// If the active slab is full (Metadata or Data), we rotate and try the new one.
	for {
		active, written, shouldRotate := mt.putActive(key, value, checksum)

		if written {
			if shouldRotate {
				mt.rotateActive(active)
			}
			return
		}

		// Slab was full. Initiate rotation and try again on the fresh slab.
		mt.rotateActive(active)
	}
}

func (mt *MemTable) rotateActive(active *memFile) {
	mt.files.Lock()
	defer mt.files.Unlock()

	if mt.files.active != active {
		// Somebody else won the race and rotated
		return
	}

	// Swap to new active & flush
	mt.files.active = mt.newMemFile()
	mt.flushUnderLock(active)
}

// putActive adds data to the active file, and returns active file and a boolean
// indicating if active should be rotated due to size.
// and returns that active file along with an indicator if active still has capacity.
func (mt *MemTable) putActive(
	key Key, value []byte, checksum *uint32,
) (
	_ *memFile, dataWritten bool, shouldRotate bool,
) {
	mt.files.RLock()
	defer mt.files.RUnlock()

	active := mt.files.active

	// 1. Reserve Metadata Index (Atomic)
	idx := active.nextIdx.Add(1) - 1

	// 2. Metadata Overflow Guard
	// If we run out of slots, we signal rotation immediately.
	if idx >= int64(len(active.entries)) {
		return active, false, true
	}

	// 3. Reserve Data Space (Atomic)
	offset, err := active.buf.Append(value)
	if err != nil {
		mt.ReportError(fmt.Errorf("memtable append failed: %w", err))
		return active, false, false
	}

	// 4. Populate Entry (Private to this thread)
	// No one can see this yet because it isn't in the skipmap.
	active.entries[idx] = makeEntry(key, offset, value, mt.Resilience.ChecksumHasher, checksum)

	// 5. PUBLISH to SkipMap (Lock-Free)
	// Now Get() can find the index and safely read the entry.
	active.data.Store(key, int(idx))

	// 6. Check for Rotation
	// Rotate if we hit the data limit OR if we are nearly out of metadata slots.
	shouldRotate = (offset+int64(len(value)) >= mt.writeBufferSize) ||
		(idx >= int64(len(active.entries)-1))

	return active, true, shouldRotate
}

func (mt *MemTable) putLargeWithChecksum(key Key, value []byte, checksum *uint32) {
	blob := &memFile{
		buf: mt.slabPool.AcquireUnpooled(int64(len(value))),
		// Pre-size to 1 and fill it immediately
		entries: make([]metadata.BlobRecord, 1),
		data:    skipmap.NewUint64[int](),
		done:    make(chan struct{}),
	}

	offset, _ := blob.buf.Append(value)
	blob.entries[0] = makeEntry(key, offset, value, mt.Resilience.ChecksumHasher, checksum)
	blob.nextIdx.Store(1) // Mark 1 entry as valid
	blob.data.Store(uint64(key), 0)

	mt.files.Lock()
	defer mt.files.Unlock()
	mt.flushUnderLock(blob)
}

// flushUnderLock rotates the given memfile out
// CALLER MUST HOLD mt.files.Lock()
func (mt *MemTable) flushUnderLock(mf *memFile) {
	// 1. Mark as Frozen: No more writes will ever happen to this buffer.
	mf.buf.isFrozen.Store(true)

	// 2. Add current memfile to files list (preserves most recent data)
	mt.files.flushing = append(mt.files.flushing, mf)

	// We do NOT Unpin here. The MemTable is "handing off" its
	// original pin to the background flusher.

	// Normal mode: wait for space before rotating
	for !mt.IsDegraded() && len(mt.files.flushing) >= mt.MaxInflightBatches {
		mt.files.cond.Wait()
	}

	// Degraded mode: drop the oldest if over capacity (AFTER adding current)
	if mt.IsDegraded() && len(mt.files.flushing) > mt.MaxInflightBatches {
		oldest := mt.files.flushing[0]
		mt.files.flushing = mt.files.flushing[1:]
		oldest.buf.Unpin()

		log.Warn("degraded mode: dropped oldest unflushed memtable",
			"size_bytes", oldest.buf.Len())

		// Note: Bloom filter not updated - will accumulate false positives
		// for dropped keys. Acceptable in degraded mode (causes extra disk lookups)
		mt.files.cond.Broadcast()
	}

	// Degraded mode: don't send to workers (they've stopped)
	// Memfile stays in files.flushing, will be dropped on next rotation if over capacity
	if mt.IsDegraded() {
		return
	}

	// Send to flush workers
	select {
	case mt.flushCh <- mf:
		// Sent successfully
	default:
		// Channel full - recheck if degraded or panic
		if !mt.IsDegraded() {
			panic("flushUnderLock: flushCh full despite backpressure")
		}
		// If degraded, workers just stopped - memfile stays in files.flushing
	}
}

// Get retrieves value from active or files memfiles
func (mt *MemTable) Get(key Key) (io.ReadCloser, bool) {
	mt.files.RLock()
	defer mt.files.RUnlock()

	// Check Active
	if mf := mt.files.active; mf != nil {
		if pos, ok := mf.data.Load(uint64(key)); ok {
			// SAFETY: Even though entries is size 128k, 'pos' is guaranteed
			// to be < nextIdx and fully initialized because of the skipmap Store.
			entry := mf.entries[pos]
			return mf.buf.NewSectionReader(entry.Pos, entry.Size), true
		}
	}

	// Check Flushing (Newest to Oldest)
	for i := len(mt.files.flushing) - 1; i >= 0; i-- {
		mf := mt.files.flushing[i]
		if pos, ok := mf.data.Load(uint64(key)); ok {
			entry := mf.entries[pos]
			return mf.buf.NewSectionReader(entry.Pos, entry.Size), true
		}
	}

	return nil, false
}

func (mt *MemTable) openSegment() (*SegmentWriter, error) {
	segmentID := mt.segmentID.Add(1)
	segmentPath := getSegmentPath(mt.Path, mt.Shards, segmentID)
	return NewSegmentWriter(segmentID, segmentPath, mt.SegmentSize, mt.footerPool)
}

// flushWorker processes files memfiles
func (mt *MemTable) flushWorker() {
	defer mt.wg.Done()

	// Create per-worker blob writer (shared across flushes for efficiency)
	// Segment files are much larger than memfiles (GBs vs MBs)
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
		case mf, ok := <-mt.flushCh:
			if !ok {
				return
			}

			// Execute the flush logic
			nextWriter, flushErr := mt.processFlush(mf, writer)

			if flushErr != nil {
				mt.ReportError(flushErr)
				// We do NOT return. We keep the loop running so we can
				// pull the next mf from flushCh and trigger its close(mf.done).
			}

			writer = nextWriter
		case <-mt.stopCh:
			// Stop immediately - caller must call Drain() before Close() if needed
			return
		}
	}
}

func (mt *MemTable) processFlush(
	mf *memFile, writer *SegmentWriter,
) (newWriter *SegmentWriter, fatal error) {
	// 1. Guaranteed signal and RAM cleanup
	defer func() {
		mt.removeFrozen(mf)
		mf.buf.Unpin()
		close(mf.done)
	}()

	// 2. Short-circuit if we are already in trouble
	if mt.IsDegraded() {
		return writer, nil
	}

	// 3. Physical Write
	if err := mt.flushMemFile(mf, writer); err != nil {
		return writer, err
	}

	// 4. Segment Rotation Logic
	if writer.CurrentPos() >= mt.SegmentSize {
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close segment failed: %w", err)
		}
		// Open next segment
		next, err := mt.openSegment()
		if err != nil {
			return nil, err
		}
		return next, nil
	}

	return writer, nil
}

func (mt *MemTable) flushMemFile(mf *memFile, writer *SegmentWriter) error {
	if mt.testingInjectWriteErr != nil {
		if err := mt.testingInjectWriteErr(); err != nil {
			mt.ReportError(err)
			return err
		}
	}

	// 1. Determine how many entries were actually written
	count := mf.nextIdx.Load()
	if count > int64(len(mf.entries)) {
		count = int64(len(mf.entries))
	}

	// Slice the pre-allocated records to the actual count.
	// These currently have POS = relative offset in slab.
	records := mf.entries[:count]

	// 2. Physical Write to NVMe (O_DIRECT)
	// IMPORTANT: writer.WriteSlab transforms records[i].Pos from RELATIVE to ABSOLUTE.
	if err := writer.WriteSlab(mf.buf.AlignedBytes(), records); err != nil {
		return fmt.Errorf("physical write failed: %w", err)
	}

	// Test fails here because this block is likely missing!
	if mt.testingInjectIndexErr != nil {
		if err := mt.testingInjectIndexErr(); err != nil {
			mt.ReportError(err)
			return err
		}
	}

	// 3. Update the Persistent Index (LSM-tree or SkipList)
	// This call makes the data visible to Get() requests globally.
	if err := mt.PutBatch(writer.id, records); err != nil {
		return fmt.Errorf("index update failed: %w", err)
	}

	return nil
}

// removeFrozen removes memfile from files list after flush
func (mt *MemTable) removeFrozen(target *memFile) {
	mt.files.Lock()
	defer mt.files.Unlock()

	for i, mf := range mt.files.flushing {
		if mf == target {
			// removeNode from slice while preserving order (standard Go idiom)
			mt.files.flushing = append(mt.files.flushing[:i], mt.files.flushing[i+1:]...)
			mt.files.cond.Broadcast()
			return
		}
	}
}

// Drain waits for all memfiles to be flushed
// In degraded mode, this is a no-op (workers stopped, nothing to drain)
func (mt *MemTable) Drain() {
	if mt.IsDegraded() {
		return
	}

	var signals []chan struct{}
	var toFlush *memFile

	// 1. Snapshot and Rotate under Lock
	mt.files.Lock()

	// Check if active needs to be moved to the pipeline
	if mt.files.active.buf.Len() > 0 {
		toFlush = mt.files.active
		mt.files.flushing = append(mt.files.flushing, toFlush)
		mt.files.active = mt.newMemFile() // Replace with fresh empty
	}

	// Capture all signals currently in the pipeline
	for _, f := range mt.files.flushing {
		signals = append(signals, f.done)
	}
	mt.files.Unlock()

	// 2. Perform Send outside of lock
	if toFlush != nil {
		// This ensures we don't hold the mutex if the worker queue is full
		mt.flushCh <- toFlush
	}

	// 3. The Universal Barrier
	// Even if toFlush was nil, we still wait for any previously
	// started flushes to complete.
	for _, sig := range signals {
		<-sig
	}
}

// Close shuts down workers and waits for completion
func (mt *MemTable) Close() {
	select {
	case <-mt.stopCh:
	default:
		close(mt.stopCh)
	}
	mt.wg.Wait()
}

// TestingClearMemtable clears active memfile
// ONLY for use in tests - this breaks the normal memtable contract
func (mt *MemTable) TestingClearMemtable() {
	mt.files.Lock()
	defer mt.files.Unlock()
	mt.files.active = mt.newMemFile()

	// Drain flush channel
	for {
		select {
		case <-mt.flushCh:
			// Drop any pending flushes
		default:
			return
		}
	}
}
