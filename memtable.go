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
	// state: [ Metadata Index (32b) | Data Offset (32b) ]
	// This packed word is our single "Point of Commitment" for both index and data reservation.
	buf     *MmapBuffer
	state   atomic.Uint64
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
		
		// active is the currently active memFile.
		// However, it may be temporarily unavailable (nil).  If that's the case,
		// caller must wait on activeReady until active is available.
		active      *memFile
		activeReady *sync.Cond
		
		// set of currently flushing files.
		flushing []*memFile
		
		_active struct {
			*sync.Cond
			*memFile
		}
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
	stopCh := make(chan struct{})
	mt := &MemTable{
		config:       cfg,
		Batcher:      b,
		ErrorRporter: reporter,
		// Pool size accounts for flushing slots + the active writer.
		slabPool: NewMmapPool(
			cfg.MaxInflightBatches+1, cfg.WriteBufferSize, cfg.LargeWriteThreshold,
		),
		footerPool:      NewMmapPool(cfg.MaxInflightBatches+1, 256<<10, 0),
		flushCh:         make(chan *memFile, cfg.MaxInflightBatches),
		stopCh:          stopCh,
		writeBufferSize: cfg.WriteBufferSize,
	}
	mt.segmentID.Store(time.Now().UnixNano())
	
	// Initialize active memfile
	mt.files.activeReady = sync.NewCond(&mt.files)
	mt.files.active = mt.newMemFile(true)
	
	// Start I/O workers for flushing memfiles
	mt.wg.Add(cfg.FlushConcurrency)
	for i := 0; i < cfg.FlushConcurrency; i++ {
		go mt.flushWorker()
	}
	
	return mt
}

func (mt *MemTable) newMemFile(pooled bool) *memFile {
	acquire := mt.slabPool.Acquire
	if !pooled {
		acquire = func() *MmapBuffer {
			return mt.slabPool.AcquireUnpooled(mt.WriteBufferSize)
		}
	}
	
	// Heuristic for metadata slot capacity based on average blob size.
	capacity := min(1<<20, max(4096, mt.writeBufferSize/8192))
	
	return &memFile{
		buf:     acquire(),
		entries: make([]metadata.BlobRecord, capacity),
		data:    skipmap.NewUint64[int](),
		done:    make(chan struct{}),
	}
}

// Put stores key-value in memtable
func (mt *MemTable) Put(key Key, value []byte) {
	mt.putWithChecksum(key, value, nil)
}

// PutChecksummed stores key-value with an explicit checksum
func (mt *MemTable) PutChecksummed(key Key, value []byte, checksum uint32) {
	mt.putWithChecksum(key, value, &checksum)
}

func makeEntry(
		key Key, offset int64, val []byte, hasher Hasher, checksum *uint32,
) metadata.BlobRecord {
	entry := metadata.BlobRecord{
		Hash:        uint64(key),
		Pos:         offset,
		LogicalSize: int64(len(val)), // Renamed from Size to LogicalSize
		Flags:       metadata.InvalidChecksum,
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
	if mt.files.active != active {
		mt.files.Unlock()
		return
	}
	
	// 1. Add (old) active to the flushing list while still locked.
	// This ensures Get() will find the data in the list even while active is nil.
	shouldSend := mt.prepareFlushUnderLock(active)
	
	// 2. Mark (new) active as unavailable until we acquire from the pool
	mt.files.active = nil
	mt.files.Unlock()
	
	// 3. Acquire ram.  May block, so executed w/out lock.
	newMF := mt.newMemFile(true)
	mt.files.activeReady.Broadcast() // Wake everybody Put() up.
	
	// 4. Logical commit.
	mt.files.Lock()
	mt.files.active = newMF
	mt.files.Unlock()
	
	// 5. Flush.
	if shouldSend {
		mt.sendToFlusher(active)
	}
}

// sendToFlusher must run without mt.files.Lock held.  It sends memfile
// to the flushers.
func (mt *MemTable) sendToFlusher(mf *memFile) {
	// NB: no "default:" case; If the channel is full, the worker blocks.
	// Since we are OUTSIDE mt.files.Lock, this is safe and provides backpressure.
	select {
	case mt.flushCh <- mf:
	case <-mt.stopCh:
	}
}

// prepareFlushUnderLock rotates the given memfile out and returns true if mf should
// be sent to flush woerks.
// mt.files.Lock() must be held
func (mt *MemTable) prepareFlushUnderLock(mf *memFile) bool {
	// Seal memtable:
	finalOff := int64(mf.state.Load() & 0xFFFFFFFF)
	mf.buf.Seal(finalOff)
	
	// Add current memfile to files list
	mt.files.flushing = append(mt.files.flushing, mf)
	
	if mt.IsDegraded() {
		// 1. Manage Capacity in Degraded Mode
		// If we are over the limit, drop the oldest data to bound memory
		if len(mt.files.flushing) > mt.MaxInflightBatches {
			oldest := mt.files.flushing[0]
			mt.files.flushing = mt.files.flushing[1:]
			oldest.buf.Unpin() // Return memory to pool (if hard pool) or munmap
			mt.files.activeReady.Broadcast()
		}
		return false
	}
	return true
}

// putActive adds data to the active file using a unified atomic reservation.
func (mt *MemTable) putActive(
		key Key, value []byte, checksum *uint32,
) (
		_ *memFile, dataWritten bool, shouldRotate bool,
) {
	// 1. Start with the RLock for the high-concurrency "Green Light" path.
	mt.files.RLock()
	active := mt.files.active
	
	if active == nil {
		// 2. GAP DETECTED: We must upgrade to a full Lock to use the Cond.
		mt.files.RUnlock()
		mt.files.Lock()
		for mt.files.active == nil {
			mt.files.activeReady.Wait()
		}
		// Capture the new active file while we still hold the exclusive Lock.
		active = mt.files.active
		
		// 3. STAY LOCKED: Instead of RLocking again, we just use the exclusive Lock
		// we already have to perform the CAS. This is 100% race-proof.
		defer mt.files.Unlock()
	} else {
		// Standard path: Release RLock when done.
		defer mt.files.RUnlock()
	}
	
	valLen := uint32(len(value))
	for {
		oldState := active.state.Load()
		oldOff := uint32(oldState & 0xFFFFFFFF)
		oldIdx := uint32(oldState >> 32)
		
		newOff := oldOff + valLen
		newIdx := oldIdx + 1
		
		// Bounds Check
		if newOff > uint32(active.buf.Cap()) || newIdx > uint32(len(active.entries)) {
			return active, false, true
		}
		
		newState := (uint64(newIdx) << 32) | uint64(newOff)
		if active.state.CompareAndSwap(oldState, newState) {
			active.buf.WriteAt(value, int64(oldOff))
			active.entries[oldIdx] = makeEntry(key, int64(oldOff), value, mt.Resilience.ChecksumHasher, checksum)
			active.data.Store(key, int(oldIdx))
			
			shouldRotate = (newOff >= uint32(mt.writeBufferSize)) ||
					(newIdx >= uint32(len(active.entries)-1))
			
			return active, true, shouldRotate
		}
	}
}

func (mt *MemTable) putLargeWithChecksum(key Key, value []byte, checksum *uint32) {
	blob := &memFile{
		buf: mt.slabPool.AcquireUnpooled(int64(len(value))),
		// Pre-size to 1 and fill it immediately
		entries: make([]metadata.BlobRecord, 1),
		data:    skipmap.NewUint64[int](),
		done:    make(chan struct{}),
	}
	
	// Use WriteAt and Seal for the unpooled buffer to follow the protocol.
	blob.buf.WriteAt(value, 0)
	blob.entries[0] = makeEntry(key, 0, value, mt.Resilience.ChecksumHasher, checksum)
	
	// Finalize state for the large write.
	blob.buf.Seal(int64(len(value)))
	blob.state.Store((1 << 32) | uint64(len(value)))
	blob.data.Store(uint64(key), 0)
	
	mt.files.Lock()
	shouldSend := mt.prepareFlushUnderLock(blob)
	mt.files.Unlock()
	if shouldSend {
		mt.sendToFlusher(blob)
	}
}

// Get retrieves value from active or flushing memfiles
func (mt *MemTable) Get(key Key) (io.ReadCloser, bool) {
	mt.files.RLock()
	defer mt.files.RUnlock()
	
	// Check Active
	if mf := mt.files.active; mf != nil {
		if pos, ok := mf.data.Load(uint64(key)); ok {
			entry := mf.entries[pos]
			return mf.buf.NewSectionReader(entry.Pos, entry.LogicalSize), true
		}
	}
	
	// Check Flushing (Newest to Oldest)
	for i := len(mt.files.flushing) - 1; i >= 0; i-- {
		mf := mt.files.flushing[i]
		if pos, ok := mf.data.Load(uint64(key)); ok {
			entry := mf.entries[pos]
			return mf.buf.NewSectionReader(entry.Pos, entry.LogicalSize), true
		}
	}
	
	return nil, false
}

func (mt *MemTable) openSegment() (*SegmentWriter, error) {
	segmentID := mt.segmentID.Add(1)
	segmentPath := getSegmentPath(mt.Path, mt.Shards, segmentID)
	return NewSegmentWriter(segmentID, segmentPath, mt.SegmentSize, mt.footerPool)
}

// flushWorker processes memfiles
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
		case mf, ok := <-mt.flushCh:
			if !ok {
				return
			}
			
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
	
	// Determine how many entries were actually written from bit-packed state.
	count := int64(mf.state.Load() >> 32)
	records := mf.entries[:count]
	
	// Physical Write to NVMe (O_DIRECT)
	// IMPORTANT: writer.WriteSlab transforms records[i].Pos from RELATIVE to ABSOLUTE.
	if err := writer.WriteSlab(mf.buf.AlignedBytes(), records); err != nil {
		return fmt.Errorf("physical write failed: %w", err)
	}
	
	if mt.testingInjectIndexErr != nil {
		if err := mt.testingInjectIndexErr(); err != nil {
			mt.ReportError(err)
			return err
		}
	}
	
	// Update the Persistent Index
	if err := mt.PutBatch(writer.id, records); err != nil {
		return fmt.Errorf("index update failed: %w", err)
	}
	
	return nil
}

// removeFrozen removes memfile from flushing list after flush
func (mt *MemTable) removeFrozen(target *memFile) {
	mt.files.Lock()
	defer mt.files.Unlock()
	
	for i, mf := range mt.files.flushing {
		if mf == target {
			mt.files.flushing = append(mt.files.flushing[:i], mt.files.flushing[i+1:]...)
			mt.files.activeReady.Broadcast()
			return
		}
	}
}

// Drain waits for all memfiles to be flushed
func (mt *MemTable) Drain() {
	if mt.IsDegraded() {
		return
	}
	
	// Phase 1: Resolve the current active file or gap
	mt.files.RLock()
	active := mt.files.active
	if active == nil {
		mt.files.RUnlock()
		mt.files.Lock()
		for mt.files.active == nil {
			mt.files.activeReady.Wait()
		}
		mt.files.Unlock()
	} else {
		hasData := (active.state.Load() & 0xFFFFFFFF) > 0
		mt.files.RUnlock()
		if hasData {
			mt.rotateActive(active)
		}
	}
	
	// Phase 2: Drain the queue until empty (Not just a snapshot)
	// This ensures no 'ghost' flushes are left behind when the benchmark ends.
	for {
		mt.files.Lock()
		if len(mt.files.flushing) == 0 {
			mt.files.Unlock()
			break
		}
		// Wait for the oldest one. Even if new ones are added,
		// we loop until the count is zero.
		done := mt.files.flushing[0].done
		mt.files.Unlock()
		select {
		case <-done:
		case <-mt.stopCh:
		}
		
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
