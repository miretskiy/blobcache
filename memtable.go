package blobcache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/zhangyunhao116/skipmap"
)

// memFile represents a single memtable with skipmap and size tracking
type memFile struct {
	wPos      int // write position in the buffer.
	buf       *MmapBuffer
	nextEntry int
	entries   []metadata.BlobRecord
	data      *skipmap.Uint64Map[int] // key -> offset in entries
	flushDone chan struct{}           // Closed when flush completes

	// Atomic state to synchronize memFile rotation.
	// the "rotator" -- Go routine that decides it's time to rotate
	// wil set memtable active pointer to nil (under lock), and mark memfile retired.
	// Rotator will wait on writesDone if pending writes > 0.
	// Whichever thread decrements pendingWrites to 0 while retire is true closes writesDone.
	pendingWrites atomic.Int64
	retired       atomic.Bool
	writesDone    signal
}

type signal struct {
	sync.Once
	ch chan struct{}
}

func (s *signal) Close() {
	s.Do(func() {
		close(s.ch)
	})
}

type snapshot struct {
	// files is ordered: oldest -> newest.
	files []*memFile
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
	mu struct {
		// Mutex protects access to active and flushing slices;
		// However, exclusive lock is minimized to update internal state
		// (rotate active for example) and for some bookkeeping tasks.
		// The actual Write (in memory) is done w/out lock.
		sync.Mutex

		// active is the currently active memFile.
		// However, it may be temporarily unavailable (nil).  If that's the case,
		// caller must wait on activeReady until active is available.
		active      *memFile
		activeReady *sync.Cond

		// set of currently flushing memfiles.
		flushing []*memFile
	}

	// Read state snapshot.
	readState atomic.Pointer[snapshot]

	slabPool    *MmapPool
	footerPool  *MmapPool
	entriesPool sync.Pool // amortize []blobrecord.Entry allocations.

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
		slabPool: NewMmapPool("slab", cfg.WriteBufferSize, cfg.LargeWriteThreshold,
			cfg.MaxInflightBatches+1),
		footerPool: NewMmapPool("footer", 256<<10, 0, cfg.MaxInflightBatches+1),
		entriesPool: sync.Pool{
			New: func() any {
				// Only allocate the backing array once
				capacity := min(1<<20, max(4096, cfg.WriteBufferSize/8192))
				s := make([]metadata.BlobRecord, capacity)
				return &s
			},
		},
		flushCh:         make(chan *memFile, cfg.MaxInflightBatches),
		stopCh:          stopCh,
		writeBufferSize: cfg.WriteBufferSize,
	}
	mt.segmentID.Store(time.Now().UnixNano())

	// Initialize active memfile
	mt.mu.Lock()
	mt.mu.activeReady = sync.NewCond(&mt.mu)
	mt.mu.active = mt.newMemFile(0 /* pooled */)
	mt.publishSnapshot()
	mt.mu.Unlock()

	// Start I/O workers for flushing memfiles
	mt.wg.Add(cfg.FlushConcurrency)
	for i := 0; i < cfg.FlushConcurrency; i++ {
		go mt.flushWorker()
	}

	return mt
}

func (mt *MemTable) newMemFile(size int) *memFile {
	buf := func() *MmapBuffer {
		if size == 0 {
			return mt.slabPool.Acquire()
		} else {
			return mt.slabPool.AcquireUnpooled(int64(size))
		}
	}()

	pEntries := mt.entriesPool.Get().(*[]metadata.BlobRecord)
	buf.AddOnRelease(func() {
		mt.entriesPool.Put(pEntries)
	})

	mf := memFile{
		buf:       buf,
		entries:   (*pEntries)[:cap(*pEntries)],
		data:      skipmap.NewUint64[int](),
		flushDone: make(chan struct{}),
	}
	mf.writesDone.ch = make(chan struct{})
	return &mf
}

// Put stores key-value in memtable
func (mt *MemTable) Put(key Key, value []byte) {
	mt.putWithChecksum(key, value, nil)
}

// PutChecksummed stores key-value with an explicit checksum
func (mt *MemTable) PutChecksummed(key Key, value []byte, checksum uint32) {
	mt.putWithChecksum(key, value, &checksum)
}

// Get copies the value into dst, growing/allocating if necessary.
// This is the safest way to "capture" the data for later use.
func (mt *MemTable) Get(key Key, dst []byte) ([]byte, bool) {
	found := mt.View(key, func(data []byte) {
		if cap(dst) < len(data) {
			dst = make([]byte, len(data))
		} else {
			dst = dst[:len(data)]
		}
		copy(dst, data)
	})
	return dst, found
}

// View provides scoped, zero-copy access to a value.
// data passed to the function is valid for the duration of this function ONLY.
func (mt *MemTable) View(key Key, fn func(data []byte)) bool {
	data, release, found := mt.ZeroCopyView(key)
	if !found {
		return false
	}
	defer release()
	fn(data)
	return true
}

// ZeroCopyView returns a raw pointer to memory. Expert use only.
// The caller must not stash the returned byte value, and the release function
// must be called expediently. Otherwise, the memtable may stall.
func (mt *MemTable) ZeroCopyView(key Key) ([]byte, Releaser, bool) {
	s := mt.readState.Load()

	// Iterate newest->oldest
	for i := len(s.files) - 1; i >= 0; i-- {
		mf := s.files[i]

		if data, ok := mt.lookupAndPin(mf, key); ok {
			return data, mf.buf.Unpin, true
		}
	}

	return nil, nil, false
}

func (mt *MemTable) lookupAndPin(mf *memFile, key Key) ([]byte, bool) {
	// Skipmap Load is lock-free and thread-safe.
	if pos, ok := mf.data.Load(uint64(key)); ok {
		mf.buf.refCount.Add(1)
		entry := mf.entries[pos]
		return mf.buf.raw[entry.Pos : entry.Pos+entry.LogicalSize], true
	}
	return nil, false
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
	} else {
		mt.putActive(key, value, checksum)
	}
}

func (mt *MemTable) putLargeWithChecksum(key Key, value []byte, checksum *uint32) {
	blob := mt.newMemFile(len(value))
	blob.buf.WriteAt(value, 0)
	blob.entries[0] = makeEntry(key, 0, value, mt.Resilience.ChecksumHasher, checksum)
	blob.nextEntry = 1
	blob.wPos = len(value)
	blob.data.Store(key, 0)

	mt.mu.Lock()
	shouldSend := mt.prepareFlushUnderLock(blob)
	mt.publishSnapshot() // Make the blob visible to readers
	mt.mu.Unlock()
	if shouldSend {
		mt.sendToFlusher(blob)
	}
}

// putActive adds data to the active file using a unified atomic reservation.
func (mt *MemTable) putActive(key Key, value []byte, checksum *uint32) {
	mt.mu.Lock()

	for mt.mu.active == nil {
		mt.mu.activeReady.Wait()
	}
	active := mt.mu.active
	wPos := active.wPos
	entryIdx := active.nextEntry

	// 1. ROTATION BRANCH
	if wPos+len(value) > active.buf.Cap() || entryIdx == len(active.entries) {
		// SET RETIRED UNDER LOCK: This ensures no writer finishing after this
		// point can miss the fact that the slab is retired.
		mt.mu.active = nil
		active.retired.Store(true)
		shouldSend := mt.prepareFlushUnderLock(active)

		waitCh := active.writesDone.ch
		// DOUBLE CHECK: If writes hit 0 before we set retired or during this block,
		// we must close the channel ourselves.
		if active.pendingWrites.Load() == 0 {
			active.writesDone.Close()
		}
		mt.mu.Unlock()

		// Allocate new slab
		newMF := mt.newMemFile(0 /* pooled */)

		mt.mu.Lock()
		mt.mu.active = newMF
		mt.mu.activeReady.Broadcast()
		mt.publishSnapshot()
		mt.mu.Unlock()

		// THE BLOCKING POINT: Now safe because the "Last Man Out" check is foolproof.
		if waitCh != nil {
			<-waitCh
		}

		if shouldSend {
			mt.sendToFlusher(active)
		}

		// Use recursion to handle the write on the new slab.
		// IMPORTANT: Do NOT manually store 1 in pendingWrites before this.
		mt.putActive(key, value, checksum)
		return
	} else {
		// 2. SUCCESS PATH: Atomic Reservation
		active.pendingWrites.Add(1)
		active.wPos += len(value)
		active.nextEntry++
		mt.mu.Unlock()
	}

	// PARALLEL WRITE
	active.buf.WriteAt(value, int64(wPos))
	active.entries[entryIdx] = makeEntry(
		key, int64(wPos), value, mt.Resilience.ChecksumHasher, checksum)
	active.data.Store(uint64(key), entryIdx)

	// 3. CHECK-OUT: The "Last Man Out" logic
	// We decrement first, then check if we are the closer.
	if active.pendingWrites.Add(-1) == 0 {
		if active.retired.Load() {
			active.writesDone.Close()
		}
	}
}

// prepareFlushUnderLock rotates the given memfile out and returns true if mf should
// be sent to flush workers.
// mt.mu.Lock() must be held
func (mt *MemTable) prepareFlushUnderLock(mf *memFile) bool {
	mf.buf.Seal(int64(mf.wPos))

	// Rotate active to flushing; mark active nil.
	// It will remain nil until memory can be acquired to create new active.
	mt.mu.flushing = append(mt.mu.flushing, mf)
	if mt.IsDegraded() {
		// Manage Capacity in Degraded Mode
		if len(mt.mu.flushing) > mt.MaxInflightBatches {
			oldest := mt.mu.flushing[0]
			mt.mu.flushing = mt.mu.flushing[1:]
			oldest.buf.Unpin()
		}
		// Always publish after modifying flushing list so readers see new data
		mt.publishSnapshot()
		return false
	}
	return true
}

// sendToFlusher must run without mt.mu held. It sends memfile to the flushers.
func (mt *MemTable) sendToFlusher(mf *memFile) {
	// NB: no "default:" case; If the channel is full, the worker blocks.
	// Since we are OUTSIDE the mutex, this is safe and provides backpressure.
	select {
	case mt.flushCh <- mf:
	case <-mt.stopCh:
	}
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
				// pull the next mf from flushCh and trigger its close(mf.flushDone).
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
		close(mf.flushDone)
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

	records := mf.entries[:mf.nextEntry]

	// Physical Write to NVMe (O_DIRECT)
	// WriteSlab returns records with Pos transformed from RELATIVE to ABSOLUTE
	absoluteRecords, err := writer.WriteSlab(mf.buf.AlignedBytes(), records)
	if err != nil {
		return fmt.Errorf("physical write failed: %w", err)
	}

	if mt.testingInjectIndexErr != nil {
		if err := mt.testingInjectIndexErr(); err != nil {
			mt.ReportError(err)
			return err
		}
	}

	// Update the Persistent Index with ABSOLUTE positions
	if err := mt.PutBatch(writer.id, absoluteRecords); err != nil {
		return fmt.Errorf("index update failed: %w", err)
	}

	return nil
}

// removeFrozen removes memfile from flushing list after flush
func (mt *MemTable) removeFrozen(target *memFile) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	found := false
	for i, mf := range mt.mu.flushing {
		if mf == target {
			mt.mu.flushing = append(mt.mu.flushing[:i], mt.mu.flushing[i+1:]...)
			found = true
			break
		}
	}

	if found {
		// Update the lock-free snapshot for readers.
		// Since we are still under mt.mu.Lock(), and mf.buf.Unpin()
		// hasn't been called by the caller yet, this is 100% safe.
		mt.publishSnapshot()
	}
}

// Drain waits for all memfiles to be flushed
func (mt *MemTable) Drain() {
	if mt.IsDegraded() {
		return
	}

	mt.mu.Lock()
	active := mt.mu.active
	var waitCh chan struct{}
	shouldFlush := active != nil && active.wPos > 0 && mt.prepareFlushUnderLock(active)
	if active != nil {
		if active.pendingWrites.Load() > 0 {
			waitCh = active.writesDone.ch
		}
		active.retired.Store(true)
		mt.mu.active = nil
	}
	mt.mu.Unlock() // mt.mu.active remains nil until we're done draining

	if waitCh != nil {
		<-waitCh
	}
	if shouldFlush {
		mt.sendToFlusher(active)
	}
	// Phase 2: Drain the queue until empty (Not just a snapshot)
	// This ensures no 'ghost' flushes are left behind when the benchmark ends.
	for {
		mt.mu.Lock()
		if len(mt.mu.flushing) == 0 {
			mt.mu.Unlock()
			break
		}
		// Wait for the oldest one. Even if new ones are added,
		// we loop until the count is zero.
		done := mt.mu.flushing[0].flushDone
		mt.mu.Unlock()
		select {
		case <-done:
		case <-mt.stopCh:
		}
	}

	newActive := mt.newMemFile(0 /* pooled */)
	mt.mu.Lock()
	mt.mu.active = newActive
	mt.mu.activeReady.Broadcast()
	mt.publishSnapshot() // Make new active visible to readers
	mt.mu.Unlock()
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

// mu.Lock must be held
func (mt *MemTable) publishSnapshot() {
	// 1. Prepare the new snapshot
	count := len(mt.mu.flushing)
	if mt.mu.active != nil {
		count++
	}
	files := make([]*memFile, 0, count)
	files = append(files, mt.mu.flushing...)
	if mt.mu.active != nil {
		files = append(files, mt.mu.active)
	}
	newSnap := &snapshot{files: files}

	// 2. PIN everything in the new snapshot
	for _, mf := range newSnap.files {
		mf.buf.refCount.Add(1)
	}

	// 3. SWAP out the old snapshot
	oldSnap := mt.readState.Swap(newSnap)

	// 4. UNPIN everything from the old snapshot
	if oldSnap != nil {
		for _, mf := range oldSnap.files {
			mf.buf.Unpin()
		}
	}
}
