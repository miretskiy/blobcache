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
	index     *skipmap.Uint64Map[int] // key -> offset in entries
	flushDone chan struct{}           // Closed when flush completes

	// Atomic state to synchronize memFile rotation.
	pendingWrites atomic.Int64
	retired       atomic.Bool
	writesDone    *signal
}

type signal struct {
	sync.Once
	ch chan struct{}
}

func newSignal() *signal {
	return &signal{
		ch: make(chan struct{}),
	}
}

func (s *signal) Close() {
	s.Do(func() {
		close(s.ch)
	})
}

// MemTable provides async write buffering with in-memory read support.
type MemTable struct {
	config
	Batcher
	ErrorRporter

	segmentID atomic.Int64

	// ONE LOCK TO RULE THEM ALL
	mu struct {
		sync.RWMutex
		active      *memFile
		activeReady chan struct{}
		flushing    []*memFile
	}

	slabPool    *MmapPool
	footerPool  *MmapPool
	entriesPool sync.Pool

	flushCh chan *memFile
	stopCh  chan struct{}
	wg      sync.WaitGroup

	writeBufferSize int64
}

func NewMemTable(cfg config, b Batcher, reporter ErrorRporter) *MemTable {
	stopCh := make(chan struct{})
	mt := &MemTable{
		config:       cfg,
		Batcher:      b,
		ErrorRporter: reporter,
		// Pool size: MaxInflightBatches + 1 (Active)
		slabPool: NewMmapPool("slab", cfg.WriteBufferSize, cfg.LargeWriteThreshold,
			cfg.MaxInflightBatches+1),
		footerPool: NewMmapPool("footer", 256<<10, 0, cfg.MaxInflightBatches+1),
		entriesPool: sync.Pool{
			New: func() any {
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

	mt.mu.active = mt.newMemFile(0)

	mt.wg.Add(cfg.FlushConcurrency)
	for i := 0; i < cfg.FlushConcurrency; i++ {
		go mt.flushWorker()
	}

	return mt
}

// newMemFile creates a new memory table.
func (mt *MemTable) newMemFile(size int) *memFile {
	buf := func() *MmapBuffer {
		if size > 0 {
			return mt.slabPool.AcquireUnpooled(int64(size))
		}
		// DEADLOCK FIX: Bypass pool in degraded mode.
		// Since we don't send to flusher in degraded mode, these buffers stay
		// in the list until eviction.
		if mt.IsDegraded() {
			return mt.slabPool.AcquireUnpooled(mt.writeBufferSize)
		}
		return mt.slabPool.Acquire()
	}()

	pEntries := mt.entriesPool.Get().(*[]metadata.BlobRecord)
	buf.AddOnRelease(func() {
		mt.entriesPool.Put(pEntries)
	})

	mf := memFile{
		buf:        buf,
		entries:    (*pEntries)[:cap(*pEntries)],
		index:      skipmap.NewUint64[int](),
		flushDone:  make(chan struct{}),
		writesDone: newSignal(),
	}
	return &mf
}

func (mt *MemTable) Put(key Key, value []byte) {
	mt.putWithChecksum(key, value, nil)
}

func (mt *MemTable) PutChecksummed(key Key, value []byte, checksum uint32) {
	mt.putWithChecksum(key, value, &checksum)
}

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

func (mt *MemTable) View(key Key, fn func(data []byte)) bool {
	data, release, found := mt.ZeroCopyView(key)
	if !found {
		return false
	}
	defer release()
	fn(data)
	return true
}

func (mt *MemTable) ZeroCopyView(key Key) ([]byte, Releaser, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if mt.mu.active != nil {
		if data, ok := mt.lookupAndPin(mt.mu.active, key); ok {
			return data, mt.mu.active.buf.Unpin, true
		}
	}

	for i := len(mt.mu.flushing) - 1; i >= 0; i-- {
		mf := mt.mu.flushing[i]
		if data, ok := mt.lookupAndPin(mf, key); ok {
			return data, mf.buf.Unpin, true
		}
	}

	return nil, nil, false
}

func (mt *MemTable) lookupAndPin(mf *memFile, key Key) ([]byte, bool) {
	if pos, ok := mf.index.Load(key); ok {
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
		Hash:        key,
		Pos:         offset,
		LogicalSize: int64(len(val)),
		Flags:       metadata.InvalidChecksum,
	}
	if checksum != nil {
		entry.Flags = uint64(*checksum)
	} else if hasher != nil {
		h := hasher()
		h.Write(val)
		entry.Flags = uint64(h.Sum32())
	}
	return entry
}

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
	blob.index.Store(key, 0)

	mt.mu.Lock()
	// FIX: Capture shouldSend status
	shouldSend := mt.prepareFlushUnderLock(blob)
	mt.mu.Unlock()

	// FIX: Only send if not degraded
	if shouldSend {
		mt.sendToFlusher(blob)
	}
}

func (mt *MemTable) setActiveReadySemaphoreUnderLock() {
	mt.mu.activeReady = make(chan struct{})
}

func (mt *MemTable) putActive(key Key, value []byte, checksum *uint32) {
	mt.mu.Lock()

	if mt.mu.activeReady != nil {
		wait := mt.mu.activeReady
		mt.mu.Unlock()
		<-wait
		mt.putActive(key, value, checksum)
		return
	}

	if active := mt.mu.active; active.wPos+len(value) > active.buf.Cap() || active.nextEntry == len(active.entries) {
		mt.setActiveReadySemaphoreUnderLock()
		mt.mu.active = nil
		active.retired.Store(true)

		// FIX: Capture shouldSend status
		shouldSend := mt.prepareFlushUnderLock(active)

		waitCh := active.writesDone.ch
		hasPending := active.pendingWrites.Load() > 0
		mt.mu.Unlock()

		newMF := mt.newMemFile(0)

		mt.mu.Lock()
		mt.mu.active = newMF
		close(mt.mu.activeReady)
		mt.mu.activeReady = nil
		mt.mu.Unlock()

		if hasPending {
			<-waitCh
		}

		// FIX: Only send if not degraded
		if shouldSend {
			mt.sendToFlusher(active)
		}

		mt.putActive(key, value, checksum)
		return
	}

	active := mt.mu.active
	active.pendingWrites.Add(1)
	entryIdx := active.nextEntry
	active.nextEntry++
	wPos := active.wPos
	active.wPos += len(value)
	mt.mu.Unlock()

	active.buf.WriteAt(value, int64(wPos))
	active.entries[entryIdx] = makeEntry(
		key, int64(wPos), value, mt.Resilience.ChecksumHasher, checksum)
	active.index.Store(key, entryIdx)

	if active.pendingWrites.Add(-1) == 0 {
		if active.retired.Load() {
			active.writesDone.Close()
		}
	}
}

// prepareFlushUnderLock moves memfile to flushing list.
// Returns TRUE if the file should be sent to the flusher.
// Returns FALSE if we are in degraded mode (memory-only).
func (mt *MemTable) prepareFlushUnderLock(mf *memFile) bool {
	mf.buf.Seal(int64(mf.wPos))
	mt.mu.flushing = append(mt.mu.flushing, mf)

	if mt.IsDegraded() {
		// Manage Capacity in Degraded Mode
		if len(mt.mu.flushing) > mt.MaxInflightBatches {
			oldest := mt.mu.flushing[0]
			mt.mu.flushing = mt.mu.flushing[1:]
			// Eviction: Drop List Ref (#1).
			// Since we returned false below, there is no Worker Ref (#2).
			// This will correctly recycle the buffer.
			oldest.buf.Unpin()
		}
		// FIX: Do NOT send to flusher in degraded mode.
		// Keep in list (Ref=1) until evicted.
		return false
	}

	return true
}

func (mt *MemTable) sendToFlusher(mf *memFile) {
	// Add Worker Ref (#2)
	mf.buf.refCount.Add(1)

	select {
	case mt.flushCh <- mf:
	case <-mt.stopCh:
		mf.buf.Unpin()
	}
}

func (mt *MemTable) openSegment() (*SegmentWriter, error) {
	segmentID := mt.segmentID.Add(1)
	segmentPath := getSegmentPath(mt.Path, mt.Shards, segmentID)
	return NewSegmentWriter(segmentID, segmentPath, mt.SegmentSize, mt.footerPool, mt.IO.FDataSync)
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
		case mf, ok := <-mt.flushCh:
			if !ok {
				return
			}
			nextWriter, flushErr := mt.processFlush(mf, writer)
			if flushErr != nil {
				mt.ReportError(flushErr)
			}
			writer = nextWriter
		case <-mt.stopCh:
			return
		}
	}
}

func (mt *MemTable) processFlush(
	mf *memFile, writer *SegmentWriter,
) (newWriter *SegmentWriter, fatal error) {
	defer func() {
		// 1. Release List Ref (#1)
		mt.removeFrozen(mf)
		// 2. Release Worker Ref (#2)
		mf.buf.Unpin()
		close(mf.flushDone)
	}()

	if mt.IsDegraded() {
		return writer, nil
	}

	if err := mt.flushMemFile(mf, writer); err != nil {
		return writer, err
	}

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
	if err := mt.PutBatch(writer.id, absoluteRecords); err != nil {
		return fmt.Errorf("index update failed: %w", err)
	}
	return nil
}

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
	// Release List Ref (#1)
	if found {
		target.buf.Unpin()
	}
}

func (mt *MemTable) Drain() {
	if mt.IsDegraded() {
		return
	}

	mt.mu.Lock()
	active := mt.mu.active
	var waitCh chan struct{}

	shouldFlush := active != nil && active.wPos > 0

	// FIX: Capture result of prepareFlush
	shouldSend := false
	if shouldFlush {
		shouldSend = mt.prepareFlushUnderLock(active)

		mt.mu.active = nil
		active.retired.Store(true)

		if active.pendingWrites.Load() > 0 {
			waitCh = active.writesDone.ch
		}
		if mt.mu.activeReady != nil {
			close(mt.mu.activeReady)
			mt.mu.activeReady = nil
		}
	}
	mt.mu.Unlock()

	if waitCh != nil {
		<-waitCh
	}
	// FIX: Use captured result
	if shouldSend {
		mt.sendToFlusher(active)
	}

	for {
		mt.mu.Lock()
		if len(mt.mu.flushing) == 0 {
			mt.mu.Unlock()
			break
		}
		done := mt.mu.flushing[0].flushDone
		mt.mu.Unlock()
		select {
		case <-done:
		case <-mt.stopCh:
		}
	}

	newActive := mt.newMemFile(0)
	mt.mu.Lock()
	mt.mu.active = newActive
	mt.mu.Unlock()
}

func (mt *MemTable) Close() {
	select {
	case <-mt.stopCh:
	default:
		close(mt.stopCh)
	}
	mt.wg.Wait()
}
