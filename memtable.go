package blobcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/miretskiy/blobcache/index"
	"github.com/zhangyunhao116/skipmap"
)

// memEntry holds value and optional user-provided checksum
type memEntry struct {
	value       []byte
	checksum    uint32 // User-provided checksum
	hasChecksum bool   // True if checksum was explicitly provided by user
}

// memFile represents a single memtable with skipmap and size tracking
type memFile struct {
	data *skipmap.StringMap[*memEntry]
	size atomic.Int64
}

// createBlobWriter creates appropriate writer based on config
func createBlobWriter(cfg config, workerID int) BlobWriter {
	// Use segments if SegmentSize > 0
	if cfg.SegmentSize > 0 {
		return NewSegmentWriter(cfg.Path, cfg.SegmentSize, cfg.DirectIOWrites, cfg.Fsync, workerID)
	}

	// Per-blob mode
	if cfg.DirectIOWrites {
		return NewDirectIOWriter(cfg.Path, cfg.Shards, cfg.Fsync)
	}
	return NewBufferedWriter(cfg.Path, cfg.Shards, cfg.Fsync)
}

// MemTable provides async write buffering with in-memory read support
// Uses atomic memFile pointer for lock-free writes
type MemTable struct {
	// Active memfile - currently accepting writes (lock-free)
	active atomic.Pointer[memFile]

	// Frozen coordination
	frozen struct {
		sync.RWMutex
		inflight []*memFile
		cond     *sync.Cond
	}

	// Background flush workers
	flushCh chan *memFile
	stopCh  chan struct{}
	wg      sync.WaitGroup

	cache           *Cache
	writeBufferSize int64
}

// newMemTable creates a memtable with skipmap-based storage
func (c *Cache) newMemTable() *MemTable {
	mt := &MemTable{
		flushCh:         make(chan *memFile, c.cfg.MaxInflightBatches),
		stopCh:          make(chan struct{}),
		cache:           c,
		writeBufferSize: c.cfg.WriteBufferSize,
	}
	mt.frozen.inflight = make([]*memFile, 0)
	mt.frozen.cond = sync.NewCond(&mt.frozen)

	// Initialize active memfile
	mf := &memFile{
		data: skipmap.NewString[*memEntry](),
	}
	mt.active.Store(mf)

	// Start I/O workers for flushing memfiles
	for i := 0; i < c.cfg.MaxInflightBatches; i++ {
		mt.wg.Add(1)
		workerID := i
		go mt.flushWorker(workerID)
	}

	return mt
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

// putWithChecksum is the internal implementation for Put and PutChecksummed
// checksum may be nil (will be computed during flush if ChecksumHash configured)
func (mt *MemTable) putWithChecksum(key Key, value []byte, checksum *uint32) {
retry:
	mf := mt.active.Load()

	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	entry := &memEntry{
		value: value,
	}
	if checksum != nil {
		entry.checksum = *checksum
		entry.hasChecksum = true
	}
	mf.data.Store(keyStr, entry)

	bloom := mt.cache.bloom.Load()
	bloom.Add(key)

	newSize := mf.size.Add(int64(len(value)))
	if newSize >= mt.writeBufferSize {
		mt.frozen.Lock()
		if mt.active.Load() != mf {
			mt.frozen.Unlock()
			goto retry
		}

		mt.doRotateUnderLock(mf)
		mt.frozen.Unlock()
	}
}

// doRotateUnderLock rotates the given memfile out
// CALLER MUST HOLD mt.frozen.Lock()
func (mt *MemTable) doRotateUnderLock(mf *memFile) {
	// Wait for space FIRST, before modifying any state
	for len(mt.frozen.inflight) >= mt.cache.cfg.MaxInflightBatches {
		mt.frozen.cond.Wait()
		// After Wait(), check if another thread already rotated this mf
		if mt.active.Load() != mf {
			return // Already rotated by another thread
		}
	}

	// Add to frozen list before swapping active (maintains visibility)
	mt.frozen.inflight = append(mt.frozen.inflight, mf)

	// Swap to new active
	newMf := &memFile{
		data: skipmap.NewString[*memEntry](),
	}
	if old := mt.active.Swap(newMf); old != mf {
		panic(fmt.Errorf("active map changed under lock: expected %p found %p", mf, old))
	}

	// Send to flush workers
	select {
	case mt.flushCh <- mf:
	default:
		panic("doRotateUnderLock: flushCh full despite backpressure")
	}
}

// Get retrieves value from active or frozen memfiles
func (mt *MemTable) Get(key Key) ([]byte, bool) {
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))

	mf := mt.active.Load()
	if entry, ok := mf.data.Load(keyStr); ok {
		return entry.value, true
	}

	mt.frozen.RLock()
	defer mt.frozen.RUnlock()

	for i := len(mt.frozen.inflight) - 1; i >= 0; i-- {
		if entry, ok := mt.frozen.inflight[i].data.Load(keyStr); ok {
			return entry.value, true
		}
	}

	return nil, false
}

// flushWorker processes frozen memfiles
func (mt *MemTable) flushWorker(workerID int) {
	defer mt.wg.Done()

	// Create per-worker blob writer
	writer := createBlobWriter(mt.cache.cfg, workerID)
	defer writer.Close()

	for {
		select {
		case mf := <-mt.flushCh:
			mt.flushMemFile(mf, writer)
			mt.removeFrozen(mf)
		case <-mt.stopCh:
			// Drain remaining memfiles
			for {
				select {
				case mf := <-mt.flushCh:
					mt.flushMemFile(mf, writer)
					mt.removeFrozen(mf)
				default:
					return
				}
			}
		}
	}
}

func (mt *MemTable) flushMemFile(mf *memFile, writer BlobWriter) {
	ctx := context.Background()
	now := time.Now().UnixNano()

	// Phase 1: Write all blob files and collect records for index
	var records []index.Record

	mf.data.Range(func(keyStr string, entry *memEntry) bool {
		value := entry.value
		if len(value) == 0 {
			return true
		}

		key := unsafe.Slice(unsafe.StringData(keyStr), len(keyStr))

		// Write blob via writer interface
		if err := writer.Write(key, value); err != nil {
			fmt.Printf("Warning: blob write failed for key %x: %v\n", key, err)
			return true // Continue with other entries
		}

		// Compute or use provided checksum
		var checksum uint32
		var hasChecksum bool

		if entry.hasChecksum {
			// User provided explicit checksum
			checksum = entry.checksum
			hasChecksum = true
		} else if mt.cache.cfg.ChecksumHash != nil {
			// Compute checksum using hash
			h := mt.cache.cfg.ChecksumHash()
			h.Write(value)
			checksum = h.Sum32()
			hasChecksum = true
		}

		// Get position for index (segment mode returns segmentID+pos, per-blob returns zeros)
		pos := writer.Pos()

		records = append(records, index.Record{
			Key:         key,
			SegmentID:   pos.SegmentID,
			Pos:         pos.Pos,
			Size:        len(value),
			CTime:       now,
			Checksum:    checksum,
			HasChecksum: hasChecksum,
		})
		return true
	})

	// Phase 2: Batch update index
	if len(records) == 0 {
		return
	}

	if err := mt.cache.index.PutBatch(ctx, records); err != nil {
		fmt.Printf("Warning: index batch insert failed: %v\n", err)
		return
	}

	// Phase 3: Update size tracking and trigger reactive eviction if needed
	var addedBytes int64
	for _, rec := range records {
		addedBytes += int64(rec.Size)
	}
	newSize := mt.cache.approxSize.Add(addedBytes)

	// Trigger eviction if over limit (reactive)
	if mt.cache.cfg.MaxSize > 0 && newSize > mt.cache.cfg.MaxSize {
		if err := mt.cache.runEviction(ctx); err != nil {
			fmt.Printf("Warning: reactive eviction failed: %v\n", err)
		}
	}
}

// removeFrozen removes memfile from frozen list after flush
func (mt *MemTable) removeFrozen(target *memFile) {
	mt.frozen.Lock()
	defer mt.frozen.Unlock()

	for i, mf := range mt.frozen.inflight {
		if mf == target {
			mt.frozen.inflight = append(mt.frozen.inflight[:i], mt.frozen.inflight[i+1:]...)
			mt.frozen.cond.Broadcast()
			return
		}
	}
}

// Drain waits for all memfiles to be flushed
func (mt *MemTable) Drain() {
	mt.frozen.Lock()
	defer mt.frozen.Unlock()

	// Send active directly if it has data (no need to rotate/create new active)
	mf := mt.active.Load()
	if mf.size.Load() > 0 {
		// Wait for space in inflight
		for len(mt.frozen.inflight) >= mt.cache.cfg.MaxInflightBatches {
			mt.frozen.cond.Wait()
		}

		// Add to frozen
		mt.frozen.inflight = append(mt.frozen.inflight, mf)

		// Create empty active (drain context, no more writes expected)
		emptyMf := &memFile{data: skipmap.NewString[*memEntry]()}
		mt.active.Store(emptyMf)

		// Send to flush
		mt.flushCh <- mf
	}

	// Wait for all inflight to be processed
	for len(mt.frozen.inflight) > 0 {
		mt.frozen.cond.Wait()
	}
}

// Close shuts down workers and waits for completion
func (mt *MemTable) Close() error {
	select {
	case <-mt.stopCh:
		return nil
	default:
		close(mt.stopCh)
	}
	mt.wg.Wait()
	return nil
}

// TestingClearMemtable clears active memfile
// ONLY for use in tests - this breaks the normal memtable contract
func (mt *MemTable) TestingClearMemtable() {
	// Create new empty memfile
	newMf := &memFile{
		data: skipmap.NewString[*memEntry](),
	}
	mt.active.Store(newMf)

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
