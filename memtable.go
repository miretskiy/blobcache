package blobcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
	"github.com/zhangyunhao116/skipmap"
)

// memFile represents a single memtable with skipmap and size tracking
type memFile struct {
	data *skipmap.StringMap[[]byte]
	size atomic.Int64
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
	blobWriter      BlobWriter
	writeBufferSize int64
}

// newMemTable creates a memtable with skipmap-based storage
func (c *Cache) newMemTable() *MemTable {
	// Choose blob writer based on config
	var writer BlobWriter
	if c.cfg.DirectIOWrites {
		writer = NewDirectIOWriter(c.cfg.Path, c.cfg.Shards)
	} else {
		writer = NewBufferedWriter(c.cfg.Path, c.cfg.Shards)
	}

	mt := &MemTable{
		flushCh:         make(chan *memFile, c.cfg.MaxInflightBatches),
		stopCh:          make(chan struct{}),
		cache:           c,
		blobWriter:      writer,
		writeBufferSize: c.cfg.WriteBufferSize,
	}
	mt.frozen.inflight = make([]*memFile, 0)
	mt.frozen.cond = sync.NewCond(&mt.frozen)

	// Initialize active memfile
	mf := &memFile{
		data: skipmap.NewString[[]byte](),
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

// Put stores key-value in memtable
// Caller must ensure key and value are not modified after this call
func (mt *MemTable) Put(key, value []byte) {
retry:
	mf := mt.active.Load()

	keyStr := unsafe.String(unsafe.SliceData(key), len(key))
	mf.data.Store(keyStr, value)

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
	// Add to frozen list before swapping active (maintains visibility)
	mt.frozen.inflight = append(mt.frozen.inflight, mf)

	// Swap to new active immediately - new writes go to new memfile
	newMf := &memFile{
		data: skipmap.NewString[[]byte](),
	}
	if old := mt.active.Swap(newMf); old != mf {
		panic(fmt.Errorf("active map changed under lock: expected %p found %p", mf, old))
	}

	// Wait for space in flush queue (after swap, so new writes don't block)
	// After swap, mf can never be active again, so no need to check after Wait()
	for len(mt.frozen.inflight) > mt.cache.cfg.MaxInflightBatches {
		mt.frozen.cond.Wait()
	}

	// Send to flush workers
	select {
	case mt.flushCh <- mf:
	default:
		panic("doRotateUnderLock: flushCh full despite backpressure")
	}
}

// Get retrieves value from active or frozen memfiles
func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	keyStr := unsafe.String(unsafe.SliceData(key), len(key))

	mf := mt.active.Load()
	if value, ok := mf.data.Load(keyStr); ok {
		return value, true
	}

	mt.frozen.RLock()
	defer mt.frozen.RUnlock()

	for i := len(mt.frozen.inflight) - 1; i >= 0; i-- {
		if value, ok := mt.frozen.inflight[i].data.Load(keyStr); ok {
			return value, true
		}
	}

	return nil, false
}

// flushWorker processes frozen memfiles - writes one file per blob
func (mt *MemTable) flushWorker(workerID int) {
	defer mt.wg.Done()

	for {
		select {
		case mf := <-mt.flushCh:
			mt.flushMemFile(mf)
			mt.removeFrozen(mf)
		case <-mt.stopCh:
			// Drain remaining memfiles
			for {
				select {
				case mf := <-mt.flushCh:
					mt.flushMemFile(mf)
					mt.removeFrozen(mf)
				default:
					return
				}
			}
		}
	}
}

func (mt *MemTable) flushMemFile(mf *memFile) {
	ctx := context.Background()
	now := time.Now().UnixNano()

	// Phase 1: Write all blob files and collect records for index
	var records []index.Record

	mf.data.Range(func(keyStr string, value []byte) bool {
		if len(value) == 0 {
			return true
		}

		key := unsafe.Slice(unsafe.StringData(keyStr), len(keyStr))
		k := base.NewKey(key, mt.cache.cfg.Shards)

		// Write blob via writer interface
		if err := mt.blobWriter.Write(k, value); err != nil {
			fmt.Printf("Warning: blob write failed for key %x: %v\n", key, err)
			return true // Continue with other entries
		}

		// Get position for index (segment mode returns segmentID+pos, per-blob returns zeros)
		pos := mt.blobWriter.Pos()

		records = append(records, index.Record{
			Key:       k,
			SegmentID: pos.SegmentID,
			Pos:       pos.Pos,
			Size:      len(value),
			CTime:     now,
		})
		return true
	})

	// Phase 2: Batch update index
	if len(records) == 0 {
		return
	}

	if err := mt.cache.index.PutBatch(ctx, records); err != nil {
		fmt.Printf("Warning: index batch insert failed: %v\n", err)
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
	mf := mt.active.Load()
	if mf.size.Load() > 0 {
		mt.doRotateUnderLock(mf)
	}

	for len(mt.frozen.inflight) > 0 {
		mt.frozen.cond.Wait()
	}
	mt.frozen.Unlock()
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
		data: skipmap.NewString[[]byte](),
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
