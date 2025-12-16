package blobcache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
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
	data *skipmap.Uint64Map[*memEntry]
	size atomic.Int64
}

// MemTable provides async write buffering with in-memory read support
// Uses atomic memFile pointer for lock-free writes
type MemTable struct {
	config
	batcher
	newWriter func() BlobWriter
	
	// Active memfile - currently accepting writes (lock-free)
	active atomic.Pointer[memFile]
	
	// Frozen coordination
	frozen struct {
		sync.RWMutex
		inflight []*memFile
		cond     *sync.Cond
	}
	
	seq atomic.Int64
	
	// Background flush workers
	flushCh chan *memFile
	stopCh  chan struct{}
	wg      sync.WaitGroup
	
	writeBufferSize int64
}

// newMemTable creates a memtable with skipmap-based storage
func (c *Cache) newMemTable(cfg config, storage *Storage) *MemTable {
	mt := &MemTable{
		config:  cfg,
		batcher: c,
		flushCh: make(chan *memFile, c.MaxInflightBatches),
		stopCh:  make(chan struct{}),
		newWriter: func() BlobWriter {
			return storage.NewSegmentWriter()
		},
		writeBufferSize: c.WriteBufferSize,
	}
	mt.frozen.cond = sync.NewCond(&mt.frozen)
	mt.seq.Store(time.Now().UnixNano())
	
	// Initialize active memfile
	mf := &memFile{
		data: skipmap.NewUint64[*memEntry](),
	}
	mt.active.Store(mf)
	
	// Start I/O workers for flushing memfiles
	mt.wg.Add(c.MaxInflightBatches)
	for i := 0; i < c.MaxInflightBatches; i++ {
		go mt.flushWorker()
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
// checksum may be nil (will be computed during flush if ChecksumHasher configured)
func (mt *MemTable) putWithChecksum(key Key, value []byte, checksum *uint32) {
retry:
	mf := mt.active.Load()
	
	entry := &memEntry{
		value: value,
	}
	if checksum != nil {
		entry.checksum = *checksum
		entry.hasChecksum = true
	}
	mf.data.Store(uint64(key), entry)
	
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
	for len(mt.frozen.inflight) >= mt.MaxInflightBatches {
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
		data: skipmap.NewUint64[*memEntry](),
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
	mf := mt.active.Load()
	if entry, ok := mf.data.Load(uint64(key)); ok {
		return entry.value, true
	}
	
	mt.frozen.RLock()
	defer mt.frozen.RUnlock()
	
	for i := len(mt.frozen.inflight) - 1; i >= 0; i-- {
		if entry, ok := mt.frozen.inflight[i].data.Load(uint64(key)); ok {
			return entry.value, true
		}
	}
	
	return nil, false
}

// flushWorker processes frozen memfiles
func (mt *MemTable) flushWorker() {
	defer mt.wg.Done()
	
	// Create per-worker blob writer
	writer := mt.newWriter()
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
	// Phase 1: Write all blob files and collect records for index
	var records []index.KeyValue
	
	mf.data.Range(func(key uint64, entry *memEntry) bool {
		value := entry.value
		if len(value) == 0 {
			return true
		}
		
		// Compute or use provided checksum
		checksum := metadata.InvalidChecksum
		
		if entry.hasChecksum {
			// User provided explicit checksum
			checksum = uint64(entry.checksum)
		} else if mt.Resilience.ChecksumHasher != nil {
			// Compute checksum using hash
			h := mt.Resilience.ChecksumHasher()
			h.Write(value)
			checksum = uint64(h.Sum32())
		}
		
		// Write blob via writer interface (pass checksum for footer)
		if err := writer.Write(Key(key), value, checksum); err != nil {
			fmt.Printf("Warning: blob write failed for key %x: %v\n", key, err)
			return true // Continue with other entries
		}
		
		// Get position for index
		pos := writer.Pos()
		
		records = append(records, index.KeyValue{
			Key: Key(key),
			Val: index.Value{
				SegmentID: pos.SegmentID,
				Pos:       pos.Pos,
				Size:      int64(len(value)),
				Checksum:  checksum,
			},
		})
		return true
	})
	
	// Phase 2: Batch update index
	if len(records) == 0 {
		return
	}
	
	if err := mt.PutBatch(records); err != nil {
		fmt.Printf("Warning: index batch insert failed: %v\n", err)
		return
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
		for len(mt.frozen.inflight) >= mt.MaxInflightBatches {
			mt.frozen.cond.Wait()
		}
		
		// Add to frozen
		mt.frozen.inflight = append(mt.frozen.inflight, mf)
		
		// Create empty active (drain context, no more writes expected)
		emptyMf := &memFile{data: skipmap.NewUint64[*memEntry]()}
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
	// Create new empty memfile
	newMf := &memFile{
		data: skipmap.NewUint64[*memEntry](),
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
