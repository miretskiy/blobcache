package blobcache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/miretskiy/blobcache/base"
	"github.com/ncw/directio"
	"github.com/zhangyunhao116/skipmap"
)

// Metrics counters (TODO: replace with proper metrics system)
var (
	metricsAlignedWrites   atomic.Int64
	metricsUnalignedWrites atomic.Int64
)

// isAligned checks if slice is aligned to BlockSize
func isAligned(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	return uintptr(unsafe.Pointer(&b[0]))%uintptr(directio.BlockSize) == 0
}

// writeAlignedBlocks writes aligned blocks to file, returns unaligned remainder
func writeAlignedBlocks(f *os.File, buf []byte) ([]byte, error) {
	alignedLen := (len(buf) / directio.BlockSize) * directio.BlockSize
	if alignedLen > 0 {
		if _, err := f.Write(buf[:alignedLen]); err != nil {
			return nil, err
		}
	}
	return buf[alignedLen:], nil
}

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
func (mt *MemTable) Put(key, value []byte) error {
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

	return nil
}

// doRotateUnderLock rotates the given memfile out
// CALLER MUST HOLD mt.frozen.Lock()
func (mt *MemTable) doRotateUnderLock(mf *memFile) {
	for len(mt.frozen.inflight) >= mt.cache.cfg.MaxInflightBatches {
		mt.frozen.cond.Wait()
	}

	// Add to frozen list before swapping active (maintains visibility)
	mt.frozen.inflight = append(mt.frozen.inflight, mf)

	newMf := &memFile{
		data: skipmap.NewString[[]byte](),
	}
	mt.active.Store(newMf)

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

// segmentFile tracks an open segment file and its metadata
type segmentFile struct {
	file *os.File
	id   string
	size int64
}

// Close closes the underlying file if open
func (sf *segmentFile) Close() {
	if sf != nil && sf.file != nil {
		_ = sf.file.Close()
		sf.file = nil
	}
}

func newSegmentFile(basePath string, workerID int) (*segmentFile, error) {
	// Generate new segment ID and path
	now := time.Now().UnixNano()
	segmentID := fmt.Sprintf("%d-%02d", now, workerID)
	segmentPath := filepath.Join(basePath, "segments", segmentID+".seg")

	// Create new segment file with DirectIO (O_APPEND for sequential writes)
	file, err := directio.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &segmentFile{
		file: file,
		id:   segmentID,
		size: st.Size(),
	}, nil
}

// flushWorker processes frozen memfiles
// Manages segment file lifecycle - keeps file open across multiple memfile flushes
// until segment reaches SegmentSize threshold
func (mt *MemTable) flushWorker(workerID int) {
	defer mt.wg.Done()
	ctx := context.Background()

	var segment *segmentFile
	defer segment.Close()

	// openSegment returns current segment, opening new one if needed or if size exceeded
	openSegment := func() (*segmentFile, error) {
		// Rotate if no file or size exceeded
		if segment == nil || segment.size >= mt.cache.cfg.SegmentSize {
			segment.Close() // safe to close nil segment
			seg, err := newSegmentFile(mt.cache.cfg.Path, workerID)
			if err != nil {
				return nil, err
			}
			segment = seg
		}
		return segment, nil
	}

	// recordWrite handles flush result - updates segment size or closes failed segment
	flushMemFile := func(mf *memFile) error {
		seg, err := openSegment()
		if err != nil {
			return err
		}
		numWritten, err := mt.flushMemFileDataLargeWrites(ctx, seg, mf)
		if err != nil {
			return err
		}
		seg.size += numWritten
		return nil
	}

	for {
		select {
		case mf := <-mt.flushCh:
			if err := flushMemFile(mf); err != nil {
				fmt.Printf("Error: error flushing memfile: %v\n", err)
				segment.Close()
			}
			mt.removeFrozen(mf)
		case <-mt.stopCh:
			// Drain remaining memfiles
			for {
				select {
				case mf := <-mt.flushCh:
					if err := flushMemFile(mf); err != nil {
						fmt.Printf("Error: error flushing memfile: %v\n", err)
						segment.Close()
					}
					mt.removeFrozen(mf)
				default:
					return
				}
			}
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

const dioWriteSize = 64 << 20

var alignedBufferPool = sync.Pool{
	New: func() any { return directio.AlignedBlock(dioWriteSize) },
}

func (mt *MemTable) flushMemFileDataLargeWrites(
	ctx context.Context, segment *segmentFile, mf *memFile,
) (int64, error) {
	if segment == nil || segment.file == nil {
		return 0, fmt.Errorf("segment is nil")
	}

	filePos := segment.size
	var blobMetas []blobMeta
	alignedBuffer := bytes.NewBuffer((alignedBufferPool.Get().([]byte))[:0])
	defer alignedBufferPool.Put(alignedBuffer.Bytes())

	putBuf := func(b []byte) error {
		for len(b) > 0 {
			l := min(len(b), alignedBuffer.Available())
			alignedBuffer.Write(b[:l])
			b = b[l:]
			if alignedBuffer.Available() == 0 {
				if _, err := segment.file.Write(alignedBuffer.Bytes()); err != nil {
					return err
				}
				alignedBuffer.Reset()
			}
		}
		return nil
	}

	mf.data.Range(func(keyStr string, value []byte) bool {
		if len(value) == 0 {
			return true
		}
		if err := putBuf(value); err != nil {
			fmt.Printf("Error: error putting value to buffer: %v\n", err)
			return false
		}
		key := unsafe.Slice(unsafe.StringData(keyStr), len(keyStr))
		blobMetas = append(blobMetas, blobMeta{key: key, pos: filePos, size: len(value)})
		filePos += int64(len(value))
		return true
	})

	// Write final leftover with padding
	if alignedBuffer.Len() > 0 {
		// Round up to block size using bit twiddling (buffer is already large enough)
		currentLen := alignedBuffer.Len()
		const mask = directio.BlockSize - 1
		paddedSize := (currentLen + mask) &^ mask
		if _, err := segment.file.Write(alignedBuffer.Bytes()[:paddedSize]); err != nil {
			return 0, err
		}
		filePos += int64(paddedSize - currentLen) // Account for padding bytes
	}

	// Calculate total bytes written
	bytesWritten := filePos - segment.size

	// Update index with blob metadata
	now := time.Now().UnixNano()
	mt.writeMemFileIndex(ctx, segment.id, now, blobMetas)
	return bytesWritten, nil
}

// flushMemFileData writes memfile data to an existing segment file and updates index
// Returns number of bytes written (for segment size tracking) and error
func (mt *MemTable) flushMemFileData(
	ctx context.Context, segment *segmentFile, mf *memFile,
) (int64, error) {
	if segment == nil || segment.file == nil {
		return 0, fmt.Errorf("segment is nil")
	}

	filePos := segment.size
	var blobMetas []blobMeta
	var leftover []byte
	var scratchBuf []byte

	mf.data.Range(func(keyStr string, value []byte) bool {
		if len(value) == 0 {
			return true
		}
		key := unsafe.Slice(unsafe.StringData(keyStr), len(keyStr))

		// Prepare buffer to write (use value directly if aligned, else scratch buffer)
		var toWrite []byte
		blobPosInFile := filePos

		// Use value directly if aligned, else copy to scratch buffer
		if len(leftover) == 0 && isAligned(value) && len(value)%directio.BlockSize == 0 {
			metricsAlignedWrites.Add(1)
			toWrite = value
		} else {
			// Need scratch buffer
			metricsUnalignedWrites.Add(1)
			needed := len(leftover) + len(value)
			if cap(scratchBuf) < needed+directio.BlockSize {
				scratchBuf = directio.AlignedBlock(needed + directio.BlockSize)
			}
			scratchBuf = scratchBuf[:0]
			scratchBuf = append(scratchBuf, leftover...)
			blobPosInFile += int64(len(leftover))
			scratchBuf = append(scratchBuf, value...)
			toWrite = scratchBuf
		}

		blobMetas = append(blobMetas, blobMeta{key: key, pos: blobPosInFile, size: len(value)})

		// Write aligned blocks, keep remainder as leftover
		var writeErr error
		leftover, writeErr = writeAlignedBlocks(segment.file, toWrite)
		if writeErr != nil {
			fmt.Printf("CRITICAL: segment write failed: %v\n", writeErr)
			return false
		}
		filePos += int64(len(toWrite) - len(leftover))

		return true
	})

	// Write final leftover with padding
	if len(leftover) > 0 {
		paddedSize := ((len(leftover) + directio.BlockSize - 1) / directio.BlockSize) * directio.BlockSize
		if cap(scratchBuf) < paddedSize {
			scratchBuf = directio.AlignedBlock(paddedSize)
		}
		scratchBuf = scratchBuf[:paddedSize]
		copy(scratchBuf, leftover)
		if _, err := segment.file.Write(scratchBuf); err != nil {
			fmt.Printf("CRITICAL: segment write failed: %v\n", err)
			return 0, err
		}
		filePos += int64(paddedSize)
	}

	// Calculate total bytes written
	bytesWritten := filePos - segment.size

	// Update index with blob metadata
	now := time.Now().UnixNano()
	mt.writeMemFileIndex(ctx, segment.id, now, blobMetas)

	return bytesWritten, nil
}

// writeMemFileIndex updates index with blob metadata
func (mt *MemTable) writeMemFileIndex(
	ctx context.Context, segmentID string, timestamp int64, blobMetas []blobMeta,
) {
	// Bulk insert into index using Appender
	appender, cleanup, err := mt.cache.index.NewAppender(ctx)
	if err != nil {
		fmt.Printf("Warning: failed to create appender: %v\n", err)
		mt.flushSkipmapFallback(ctx, segmentID, timestamp, blobMetas)
		return
	}
	defer cleanup()

	for _, meta := range blobMetas {
		k := base.Key(meta.key)
		if err := appender.AppendRow(
			k.Raw(),
			segmentID,
			meta.pos,
			meta.size,
			timestamp,
		); err != nil {
			cleanup()
			fmt.Printf("Warning: appender failed: %v\n", err)
			mt.flushSkipmapFallback(ctx, segmentID, timestamp, blobMetas)
			return
		}
	}

	if err := appender.Flush(); err != nil {
		fmt.Printf("Warning: appender flush failed: %v\n", err)
		mt.flushSkipmapFallback(ctx, segmentID, timestamp, blobMetas)
		return
	}
}

// blobMeta tracks metadata for each blob in segment
type blobMeta struct {
	key  []byte
	pos  int64
	size int
}

// flushSkipmapFallback retries index inserts one at a time
// The segment file has already been written successfully - only retry index inserts
func (mt *MemTable) flushSkipmapFallback(
	ctx context.Context, segmentID string, timestamp int64, blobMetas []blobMeta,
) {
	// Retry each index insert individually
	// Segment file write succeeded - only need to update index
	for _, meta := range blobMetas {
		k := base.Key(meta.key)
		if err := mt.cache.index.Put(ctx, k.Raw(), segmentID, meta.pos, meta.size, timestamp); err != nil {
			// Log but continue - some keys may already exist (updates/duplicates)
			fmt.Printf("Warning: individual index insert failed for key %x: %v\n", meta.key, err)
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
