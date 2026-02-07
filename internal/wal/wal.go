// Package wal implements a write-ahead log with group commit for durability.
//
// WAL entries use the unified record.Record format (35-byte header + key + value),
// enabling shared encoding/decoding code with segment files.
//
// Group commit batches multiple concurrent writers into a single fsync,
// amortizing the cost of durability across many operations.
//
// Rotation is treated as a control packet in the data stream, ensuring strict
// ordering: all writes before a ROTATE command go to the old file, all writes
// after go to the new file. This solves the distributed consensus problem between
// MemTable (which controls slab rotation) and WAL (which controls file I/O).
package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// ErrSequenceRegression is returned when a write's SeqID is older than the
// current WAL file's starting SeqID. This indicates a bug in the caller
// (typically MemTable) where a "stale" write leaked into a newer epoch.
var ErrSequenceRegression = errors.New("wal: sequence ID regression")

// WriteResult contains metadata about a completed WAL write.
// Used by callers to track record positions for index updates.
type WriteResult struct {
	Offset       int64 // Absolute file offset of the record Magic byte
	BytesWritten int64 // Physical size (Header + Key + Value)
	BytesAligned int64 // Size on disk including padding (for stats/debugging)
}

// Config configures WAL behavior.
type Config struct {
	Dir          string       // Directory for WAL files
	Flags        sys.OpenFlag // File flags: FlDirectIO, FlDSync, FlSync (default: FlDirectIO|FlDSync)
	MaxBatchSize int          // Max staging buffer size (default: 16MB). 0 means use default.
}

// DefaultMaxBatchSize is the staging buffer size for DirectIO (16MB).
// This should cover the maximum expected concurrent batch size.
const DefaultMaxBatchSize = 16 << 20

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:   dir,
		Flags: sys.FlDirectIO | sys.SyncData,
	}
}

// request is the internal ticket for group commit.
// It can be a data record or a rotation command.
type request struct {
	rec record.Record // Payload (if not isRotation)

	// Control flag: if true, this is a "rotate file" command.
	// When processed, the leader closes the current file (syncing all preceding
	// writes) and the next data write will create a new file.
	isRotation bool

	Done   bool        // Set true when request completes
	Err    error       // Error from processing (if any)
	Result WriteResult // Write result (valid when Done && Err == nil && !isRotation)
}

// Stats contains WAL metrics for observability.
type Stats struct {
	WrittenBytes atomic.Int64 // Total bytes written
	WrittenRecs  atomic.Int64 // Total records written
	SyncCount    atomic.Int64 // Number of sync calls
	GroupCommits atomic.Int64 // Number of group commit batches
}

// WAL implements write-ahead logging with group commit.
//
// Uses O_DIRECT with "Pad & Advance" strategy:
//   - All writes are padded to 4KB boundaries
//   - Zero-padding at tail is treated as EOF during recovery
//   - Bypasses page cache for consistent high-throughput at 3GB/s+
//
// WAL files are named by the first SeqID written to them, providing:
//   - Natural ordering for recovery (replay in sequence order)
//   - 1:1 pairing with ActiveSlabs (rotation commands create file boundaries)
//   - DeleteFile(firstSeqID) is called when slab is flushed to segment
type WAL struct {
	cfg Config

	mu   sync.Mutex
	cond *sync.Cond // Signals batch completion

	file           *os.File
	fileOffset     int64  // Current write position (for O_DIRECT pwrite)
	currentFirstID uint64 // First SeqID of current WAL file (0 = not yet set)

	// Sealed Guard: lastRotatedSeq is the max SeqID from the PREVIOUS closed file.
	// Any write with SeqID <= lastRotatedSeq belongs to a deleted file and is rejected.
	// currentMaxSeq tracks the max SeqID in the current open file (becomes lastRotatedSeq on close).
	lastRotatedSeq uint64
	currentMaxSeq  uint64

	// Double-buffered pending queues (ping-pong swap)
	// Pre-allocated capacity to avoid resizing during hot path
	pending  []*request
	flushing []*request

	// Reusable encode buffer for "Flatten and Flush" pattern.
	// 4KB-aligned memory from sys.AllocAligned for O_DIRECT.
	// Sizing policy: grows to max batch size, never shrinks (avoids allocation churn).
	encodeBuf []byte

	// Leader election: only one goroutine flushes at a time
	writerBusy bool

	// Shutdown
	closed atomic.Bool

	// Embedded stats for observability
	Stats
}

// Open creates or opens a WAL in the given directory.
// The first write will determine the WAL file name (using the record's SeqID).
func Open(cfg Config) (*WAL, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: create directory: %w", err)
	}

	w := &WAL{
		cfg:            cfg,
		currentFirstID: 0,                         // Will be set on first write
		pending:        make([]*request, 0, 4096), // Pre-allocate for throughput
		flushing:       make([]*request, 0, 4096),
	}
	w.cond = sync.NewCond(&w.mu)

	// Pre-allocate aligned staging buffer for O_DIRECT
	// Include FileHeaderSize since header is prepended on first write
	bufSize := cfg.MaxBatchSize
	if bufSize <= 0 {
		bufSize = DefaultMaxBatchSize
	}
	alignedSize := int(sys.PageAlign(int64(bufSize + FileHeaderSize)))
	w.encodeBuf = sys.AllocAligned(alignedSize)

	return w, nil
}

// Write adds a record to the WAL and blocks until the batch is synced.
// Multiple concurrent callers batch together for a single fsync.
// Returns WriteResult with the record's offset and size information.
func (w *WAL) Write(rec record.Record) (WriteResult, error) {
	req := &request{rec: rec}
	err := w.submit(req)
	return req.Result, err
}

// EnqueueRotation signals the WAL to close the current file after syncing
// all preceding writes. The next Write() call will create a new file.
//
// This is serialized with Write() calls, ensuring strict ordering:
// all writes before EnqueueRotation go to the old file, all writes after
// go to the new file. This solves the race condition between MemTable
// slab rotation and WAL file boundaries.
//
// Returns the firstSeqID of the file that was closed (for DeleteFile later),
// or 0 if no file was open.
func (w *WAL) EnqueueRotation() (closedFileID uint64, err error) {
	// Capture currentFirstID before submitting rotation.
	// This is safe because the rotation command will be processed in order,
	// so no new writes can change currentFirstID until after the rotation.
	w.mu.Lock()
	closedFileID = w.currentFirstID
	w.mu.Unlock()

	err = w.submit(&request{isRotation: true})
	if err != nil {
		return 0, err
	}
	return closedFileID, nil
}

// submit handles the leader election and queueing logic for any request type.
func (w *WAL) submit(req *request) error {
	if w.closed.Load() {
		return fmt.Errorf("wal: closed")
	}

	w.mu.Lock()
	w.pending = append(w.pending, req)

	for {
		// Check if our request is done
		if req.Done {
			w.mu.Unlock()
			return req.Err
		}

		// If another goroutine is flushing, wait for it
		if w.writerBusy {
			w.cond.Wait()
			continue
		}

		// Become the leader
		w.writerBusy = true

		// Ping-pong swap: pending becomes toFlush, flushing becomes new pending
		toFlush := w.pending
		w.pending = w.flushing
		w.pending = w.pending[:0] // Clear but retain capacity

		w.mu.Unlock()

		// Process batch (outside lock) - handles both data and rotation
		err := w.processBatch(toFlush)

		w.mu.Lock()
		w.writerBusy = false

		// Mark all requests in batch as done
		for _, r := range toFlush {
			if !r.Done { // May already be marked done by processBatch
				r.Err = err
				r.Done = true
			}
		}

		// Save buffer for reuse in next cycle
		w.flushing = toFlush

		// Wake all waiters
		w.cond.Broadcast()
	}
}

// processBatch handles a batch of requests, splitting on rotation commands.
// Rotation commands act as barriers:
//  1. All preceding records are flushed first
//  2. The rotation command is processed (file closed)
//  3. Iteration continues with remaining requests
func (w *WAL) processBatch(batch []*request) error {
	for len(batch) > 0 {
		// Find first rotation command
		rotationIdx := -1
		for i, req := range batch {
			if req.isRotation {
				rotationIdx = i
				break
			}
		}

		if rotationIdx == -1 {
			// No rotation commands, flush all records
			return w.flushRecords(batch)
		}

		// Flush everything before the rotation
		if rotationIdx > 0 {
			if err := w.flushRecords(batch[:rotationIdx]); err != nil {
				return err
			}
			markDone(batch[:rotationIdx], nil)
		}

		// Process the rotation command
		if err := w.closeCurrentFile(); err != nil {
			return err
		}
		batch[rotationIdx].Done = true
		batch[rotationIdx].Err = nil

		// Continue with remaining batch
		batch = batch[rotationIdx+1:]
	}
	return nil
}

// markDone marks all requests in the slice as done with the given error.
func markDone(batch []*request, err error) {
	for _, r := range batch {
		r.Done = true
		r.Err = err
	}
}

// prepareWrite validates the sequence guard, updates the max sequence tracker,
// and ensures the file is open. Used by writeLargeRecord for single-record writes.
// For batch writes, flushNormalRecords handles this differently (min/max scanning).
func (w *WAL) prepareWrite(seqID uint64) error {
	if seqID <= w.lastRotatedSeq {
		return fmt.Errorf("%w: seq %d <= sealed seq %d",
			ErrSequenceRegression, seqID, w.lastRotatedSeq)
	}
	if seqID > w.currentMaxSeq {
		w.currentMaxSeq = seqID
	}
	if w.file == nil {
		return w.ensureFile(seqID)
	}
	return nil
}

// flushRecords writes data records using "Pad & Advance" strategy for O_DIRECT.
//
// All writes are padded to 4KB boundaries with zeros. Recovery treats zeros as EOF.
// On first write, file header is prepended to avoid a hole between header and data.
//
// If the batch exceeds buffer capacity, it is split into multiple chunks.
// Records larger than the buffer are handled via writeLargeRecord (slow path).
func (w *WAL) flushRecords(batch []*request) error {
	if len(batch) == 0 {
		return nil
	}

	// 1. Scan for minimum SeqID (for naming), maximum SeqID (for guard), and validate.
	minSeq := batch[0].rec.SeqID
	var maxSeq uint64

	for _, req := range batch {
		if req.rec.SeqID < minSeq {
			minSeq = req.rec.SeqID
		}
		if req.rec.SeqID > maxSeq {
			maxSeq = req.rec.SeqID
		}

		// Sealed Guard: reject writes that belong to a PREVIOUS closed file.
		if req.rec.SeqID <= w.lastRotatedSeq {
			return fmt.Errorf("%w: seq %d <= sealed seq %d",
				ErrSequenceRegression, req.rec.SeqID, w.lastRotatedSeq)
		}
	}

	// Track max for the current file (becomes lastRotatedSeq on close)
	if maxSeq > w.currentMaxSeq {
		w.currentMaxSeq = maxSeq
	}

	// 2. Ensure file is open (lazy init using the minimum SeqID for correct naming)
	if w.file == nil {
		if err := w.ensureFile(minSeq); err != nil {
			return err
		}
	}

	// 3. Write records in chunks that fit the staging buffer
	bufCap := cap(w.encodeBuf)
	var totalBytes int
	idx := 0

	for idx < len(batch) {
		// Check if we need file header (may change after oversized writes)
		needHeader := w.fileOffset == 0
		overhead := 0
		if needHeader {
			overhead = int(sys.PageAlign(int64(FileHeaderSize)))
		}

		chunkStart := idx
		chunkSize := overhead

		for idx < len(batch) {
			recSize := batch[idx].rec.EncodedSize()
			paddedRecSize := int(sys.PageAlign(int64(recSize)))

			if chunkSize+paddedRecSize <= bufCap {
				// Record fits in current chunk (padded to block boundary)
				chunkSize += paddedRecSize
				idx++
			} else if chunkStart == idx {
				// Single record exceeds buffer - use slow path
				// writeLargeRecord handles file opening and header internally
				if err := w.writeLargeRecord(batch[idx]); err != nil {
					return err
				}
				totalBytes += recSize
				idx++
				// Reset chunk tracking - header may have been written
				chunkStart = idx
				chunkSize = 0
				overhead = 0
				needHeader = false // Header was written by writeLargeRecord
			} else {
				// Chunk is full, write what we have
				break
			}
		}

		// Write the chunk (if we have records to write)
		// Re-check header state since writeLargeRecord may have written it
		if chunkStart < idx && chunkSize > overhead {
			includeHeader := w.fileOffset == 0
			if err := w.writeChunk(batch[chunkStart:idx], includeHeader); err != nil {
				return err
			}
			totalBytes += chunkSize - overhead
		}
	}

	// Update metrics
	w.WrittenBytes.Add(int64(totalBytes))
	w.WrittenRecs.Add(int64(len(batch)))
	w.SyncCount.Add(1)
	w.GroupCommits.Add(1)

	return nil
}

// writeChunk writes a chunk of records that fits in the staging buffer.
// Populates WriteResult for each request in the chunk.
//
// Records are padded to block boundaries so that WAL-renamed segments are born
// with block-aligned record offsets, enabling XFS reflinks during compaction.
func (w *WAL) writeChunk(chunk []*request, includeHeader bool) error {
	// Calculate payload size with per-record block padding for reflink alignment.
	// Each record occupies PageAlign(recSize) bytes, and the file header (when
	// present) is padded to a full block so the first record starts at offset 4096.
	payloadSize := 0
	for _, req := range chunk {
		payloadSize = int(sys.PageAlign(int64(payloadSize + req.rec.EncodedSize())))
	}

	totalPayload := payloadSize
	if includeHeader {
		totalPayload += int(sys.PageAlign(int64(FileHeaderSize)))
	}
	writeSize := int(sys.PageAlign(int64(totalPayload)))

	buf := w.encodeBuf[:writeSize]
	clear(buf) // Zero entire buffer: inter-record padding, header padding, tail

	// Write header if needed, padded to block boundary so first record is aligned.
	bufOffset := 0
	if includeHeader {
		hdr := FileHeader{
			Magic:     FileMagic,
			Version:   FileVersion,
			CreatedAt: time.Now().UnixNano(),
		}
		hdr.EncodeTo(buf)
		bufOffset = int(sys.PageAlign(int64(FileHeaderSize)))
	}

	// Track file offset for WriteResult (before writing)
	baseFileOffset := w.fileOffset
	if includeHeader {
		baseFileOffset += sys.PageAlign(int64(FileHeaderSize))
	}

	// Serialize records with per-record block padding
	for _, req := range chunk {
		recSize := req.rec.EncodedSize()
		req.rec.EncodeTo(buf[bufOffset:])

		req.Result = WriteResult{
			Offset:       baseFileOffset,
			BytesWritten: int64(recSize),
			BytesAligned: int64(writeSize), // Shared across batch (total aligned write)
		}

		bufOffset = int(sys.PageAlign(int64(bufOffset + recSize)))
		baseFileOffset = sys.PageAlign(baseFileOffset + int64(recSize))
	}

	// Write and sync
	return w.writeAndSync(buf)
}

// writeLargeRecord handles a single record larger than the staging buffer.
// Allocates a temporary aligned buffer for the write (slow path).
// Populates WriteResult for the request.
func (w *WAL) writeLargeRecord(req *request) error {
	if err := w.prepareWrite(req.rec.SeqID); err != nil {
		return err
	}

	includeHeader := w.fileOffset == 0

	recSize := req.rec.EncodedSize()
	totalPayload := recSize
	recordOffset := w.fileOffset
	if includeHeader {
		paddedHeader := int(sys.PageAlign(int64(FileHeaderSize)))
		totalPayload = paddedHeader + recSize
		recordOffset += int64(paddedHeader)
	}
	writeSize := int(sys.PageAlign(int64(totalPayload)))

	// Allocate temporary aligned buffer
	buf := sys.AllocAligned(writeSize)
	defer sys.FreeAligned(buf)
	clear(buf) // Zero entire buffer (header padding + tail)

	// Write header if needed, padded to block boundary
	bufOffset := 0
	if includeHeader {
		hdr := FileHeader{
			Magic:     FileMagic,
			Version:   FileVersion,
			CreatedAt: time.Now().UnixNano(),
		}
		hdr.EncodeTo(buf)
		bufOffset = int(sys.PageAlign(int64(FileHeaderSize)))
	}

	// Serialize record
	req.rec.EncodeTo(buf[bufOffset:])

	// Populate WriteResult
	req.Result = WriteResult{
		Offset:       recordOffset,
		BytesWritten: int64(recSize),
		BytesAligned: int64(writeSize),
	}

	// Update metrics
	w.WrittenBytes.Add(int64(recSize))
	w.WrittenRecs.Add(1)
	w.SyncCount.Add(1)

	// Write and sync
	return w.writeAndSync(buf)
}

// writeAndSync performs the actual pwrite and fsync.
func (w *WAL) writeAndSync(buf []byte) error {
	n, err := w.file.WriteAt(buf, w.fileOffset)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("wal: short write: %d != %d", n, len(buf))
	}
	w.fileOffset += int64(n)

	if err := w.sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// closeCurrentFile syncs and closes the current WAL file.
// Resets state so next write creates a new file.
// Latches the guard: currentMaxSeq becomes the new lastRotatedSeq floor.
func (w *WAL) closeCurrentFile() error {
	if w.file == nil {
		return nil
	}

	// Always attempt both sync and close
	syncErr := w.sync()
	closeErr := w.file.Close()
	w.file = nil
	w.fileOffset = 0
	w.currentFirstID = 0

	// Latch the guard: max SeqID of closed file becomes the floor for new writes
	if w.currentMaxSeq > w.lastRotatedSeq {
		w.lastRotatedSeq = w.currentMaxSeq
	}
	w.currentMaxSeq = 0

	return errors.Join(syncErr, closeErr)
}

func (w *WAL) sync() error {
	if w.file == nil {
		return nil
	}
	return sys.SyncFile(w.file, w.cfg.Flags)
}

// ensureFile opens the WAL file if not already open.
// Uses firstSeqID to determine the file name.
// Header is written with first batch in flushRecords (not here) to avoid holes.
func (w *WAL) ensureFile(firstSeqID uint64) error {
	if w.file != nil {
		return nil
	}

	w.currentFirstID = firstSeqID
	path := w.walPath(firstSeqID)

	f, err := sys.CreateFile(path, w.cfg.Flags)
	if err != nil {
		return fmt.Errorf("wal: create file: %w", err)
	}

	w.file = f
	w.fileOffset = 0 // Header will be written with first batch
	return nil
}

// Close syncs and closes the WAL.
func (w *WAL) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Wake any waiters so they can see closed state
	w.cond.Broadcast()

	if err := w.closeCurrentFile(); err != nil {
		return fmt.Errorf("wal: close: %w", err)
	}
	return nil
}

// DeleteFile removes the WAL file for a flushed slab.
// Called by flusher after segment is written to disk.
func (w *WAL) DeleteFile(firstSeqID uint64) error {
	if firstSeqID == 0 {
		return nil // No file to delete
	}
	path := w.walPath(firstSeqID)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil // Already deleted
	}
	return err
}

// walPath returns the path for a WAL file with the given first sequence ID.
// Uses 20-digit zero-padding to handle full uint64 range and ensure proper sorting.
func (w *WAL) walPath(firstSeqID uint64) string {
	return WALFilePath(w.cfg.Dir, firstSeqID)
}

// FilePath returns the full path for a WAL file with the given sequence ID.
// Used by memtable to rename WAL files to segments.
func (w *WAL) FilePath(fileID uint64) string {
	return WALFilePath(w.cfg.Dir, fileID)
}

// WALFilePath returns the path for a WAL file given directory and sequence ID.
func WALFilePath(dir string, firstSeqID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%020d.log", firstSeqID))
}

// CurrentFirstID returns the first SeqID of the current WAL file (0 if not yet set).
func (w *WAL) CurrentFirstID() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentFirstID
}

// listWALFiles returns all WAL files in the directory, sorted by sequence.
func (w *WAL) listWALFiles() ([]string, error) {
	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && isWALFile(e.Name()) {
			files = append(files, filepath.Join(w.cfg.Dir, e.Name()))
		}
	}

	// Sort by sequence number
	sort.Slice(files, func(i, j int) bool {
		seqI, _ := parseWALFileName(filepath.Base(files[i]))
		seqJ, _ := parseWALFileName(filepath.Base(files[j]))
		return seqI < seqJ
	})

	return files, nil
}

func isWALFile(name string) bool {
	// Match pattern: wal-XXXXXXXXXXXXXXXXXXXX.log (20 digits)
	if len(name) != 28 { // 4 + 20 + 4 = 28
		return false
	}
	return name[:4] == "wal-" && name[24:] == ".log"
}

func parseWALFileName(name string) (uint64, error) {
	if !isWALFile(name) {
		return 0, fmt.Errorf("invalid WAL filename: %s", name)
	}
	var seq uint64
	_, err := fmt.Sscanf(name, "wal-%020d.log", &seq)
	return seq, err
}
