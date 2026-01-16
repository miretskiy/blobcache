// Package wal implements a write-ahead log with group commit for durability.
//
// WAL entries use the unified record.Record format (35-byte header + key + value),
// enabling shared encoding/decoding code with segment files.
//
// Group commit batches multiple concurrent writers into a single fsync,
// amortizing the cost of durability across many operations.
package wal

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// SyncMode controls how WAL entries are synced to disk.
type SyncMode int

const (
	SyncNone SyncMode = iota // No sync (testing only)
	SyncData                 // fdatasync (data only, not metadata)
	SyncFull                 // fsync (full durability)
)

// Config configures WAL behavior.
type Config struct {
	Dir      string   // Directory for WAL files
	SyncMode SyncMode // Sync mode (default: SyncData)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:      dir,
		SyncMode: SyncData,
	}
}

// request is the internal ticket for group commit.
// No channel allocation - uses Done bool + sync.Cond for signaling.
type request struct {
	rec  record.Record // Unified record format
	Done bool          // Set true when batch completes
	Err  error         // Error from sync (if any)
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
// WAL files are named by the first SeqID written to them, providing:
//   - Natural ordering for recovery (replay in sequence order)
//   - 1:1 pairing with ActiveSlabs
//   - Rotate() is called when ActiveSlab rotates
//   - DeleteFile(firstSeqID) is called when slab is flushed to segment
type WAL struct {
	cfg Config

	mu   sync.Mutex
	cond *sync.Cond // Signals batch completion

	file           *os.File
	currentFirstID uint64 // First SeqID of current WAL file (0 = not yet set)

	// Double-buffered pending queues (ping-pong swap)
	// Pre-allocated capacity to avoid resizing during hot path
	pending  []*request
	flushing []*request

	// Reusable buffer for gathered I/O (writev)
	// Slice of encoded records for net.Buffers.WriteTo
	encodeBuffers net.Buffers

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

	return w, nil
}

// Write adds a record to the WAL and blocks until the batch is synced.
// Multiple concurrent callers batch together for a single fsync.
// Uses sync.Cond for efficient signaling without per-request channel allocation.
func (w *WAL) Write(rec record.Record) error {
	if w.closed.Load() {
		return fmt.Errorf("wal: closed")
	}

	// Create request ticket (no channel allocation)
	req := &request{rec: rec}

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

		// I/O outside lock
		err := w.syncBatch(toFlush)

		w.mu.Lock()
		w.writerBusy = false

		// Mark all requests in batch as done
		for _, r := range toFlush {
			r.Err = err
			r.Done = true
		}

		// Save buffer for reuse in next cycle
		w.flushing = toFlush

		// Wake all waiters
		w.cond.Broadcast()
	}
}

// Close syncs and closes the WAL.
// Both sync and close are always attempted; errors are combined.
func (w *WAL) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Wake any waiters so they can see closed state
	w.cond.Broadcast()

	if w.file == nil {
		return nil
	}

	// Always attempt both sync and close (never ignore errors in WAL code)
	syncErr := w.sync()
	closeErr := w.file.Close()
	w.file = nil

	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("wal: close: %w", err)
	}
	return nil
}

// syncBatch writes a batch of requests to disk and syncs.
// Called by the leader goroutine outside the lock.
//
// Uses net.Buffers for gathered I/O: encodes all records into separate buffers,
// then writes them all in a single WriteTo call. Go handles IOV_MAX limits
// automatically, issuing multiple syscalls if needed.
func (w *WAL) syncBatch(batch []*request) error {
	if len(batch) == 0 {
		return nil
	}

	// Use the first record's SeqID to name the WAL file (if not already open)
	firstSeqID := batch[0].rec.SeqID
	if err := w.ensureFile(firstSeqID); err != nil {
		return err
	}

	// Encode all records into net.Buffers for gathered I/O
	// Each record gets its own buffer; WriteTo issues writev syscall(s)
	buffers := w.encodeBuffers[:0] // Reuse slice, clear contents
	var totalSize int64
	for _, req := range batch {
		encoded := record.AppendRecord(nil, req.rec)
		buffers = append(buffers, encoded)
		totalSize += int64(len(encoded))
	}
	w.encodeBuffers = buffers // Save for reuse (may have grown)

	// Write all buffers (Go handles IOV_MAX chunking automatically)
	n, err := buffers.WriteTo(w.file)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if n != totalSize {
		return fmt.Errorf("wal: short write: %d != %d", n, totalSize)
	}

	// Sync to disk
	if err := w.sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	// Update metrics
	w.WrittenBytes.Add(totalSize)
	w.WrittenRecs.Add(int64(len(batch)))
	w.SyncCount.Add(1)
	w.GroupCommits.Add(1)

	return nil
}

func (w *WAL) sync() error {
	if w.file == nil {
		return nil
	}
	switch w.cfg.SyncMode {
	case SyncData:
		return sys.Fdatasync(w.file)
	case SyncFull:
		return w.file.Sync()
	default:
		return nil
	}
}

// ensureFile opens the WAL file if not already open.
// Uses firstSeqID to determine the file name.
// Called lazily on first write to capture the first sequence ID.
func (w *WAL) ensureFile(firstSeqID uint64) error {
	if w.file != nil {
		return nil
	}

	// Capture the first SeqID for this WAL file
	w.currentFirstID = firstSeqID

	path := w.walPath(firstSeqID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("wal: create file: %w", err)
	}

	// Write file header
	hdr := FileHeader{
		Magic:     FileMagic,
		Version:   FileVersion,
		CreatedAt: time.Now().UnixNano(),
	}
	if _, writeErr := f.Write(hdr.Encode()); writeErr != nil {
		closeErr := f.Close()
		return fmt.Errorf("wal: write header: %w", errors.Join(writeErr, closeErr))
	}

	w.file = f
	return nil
}

// Rotate closes the current WAL file and prepares for a new one.
// Called by MemTable when ActiveSlab rotates.
// The next write will create a new WAL file named by its SeqID.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file (always attempt both sync and close)
	if w.file != nil {
		syncErr := w.sync()
		closeErr := w.file.Close()
		w.file = nil

		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("wal: rotate: %w", err)
		}
	}

	// Reset - next write will set currentFirstID
	w.currentFirstID = 0
	return nil
}

// DeleteFile removes the WAL file for a flushed slab.
// Called by flusher after segment is written to disk.
func (w *WAL) DeleteFile(firstSeqID uint64) error {
	path := w.walPath(firstSeqID)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil // Already deleted
	}
	return err
}

// walPath returns the path for a WAL file with the given first sequence ID.
// Uses 20-digit zero-padding to handle full uint64 range (max is 20 digits) and ensure proper sorting.
func (w *WAL) walPath(firstSeqID uint64) string {
	return filepath.Join(w.cfg.Dir, fmt.Sprintf("wal-%020d.log", firstSeqID))
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
