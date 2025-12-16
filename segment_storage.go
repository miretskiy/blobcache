package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
	"github.com/ncw/directio"
)

type Storage struct {
	config
	index *index.Index
	seq   atomic.Int64
	cache sync.Map // segmentID (int64) -> *os.File
}

func NewStorage(cfg config, idx *index.Index) *Storage {
	s := Storage{config: cfg, index: idx}
	s.seq.Store(time.Now().UnixNano())
	return &s
}

// Close closes all cached file handles
func (s *Storage) Close() error {
	s.cache.Range(func(key, value any) bool {
		if file, ok := value.(*os.File); ok {
			_ = file.Close()
		}
		s.cache.Delete(key)
		return true
	})
	return nil
}

// Get reads a blob from a segment file at the specified position
func (r *Storage) Get(key Key) (io.Reader, bool) {
	var record index.Value
	if err := r.index.Get(key, &record); err != nil {
		return nil, false
	}

	file, err := r.getSegmentFile(record.SegmentID)
	if err != nil {
		return nil, false
	}

	// Read blob data from segment
	data := make([]byte, record.Size)
	n, err := file.ReadAt(data, record.Pos)
	if err != nil && err != io.EOF {
		return nil, false
	}
	if int64(n) != record.Size {
		return nil, false
	}

	reader := bytes.NewReader(data)

	// Wrap with checksum verification if enabled
	if r.Resilience.VerifyOnRead && r.Resilience.ChecksumHasher != nil &&
		record.Checksum != metadata.InvalidChecksum {
		return newChecksumVerifyingReader(reader, r.Resilience.ChecksumHasher, uint32(record.Checksum)), true
	}

	return reader, true
}

// getSegmentPath returns the path for a segment file
func getSegmentPath(basePath string, numShards int, segmentID int64) string {
	shardNo := segmentID % int64(max(1, numShards))
	return filepath.Join(basePath, "segments",
		fmt.Sprintf("%04d", shardNo),
		fmt.Sprintf("%d.seg", segmentID),
	)
}

// getSegmentFile returns cached file or opens it
func (s *Storage) getSegmentFile(segmentID int64) (*os.File, error) {
	// Check cache
	if cached, ok := s.cache.Load(segmentID); ok {
		return cached.(*os.File), nil
	}

	segmentPath := getSegmentPath(s.Path, s.Shards, segmentID)
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, err
	}

	// Store in cache (LoadOrStore handles race)
	actual, _ := s.cache.LoadOrStore(segmentID, file)
	if actual != file {
		// Another goroutine opened it first, close ours and use theirs
		file.Close()
		return actual.(*os.File), nil
	}

	return file, nil
}

// SegmentWriter writes multiple blobs into large segment files
type SegmentWriter struct {
	config
	getSequenceID func() int64

	// Current segment state
	currentFile  *os.File
	currentID    int64
	currentPos   int64
	lastWritePos int64                 // Position of last Write() for Pos()
	leftover     []byte                // For DirectIO alignment
	records      []metadata.BlobRecord // Records for footer
}

// NewSegmentWriter creates a segment writer for a specific worker
func (s *Storage) NewSegmentWriter() *SegmentWriter {
	return &SegmentWriter{
		config:        s.config,
		getSequenceID: func() int64 { return s.seq.Add(1) },
	}
}

// openNewSegment opens a new segment file
func (w *SegmentWriter) openNewSegment() error {
	// Close current if open
	if w.currentFile != nil {
		if err := w.closeCurrentSegment(); err != nil {
			return err
		}
	}

	// Generate segment ID: timestamp for uniqueness
	w.currentID = w.getSequenceID()

	// Get segment path from path manager
	segmentPath := getSegmentPath(w.Path, w.Shards, w.currentID)

	var err error
	if w.IO.DirectIO {
		w.currentFile, err = directio.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	} else {
		w.currentFile, err = os.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}

	if err != nil {
		return err
	}

	w.currentPos = 0
	w.leftover = nil
	w.records = nil // Reset records for new segment
	return nil
}

// closeCurrentSegment finalizes current segment
func (w *SegmentWriter) closeCurrentSegment() (retErr error) {
	if w.currentFile == nil {
		return nil
	}

	defer func() {
		err := w.currentFile.Close()
		w.currentFile = nil
		retErr = errors.Join(retErr, err)
	}()

	// Write footer with all records for this segment
	if len(w.records) > 0 {
		footerBytes := metadata.AppendSegmentRecordWithFooter(nil,
			metadata.SegmentRecord{
				Records:   w.records,
				SegmentID: w.currentID,
				CTime:     time.Now(),
				IndexKey:  nil,
			})

		var err error
		if w.IO.DirectIO {
			err = w.writeDirectIO(footerBytes)
		} else {
			err = w.writeBufferred(footerBytes)
		}
		if err != nil {
			return fmt.Errorf("failed to write footer: %w", err)
		}
	}

	if w.IO.FDataSync {
		if err := fdatasync(w.currentFile); err != nil {
			return err
		}
	}

	return nil
}

// Write writes a blob to current segment
func (w *SegmentWriter) Write(key Key, value []byte, checksum uint64) error {
	// Open new segment if needed or if this write would exceed segment size
	if w.currentFile == nil || w.currentPos+int64(len(value)) > w.SegmentSize {
		if err := w.openNewSegment(); err != nil {
			return err
		}
	}

	// Track record for footer
	rec := metadata.BlobRecord{
		Hash:     uint64(key),
		Pos:      w.currentPos,
		Size:     int64(len(value)),
		Checksum: checksum,
	}
	w.records = append(w.records, rec)

	if w.IO.DirectIO {
		return w.writeDirectIO(value)
	}
	return w.writeBufferred(value)
}

// Pos returns the position where the last Write() stored data
func (w *SegmentWriter) Pos() WritePosition {
	return WritePosition{
		SegmentID: w.currentID,
		Pos:       w.lastWritePos,
	}
}

// Close finalizes and closes the current segment
func (w *SegmentWriter) Close() error {
	return w.closeCurrentSegment()
}

func (w *SegmentWriter) writeBufferred(value []byte) error {
	// Buffered write
	w.lastWritePos = w.currentPos

	// Simple buffered write
	n, err := w.currentFile.Write(value)
	if err != nil {
		return err
	}
	w.currentPos += int64(n)

	// Fdatasync after each write if enabled (not needed for DirectIO)
	if w.IO.FDataSync {
		if err := fdatasync(w.currentFile); err != nil {
			return fmt.Errorf("failed to fdatasync after write: %w", err)
		}
	}
	return nil
}

func (w *SegmentWriter) writeDirectIO(value []byte) error {
	// Each blob is padded individually (no cross-blob leftover)
	// This wastes space but keeps blob addressing simple
	w.lastWritePos = w.currentPos

	// Pad value to block size
	const mask = directio.BlockSize - 1
	paddedSize := (len(value) + mask) &^ mask

	var buf []byte
	if len(value) == paddedSize && isAligned(value) {
		// Already padded and aligned (fast path)
		buf = value
	} else {
		// Allocate aligned+padded buffer
		buf = directio.AlignedBlock(paddedSize)
		copy(buf, value)
	}

	n, err := w.currentFile.Write(buf)
	if err != nil {
		return err
	}
	w.currentPos += int64(n)
	// DirectIO doesn't need fdatasync (bypasses OS cache, writes directly to disk)
	return nil
}
