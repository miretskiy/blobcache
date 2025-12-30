package blobcache

import (
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
)

type Storage struct {
	config
	index *index.Index
	seq   atomic.Int64
	cache sync.Map // segmentID (int64) -> SegmentFile
}

func NewStorage(cfg config, idx *index.Index) *Storage {
	s := Storage{config: cfg, index: idx}
	s.seq.Store(time.Now().UnixNano())
	return &s
}

// Close closes all cached segment files
func (s *Storage) Close() error {
	var errs []error
	s.cache.Range(func(key, value any) bool {
		if closer, ok := value.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		s.cache.Delete(key)
		return true
	})
	return errors.Join(errs...)
}

// Get reads a blob from a segment file at the specified position
func (s *Storage) Get(key Key) (io.Reader, bool) {
	record, err := s.index.Get(key)
	if err != nil {
		return nil, false
	}

	// Mark as visited for Sieve eviction algorithm (lock-free)
	record.MarkVisited(true)

	sf, err := s.getSegmentFile(record.SegmentID)
	if err != nil {
		return nil, false
	}

	// 2. Give the kernel a "head start"
	if s.IO.Fadvise {
		// FadvSequential tells the kernel: "We are going to read
		// this whole range, so start prefetching it now."
		_ = Fadvise(sf.file.Fd(), Offset_t(record.Pos), record.Size, FadvSequential)
	}

	// Use SectionReader for lazy reading (reads only when caller reads)
	reader := io.NewSectionReader(sf, record.Pos, record.Size)

	// Wrap with checksum verification if enabled
	if s.Resilience.VerifyOnRead && s.Resilience.ChecksumHasher != nil &&
		record.Checksum != metadata.InvalidChecksum {
		return newChecksumVerifyingReader(reader, s.Resilience.ChecksumHasher, uint32(record.Checksum)), true
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

// getSegmentFile returns cached SegmentFile or opens it
func (s *Storage) getSegmentFile(segmentID int64) (*segmentFile, error) {
	// Check cache
	if cached, ok := s.cache.Load(segmentID); ok {
		return cached.(*segmentFile), nil
	}

	segmentPath := getSegmentPath(s.Path, s.Shards, segmentID)
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	fileSize := stat.Size()

	// Try to read segment record from footer (finalized segment)
	meta, recordEndPos, err := metadata.ReadSegmentFooterFromFile(file, fileSize, segmentID)
	if err != nil {
		// No valid footer - partial segment, read from bitcask
		metaPtr, err := s.index.GetSegmentRecord(segmentID)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("segment not in Index: %w", err)
		}
		meta = *metaPtr
		recordEndPos = fileSize // Footer will be written at EOF on first hole punch
	}

	sf := newSegmentFile(file, recordEndPos, meta)

	// Store in cache (LoadOrStore handles race)
	actual, _ := s.cache.LoadOrStore(segmentID, sf)
	if actual != sf {
		sf.Close()
		return actual.(*segmentFile), nil
	}

	return sf, nil
}

// tryReadFooterFromFile attempts to read and validate segment record from file footer
func (s *Storage) HolePunchBlob(segmentID int64, offset, size int64) error {
	sf, err := s.getSegmentFile(segmentID)
	if err != nil {
		return err
	}
	return sf.PunchHole(offset, size)
}

// SegmentWriter writes multiple blobs into large segment files
type SegmentWriter struct {
	config
	storage       *Storage
	getSequenceID func() int64

	// Current segment state
	currentSegment *segmentFile // SegmentFile for current segment
	currentID      int64
	currentPos     int64
	lastWritePos   int64 // Position of last Write() for Pos()
	leftover       []byte
}

// NewSegmentWriter creates a segment writer for a specific worker
func (s *Storage) NewSegmentWriter() *SegmentWriter {
	return &SegmentWriter{
		config:        s.config,
		storage:       s,
		getSequenceID: func() int64 { return s.seq.Add(1) },
	}
}

// openNewSegment opens a new segment file
func (w *SegmentWriter) openNewSegment() error {
	// Close current if open
	if w.currentSegment != nil {
		if err := w.closeCurrentSegment(); err != nil {
			return err
		}
	}

	// Generate segment ID: timestamp for uniqueness
	w.currentID = w.getSequenceID()

	// Get segment path from path manager
	segmentPath := getSegmentPath(w.Path, w.Shards, w.currentID)

	// Open with O_RDWR (not O_APPEND) to support both writing and hole punching
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR, 0o644)

	if err != nil {
		return err
	}

	// Pre-allocate segment size to reduce fragmentation
	if w.SegmentSize > 0 {
		if err := fallocate(file, w.SegmentSize); err != nil {
			// Non-fatal - log but continue (fallback to growing file)
			log.Warn("failed to pre-allocate segment space",
				"size", w.SegmentSize,
				"error", err)
		}
	}

	// Create SegmentFile and register in Storage cache
	// New segment: footerPos=0 (not written yet), empty meta
	sf := newSegmentFile(file, 0, metadata.SegmentRecord{
		SegmentID: w.currentID,
		CTime:     time.Now(),
	})
	w.storage.cache.Store(w.currentID, sf)
	w.currentSegment = sf

	w.currentPos = 0
	w.leftover = nil
	return nil
}

// closeCurrentSegment finalizes current segment
func (w *SegmentWriter) closeCurrentSegment() error {
	if w.currentSegment == nil {
		return nil
	}

	liveRecords := w.currentSegment.getLiveRecords()
	if len(liveRecords) > 0 {
		footerStartPos := w.currentPos
		footerBytes := metadata.AppendSegmentRecordWithFooter(nil,
			metadata.SegmentRecord{
				Records:   liveRecords,
				SegmentID: w.currentID,
				CTime:     time.Now(),
				IndexKey:  nil,
			})

		if err := w.writeBufferred(footerBytes); err != nil {
			return fmt.Errorf("failed to write footer: %w", err)
		}

		// Seal segment with footer position
		w.currentSegment.seal(footerStartPos)
	}

	if w.IO.FDataSync {
		if err := fdatasync(w.currentSegment.file); err != nil {
			return err
		}
	}

	if w.IO.Fadvise {
		_ = Fadvise(w.currentSegment.file.Fd(), 0, 0, FadvDontNeed)
	}

	// Don't close file - it stays registered in Storage cache
	w.currentSegment = nil
	return nil
}

// Write writes a blob to current segment
func (w *SegmentWriter) Write(key Key, value []byte, checksum uint64) error {
	// Open new segment if needed or if this write would exceed segment size
	if w.currentSegment == nil || w.currentPos+int64(len(value)) > w.SegmentSize {
		if err := w.openNewSegment(); err != nil {
			return err
		}
	}

	// Track record in segmentFile (for footer)
	rec := metadata.BlobRecord{
		Hash:  uint64(key),
		Pos:   w.currentPos,
		Size:  int64(len(value)),
		Flags: checksum, // Low 32 bits = checksum, high 32 bits = flags
	}
	w.currentSegment.addRecord(rec)

	return w.writeBufferred(value)
}

// Pos returns the position where the last Write() stored data
func (w *SegmentWriter) Pos() WritePosition {
	return WritePosition{
		SegmentID: w.currentID,
		Pos:       w.lastWritePos,
	}
}

func (w *SegmentWriter) Fd() uintptr {
	return w.currentSegment.file.Fd()
}

// Close finalizes and closes the current segment
func (w *SegmentWriter) Close() error {
	return w.closeCurrentSegment()
}

func (w *SegmentWriter) writeBufferred(value []byte) error {
	w.lastWritePos = w.currentPos

	// Use WriteAt instead of Write (no O_APPEND)
	n, err := w.currentSegment.WriteAt(value, w.currentPos)
	if err != nil {
		return err
	}
	w.currentPos += int64(n)

	// Fdatasync after each write if enabled
	if w.IO.FDataSync {
		if err := fdatasync(w.currentSegment.file); err != nil {
			return fmt.Errorf("failed to fdatasync after write: %w", err)
		}
	}
	return nil
}
