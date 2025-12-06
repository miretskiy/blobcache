package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/ncw/directio"
)

// SegmentWriter writes multiple blobs into large segment files
type SegmentWriter struct {
	basePath    string
	segmentSize int64
	useDirectIO bool
	workerID    int

	// Current segment state
	currentFile  *os.File
	currentID    int
	currentPos   int64
	lastWritePos int64  // Position of last Write() for Pos()
	leftover     []byte // For DirectIO alignment
}

// NewSegmentWriter creates a segment writer for a specific worker
func NewSegmentWriter(basePath string, segmentSize int64, useDirectIO bool, workerID int) *SegmentWriter {
	return &SegmentWriter{
		basePath:    basePath,
		segmentSize: segmentSize,
		useDirectIO: useDirectIO,
		workerID:    workerID,
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

	// Generate segment ID: timestamp
	now := time.Now().UnixNano()
	w.currentID = int(now)

	segmentPath := filepath.Join(w.basePath, "segments", fmt.Sprintf("%d.seg", w.currentID))

	var err error
	if w.useDirectIO {
		w.currentFile, err = directio.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	} else {
		w.currentFile, err = os.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}

	if err != nil {
		return err
	}

	w.currentPos = 0
	w.leftover = nil
	return nil
}

// closeCurrentSegment finalizes current segment
func (w *SegmentWriter) closeCurrentSegment() error {
	if w.currentFile == nil {
		return nil
	}

	// Flush leftover if DirectIO
	if w.useDirectIO && len(w.leftover) > 0 {
		const mask = directio.BlockSize - 1
		paddedSize := (len(w.leftover) + mask) &^ mask
		buf := directio.AlignedBlock(paddedSize)
		copy(buf, w.leftover)
		if _, err := w.currentFile.Write(buf); err != nil {
			return err
		}

		// Track actual size before padding
		actualSize := w.currentPos + int64(len(w.leftover))

		// Close file
		if err := w.currentFile.Close(); err != nil {
			return err
		}
		w.currentFile = nil

		// Truncate to remove padding
		segmentPath := filepath.Join(w.basePath, "segments", fmt.Sprintf("%d.seg", w.currentID))
		if err := os.Truncate(segmentPath, actualSize); err != nil {
			return err
		}
	} else {
		// Just close for buffered I/O
		if err := w.currentFile.Close(); err != nil {
			return err
		}
		w.currentFile = nil
	}

	return nil
}

// Write writes a blob to current segment
func (w *SegmentWriter) Write(key base.Key, value []byte) error {
	// Open new segment if needed
	if w.currentFile == nil || w.currentPos >= w.segmentSize {
		if err := w.openNewSegment(); err != nil {
			return err
		}
	}

	// Record position before write
	w.lastWritePos = w.currentPos

	if w.useDirectIO {
		// Handle DirectIO with alignment and leftover
		// TODO: Implement DirectIO write with leftover handling
		return fmt.Errorf("DirectIO segment write not implemented yet")
	} else {
		// Simple buffered write
		n, err := w.currentFile.Write(value)
		if err != nil {
			return err
		}
		w.currentPos += int64(n)
	}

	return nil
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
