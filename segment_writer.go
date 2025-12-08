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
	fsync       bool
	workerID    int

	// Current segment state
	currentFile  *os.File
	currentID    int64
	currentPos   int64
	lastWritePos int64  // Position of last Write() for Pos()
	leftover     []byte // For DirectIO alignment
}

// NewSegmentWriter creates a segment writer for a specific worker
func NewSegmentWriter(
	basePath string, segmentSize int64, useDirectIO bool, fsync bool, workerID int,
) *SegmentWriter {
	return &SegmentWriter{
		basePath:    basePath,
		segmentSize: segmentSize,
		useDirectIO: useDirectIO,
		fsync:       fsync,
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

	// Generate segment ID: timestamp-workerID for uniqueness
	w.currentID = time.Now().UnixNano()

	// Flat directory structure for segments
	segmentPath := filepath.Join(w.basePath, "segments", fmt.Sprintf("%d-%02d.seg", w.currentID, w.workerID))

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

	// Fdatasync on close if enabled (defensive)
	// Buffered mode already fsyncs after each write, but sync again to be safe
	// DirectIO mode doesn't need it but doesn't hurt
	if w.fsync {
		if err := fdatasync(w.currentFile); err != nil {
			w.currentFile.Close()
			return fmt.Errorf("failed to fdatasync segment: %w", err)
		}
	}

	// Close file
	if err := w.currentFile.Close(); err != nil {
		return err
	}
	w.currentFile = nil
	return nil
}

// Write writes a blob to current segment
func (w *SegmentWriter) Write(key base.Key, value []byte) error {
	// Open new segment if needed or if this write would exceed segment size
	if w.currentFile == nil || w.currentPos+int64(len(value)) > w.segmentSize {
		if err := w.openNewSegment(); err != nil {
			return err
		}
	}

	if w.useDirectIO {
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
	} else {
		// Buffered write
		w.lastWritePos = w.currentPos

		// Simple buffered write
		n, err := w.currentFile.Write(value)
		if err != nil {
			return err
		}
		w.currentPos += int64(n)

		// Fdatasync after each write if enabled (not needed for DirectIO)
		if w.fsync {
			if err := fdatasync(w.currentFile); err != nil {
				return fmt.Errorf("failed to fdatasync after write: %w", err)
			}
		}
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
