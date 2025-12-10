package blobcache

import (
	"fmt"
	"os"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ncw/directio"
)

// SegmentWriter writes multiple blobs into large segment files
type SegmentWriter struct {
	paths       SegmentPaths
	segmentSize int64
	io          IOConfig
	resilience  ResilienceConfig
	workerID    int

	// Current segment state
	currentFile  *os.File
	currentID    int64
	currentPos   int64
	lastWritePos int64           // Position of last Write() for Pos()
	leftover     []byte          // For DirectIO alignment
	records      []SegmentRecord // Records for footer
}

// NewSegmentWriter creates a segment writer for a specific worker
func NewSegmentWriter(
	basePath string, segmentSize int64, workerID int,
	io IOConfig, resilience ResilienceConfig,
) *SegmentWriter {
	return &SegmentWriter{
		paths:       SegmentPaths(basePath),
		segmentSize: segmentSize,
		io:          io,
		resilience:  resilience,
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

	// Generate segment ID: timestamp for uniqueness
	w.currentID = time.Now().UnixNano()

	// Get segment path from path manager
	segmentPath := w.paths.SegmentPath(w.currentID, w.workerID)

	var err error
	if w.io.DirectIO {
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
func (w *SegmentWriter) closeCurrentSegment() error {
	if w.currentFile == nil {
		return nil
	}

	// Write footer with all records for this segment
	if len(w.records) > 0 {
		// Encode footer (p0 is where footer starts = current position)
		footerBytes := EncodeFooter(w.currentPos, w.records, w.resilience.Checksums)

		// Write footer to file
		if _, err := w.currentFile.Write(footerBytes); err != nil {
			w.currentFile.Close()
			return fmt.Errorf("failed to write footer: %w", err)
		}
	}

	// Fdatasync on close if enabled (after footer written)
	if w.io.Fsync {
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
func (w *SegmentWriter) Write(key Key, value []byte, checksum uint32) error {
	// Open new segment if needed or if this write would exceed segment size
	if w.currentFile == nil || w.currentPos+int64(len(value)) > w.segmentSize {
		if err := w.openNewSegment(); err != nil {
			return err
		}
	}

	// Track record for footer
	// Note: Hash uses xxhash.Sum64 (for path generation compatibility)
	// not the checksum hasher (which is for data integrity)
	rec := SegmentRecord{
		Hash:     xxhash.Sum64(key),
		Pos:      w.currentPos,
		Size:     int64(len(value)),
		Checksum: checksum,
	}
	w.records = append(w.records, rec)

	if w.io.DirectIO {
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
		if w.io.Fsync {
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
