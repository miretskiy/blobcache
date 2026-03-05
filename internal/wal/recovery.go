package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/dio/align"
)

// ComputeRecoveryCheckpoint returns the highest SeqID across all segment envelopes.
// WAL entries with SeqID > checkpoint need to be replayed during recovery.
func ComputeRecoveryCheckpoint(segments []record.SegmentFooter) uint64 {
	var maxSeqID uint64
	for _, seg := range segments {
		if seg.MaxSeqID > maxSeqID {
			maxSeqID = seg.MaxSeqID
		}
	}
	return maxSeqID
}

// recoverFile replays all records from a single WAL file.
// Handles O_DIRECT padding by skipping zero regions to the next 4KB boundary.
func (w *WAL) recoverFile(path string, applyFn func(record.Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get file size for EOF detection
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	// Read and validate header
	headerBuf := make([]byte, FileHeaderSize)
	if _, err := io.ReadFull(f, headerBuf); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if _, err := DecodeFileHeader(headerBuf); err != nil {
		return fmt.Errorf("invalid header: %w", err)
	}

	pos := int64(FileHeaderSize)
	recHeaderBuf := make([]byte, record.HeaderSize)

	for pos < fileSize {
		// Read record header
		n, err := f.ReadAt(recHeaderBuf, pos)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			break
		}
		if n < record.HeaderSize {
			break
		}

		hdr, err := record.DecodeHeader(recHeaderBuf)
		if err != nil || !hdr.IsValid() {
			// O_DIRECT padding: skip to next 4KB boundary
			nextBlock := align.PageAlign(pos + 1)
			if nextBlock >= fileSize {
				break
			}
			pos = nextBlock
			continue
		}

		// Read full record (header + payload)
		payloadSize := hdr.PayloadSize()
		fullSize := record.HeaderSize + payloadSize

		// Sanity check: payload can't exceed remaining file size
		// (protects against corrupted headers with garbage sizes)
		if payloadSize < 0 || int64(fullSize) > fileSize-pos {
			nextBlock := align.PageAlign(pos + 1)
			if nextBlock >= fileSize {
				break
			}
			pos = nextBlock
			continue
		}

		fullBuf := make([]byte, fullSize)

		if _, err = f.ReadAt(fullBuf, pos); err != nil {
			break // Incomplete record at end of file
		}

		// Decode with CRC verification
		rec, err := record.DecodeRecord(fullBuf, true)
		if err != nil {
			// CRC mismatch - skip to next block boundary
			pos = align.PageAlign(pos + 1)
			continue
		}

		// Apply the record
		if err := applyFn(rec); err != nil {
			return fmt.Errorf("apply seqID=%d: %w", rec.SeqID, err)
		}

		pos += int64(fullSize)
	}

	return nil
}

// ScanMaxSeqID finds the highest SeqID in a WAL file.
// Used to determine if a WAL file can be safely truncated.
// Handles O_DIRECT padding by skipping zero regions to the next 4KB boundary.
func ScanMaxSeqID(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Get file size for EOF detection
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := stat.Size()

	var maxSeq uint64
	headerBuf := make([]byte, record.HeaderSize)
	pos := int64(FileHeaderSize)

	for pos < fileSize {
		n, err := f.ReadAt(headerBuf, pos)
		if err != nil || n < record.HeaderSize {
			break
		}

		hdr, err := record.DecodeHeader(headerBuf)
		if err != nil || !hdr.IsValid() {
			// O_DIRECT padding: skip to next 4KB boundary
			nextBlock := align.PageAlign(pos + 1)
			if nextBlock >= fileSize {
				break
			}
			pos = nextBlock
			continue
		}

		if hdr.SeqID > maxSeq {
			maxSeq = hdr.SeqID
		}

		// Advance past this record
		pos += int64(record.HeaderSize) + int64(hdr.PayloadSize())
	}

	return maxSeq, nil
}

// ListWALFiles returns all WAL file first SeqIDs that exist on disk.
// Used during recovery to find which slabs have WAL data.
func (w *WAL) ListWALFiles() ([]uint64, error) {
	files, err := w.listWALFiles()
	if err != nil {
		return nil, err
	}

	firstIDs := make([]uint64, 0, len(files))
	for _, path := range files {
		firstID, err := parseWALFileName(filepath.Base(path))
		if err == nil {
			firstIDs = append(firstIDs, firstID)
		}
	}
	return firstIDs, nil
}

// Replayer is implemented by types that can replay WAL records during recovery.
// The simple interface allows WAL to stay decoupled from Cache/MemTable internals.
type Replayer interface {
	// ReplayRecord writes a recovered record to the memtable.
	// The record should be written as-is (original SeqID, CRC, no compression).
	ReplayRecord(rec record.Record) error

	// Flush triggers a flush of the current memtable contents to segment.
	Flush()

	// Drain waits for all pending flushes to complete.
	Drain()
}

// Recover replays all WAL files that need recovery.
// For each WAL file:
//  1. Check if already committed (using isCommitted callback)
//  2. If committed, delete the file
//  3. If not, replay all records, then flush
//
// After all files are processed, Drain() is called.
// WAL files are deleted by the normal flush path (via DeleteFile after segment write).
// Returns (recovered, error) where recovered=true if any WAL files were replayed.
func (w *WAL) Recover(replayer Replayer, isCommitted func(firstSeqID uint64) bool) (bool, error) {
	files, err := w.listWALFiles()
	if err != nil {
		return false, fmt.Errorf("list WAL files: %w", err)
	}

	var recovered bool
	for _, path := range files {
		firstID, err := parseWALFileName(filepath.Base(path))
		if err != nil {
			continue // Skip malformed files
		}

		// Check if this WAL's data is already in a committed segment
		if isCommitted != nil && isCommitted(firstID) {
			// Data already in segment, just delete the orphaned WAL file
			_ = w.DeleteFile(firstID)
			continue
		}

		// Replay all records from this file
		err = w.recoverFile(path, func(rec record.Record) error {
			return replayer.ReplayRecord(rec)
		})
		if err != nil {
			return false, fmt.Errorf("recover file %s: %w", path, err)
		}

		recovered = true

		// Flush after each file - triggers segment write which deletes WAL file
		replayer.Flush()
	}

	// Wait for all flushes to complete (WAL files deleted during flush)
	replayer.Drain()

	return recovered, nil
}
