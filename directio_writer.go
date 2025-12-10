package blobcache

import (
	"fmt"
	"os"

	"github.com/ncw/directio"
)

// DirectIOWriter writes blobs using DirectIO with aligned writes, truncate, and atomic rename
type DirectIOWriter struct {
	paths BlobPaths
	fsync bool
}

// NewDirectIOWriter creates a DirectIO blob writer
func NewDirectIOWriter(basePath string, shards int, fsync bool) *DirectIOWriter {
	return &DirectIOWriter{
		paths: NewBlobPaths(basePath, shards),
		fsync: fsync,
	}
}

// isAligned is defined in platform-specific files (directio_darwin.go, directio_linux.go)

// Write writes a blob atomically using DirectIO with temp file + truncate + rename
func (w *DirectIOWriter) Write(key Key, value []byte, checksum uint32) error {
	tempPath := w.paths.TempBlobPath(key)
	finalPath := w.paths.BlobPath(key)

	// Open temp file with DirectIO
	f, err := directio.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open temp file with DirectIO: %w", err)
	}
	defer f.Close()

	actualSize := len(value)
	const mask = directio.BlockSize - 1
	paddedSize := (actualSize + mask) &^ mask

	// Determine buffer to write
	var bufToWrite []byte
	if isAligned(value) && len(value) == paddedSize {
		// Data already padded to block size boundary, use directly
		bufToWrite = value
	} else {
		// Allocate aligned+padded buffer and copy data
		bufToWrite = directio.AlignedBlock(paddedSize)
		copy(bufToWrite, value)
	}

	// Write aligned data
	if _, err := f.Write(bufToWrite); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Fdatasync if enabled (data only, not metadata)
	if w.fsync {
		if err := fdatasync(f); err != nil {
			os.Remove(tempPath)
			return fmt.Errorf("failed to fdatasync: %w", err)
		}
	}

	// Close file before truncate
	if err := f.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Truncate to actual size (remove padding if needed)
	if actualSize != paddedSize {
		if err := os.Truncate(tempPath, int64(actualSize)); err != nil {
			os.Remove(tempPath)
			return fmt.Errorf("failed to truncate file: %w", err)
		}
	}

	// Atomic rename to final location
	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename to final path: %w", err)
	}

	return nil
}

// Pos returns empty position (per-blob mode doesn't use segments)
func (w *DirectIOWriter) Pos() WritePosition {
	return WritePosition{}
}

// Close is a no-op for DirectIOWriter (no persistent state)
func (w *DirectIOWriter) Close() error {
	return nil
}
