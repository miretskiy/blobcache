package blobcache

import (
	"fmt"
	"os"
)

// BufferedWriter writes blobs using standard buffered I/O with atomic temp+rename
type BufferedWriter struct {
	paths BlobPaths
	fsync bool
}

// NewBufferedWriter creates a buffered blob writer
func NewBufferedWriter(basePath string, shards int, fsync bool) *BufferedWriter {
	return &BufferedWriter{
		paths: NewBlobPaths(basePath, shards),
		fsync: fsync,
	}
}

// Write writes a blob atomically using temp file + rename
func (w *BufferedWriter) Write(key Key, value []byte, checksum uint32) error {
	tempPath := w.paths.TempBlobPath(key)
	finalPath := w.paths.BlobPath(key)

	// Write to temp file
	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := f.Write(value); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Fdatasync if enabled (data only, not metadata)
	if w.fsync {
		if err := fdatasync(f); err != nil {
			f.Close()
			os.Remove(tempPath)
			return fmt.Errorf("failed to fdatasync: %w", err)
		}
	}

	if err := f.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename to final location
	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename to final path: %w", err)
	}

	return nil
}

// Pos returns empty position (per-blob mode doesn't use segments)
func (w *BufferedWriter) Pos() WritePosition {
	return WritePosition{}
}

// Close is a no-op for BufferedWriter (no persistent state)
func (w *BufferedWriter) Close() error {
	return nil
}
