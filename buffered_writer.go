package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/miretskiy/blobcache/base"
)

// BufferedWriter writes blobs using standard buffered I/O with atomic temp+rename
type BufferedWriter struct {
	basePath string
	shards   int
	fsync    bool
}

// NewBufferedWriter creates a buffered blob writer
func NewBufferedWriter(basePath string, shards int, fsync bool) *BufferedWriter {
	return &BufferedWriter{
		basePath: basePath,
		shards:   shards,
		fsync:    fsync,
	}
}

// Write writes a blob atomically using temp file + rename
func (w *BufferedWriter) Write(key base.Key, value []byte) error {
	// Build file paths
	shardDir := fmt.Sprintf("shard-%03d", key.ShardID())
	blobFile := fmt.Sprintf("%d.blob", key.FileID())
	shardPath := filepath.Join(w.basePath, "blobs", shardDir)
	// Include timestamp to avoid collisions on retry
	tempPath := filepath.Join(shardPath, fmt.Sprintf(".tmp-%d-%d.blob", key.FileID(), time.Now().UnixNano()))
	finalPath := filepath.Join(shardPath, blobFile)

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
