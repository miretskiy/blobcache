package blobcache

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/miretskiy/blobcache/base"
)

// BufferedWriter writes blobs using standard buffered I/O with atomic temp+rename
type BufferedWriter struct {
	basePath string
	shards   int
}

// NewBufferedWriter creates a buffered blob writer
func NewBufferedWriter(basePath string, shards int) *BufferedWriter {
	return &BufferedWriter{
		basePath: basePath,
		shards:   shards,
	}
}

// Write writes a blob atomically using temp file + rename
func (w *BufferedWriter) Write(key base.Key, value []byte) error {
	// Build file paths
	shardDir := fmt.Sprintf("shard-%03d", key.ShardID())
	blobFile := fmt.Sprintf("%d.blob", key.FileID())
	shardPath := filepath.Join(w.basePath, "blobs", shardDir)
	tempPath := filepath.Join(shardPath, fmt.Sprintf(".tmp-%d.blob", key.FileID()))
	finalPath := filepath.Join(shardPath, blobFile)

	// Write to temp file
	if err := os.WriteFile(tempPath, value, 0o644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
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
