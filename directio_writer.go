package blobcache

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/miretskiy/blobcache/base"
	"github.com/ncw/directio"
)

// DirectIOWriter writes blobs using DirectIO with aligned writes, truncate, and atomic rename
type DirectIOWriter struct {
	basePath string
	shards   int
}

// NewDirectIOWriter creates a DirectIO blob writer
func NewDirectIOWriter(basePath string, shards int) *DirectIOWriter {
	return &DirectIOWriter{
		basePath: basePath,
		shards:   shards,
	}
}

// isAligned is defined in platform-specific files (directio_darwin.go, directio_linux.go)

// Write writes a blob atomically using DirectIO with temp file + truncate + rename
func (w *DirectIOWriter) Write(key base.Key, value []byte) error {
	// Build file paths
	shardDir := fmt.Sprintf("shard-%03d", key.ShardID())
	blobFile := fmt.Sprintf("%d.blob", key.FileID())
	shardPath := filepath.Join(w.basePath, "blobs", shardDir)
	tempPath := filepath.Join(shardPath, fmt.Sprintf(".tmp-%d.blob", key.FileID()))
	finalPath := filepath.Join(shardPath, blobFile)

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
