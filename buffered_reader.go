package blobcache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
)

// BufferedReader reads blobs from individual shard files
type BufferedReader struct {
	basePath string
	shards   int
}

// NewBufferedReader creates a buffered blob reader
func NewBufferedReader(basePath string, shards int) *BufferedReader {
	return &BufferedReader{
		basePath: basePath,
		shards:   shards,
	}
}

// Get reads a blob from its deterministic shard/file path
func (r *BufferedReader) Get(key base.Key, entry *index.Entry) (io.Reader, bool) {
	// Build path from key (entry not used in per-blob mode)
	shardDir := fmt.Sprintf("shard-%03d", key.ShardID())
	blobFile := fmt.Sprintf("%d.blob", key.FileID())
	blobPath := filepath.Join(r.basePath, "blobs", shardDir, blobFile)

	file, err := os.Open(blobPath)
	if err != nil {
		return nil, false
	}

	return file, true
}

// Close is a no-op for BufferedReader (no cached state)
func (r *BufferedReader) Close() error {
	return nil
}
