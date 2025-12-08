package blobcache

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
)

// BufferedReader reads blobs from individual shard files
type BufferedReader struct {
	basePath     string
	shards       int
	index        index.Indexer
	checksumHash func() hash.Hash32
	verifyOnRead bool
}

// NewBufferedReader creates a buffered blob reader
func NewBufferedReader(basePath string, shards int, idx index.Indexer, checksumHash func() hash.Hash32, verifyOnRead bool) *BufferedReader {
	return &BufferedReader{
		basePath:     basePath,
		shards:       shards,
		index:        idx,
		checksumHash: checksumHash,
		verifyOnRead: verifyOnRead,
	}
}

// Get reads a blob from its deterministic shard/file path
func (r *BufferedReader) Get(key base.Key) (io.Reader, bool) {
	// Build path from key
	shardDir := fmt.Sprintf("shard-%03d", key.ShardID())
	blobFile := fmt.Sprintf("%d.blob", key.FileID())
	blobPath := filepath.Join(r.basePath, "blobs", shardDir, blobFile)

	// Read file data
	data, err := os.ReadFile(blobPath)
	if err != nil {
		return nil, false
	}

	reader := bytes.NewReader(data)

	// Wrap with checksum verification if enabled
	if r.verifyOnRead && r.checksumHash != nil {
		// Lookup checksum from index
		var record index.Record
		if err := r.index.Get(context.TODO(), key, &record); err != nil {
			// Not in index, skip verification
			return reader, true
		}

		if record.HasChecksum {
			return newChecksumVerifyingReader(reader, r.checksumHash, record.Checksum), true
		}
	}

	return reader, true
}

// Close is a no-op for BufferedReader (no cached state)
func (r *BufferedReader) Close() error {
	return nil
}
