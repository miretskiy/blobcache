package blobcache

import (
	"bytes"
	"context"
	"hash"
	"io"
	"os"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
)

// BufferedReader reads blobs from individual shard files
type BufferedReader struct {
	paths        BlobPaths
	index        index.Indexer
	checksumHash func() hash.Hash32
	verifyOnRead bool
}

// NewBufferedReader creates a buffered blob reader
func NewBufferedReader(basePath string, shards int, idx index.Indexer, checksumHash func() hash.Hash32, verifyOnRead bool) *BufferedReader {
	return &BufferedReader{
		paths:        BlobPaths(basePath),
		index:        idx,
		checksumHash: checksumHash,
		verifyOnRead: verifyOnRead,
	}
}

// Get reads a blob from its deterministic shard/file path
func (r *BufferedReader) Get(key base.Key) (io.Reader, bool) {
	// Read file data
	data, err := os.ReadFile(r.paths.BlobPath(key))
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
