package blobcache

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/miretskiy/blobcache/index"
)

// BufferedReader reads blobs from individual shard files
type BufferedReader struct {
	paths        BlobPaths
	index        index.Indexer
	hasher       Hasher
	verifyOnRead bool
}

// NewBufferedReader creates a buffered blob reader
func NewBufferedReader(
	basePath string, numShards int, idx index.Indexer,
	hasher Hasher, verifyOnRead bool,
) *BufferedReader {
	return &BufferedReader{
		paths:        NewBlobPaths(basePath, numShards),
		index:        idx,
		hasher:       hasher,
		verifyOnRead: verifyOnRead,
	}
}

// Get reads a blob from its deterministic shard/file path
func (r *BufferedReader) Get(key Key) (io.Reader, bool) {
	// Read file data
	data, err := os.ReadFile(r.paths.BlobPath(key))
	if err != nil {
		return nil, false
	}

	reader := bytes.NewReader(data)

	// Wrap with checksum verification if enabled
	if r.verifyOnRead && r.hasher != nil {
		// Lookup checksum from index
		var record index.Record
		if err := r.index.Get(context.TODO(), key, &record); err != nil {
			// Not in index, skip verification
			return reader, true
		}

		if record.HasChecksum {
			return newChecksumVerifyingReader(reader, r.hasher, record.Checksum), true
		}
	}

	return reader, true
}

// Close is a no-op for BufferedReader (no cached state)
func (r *BufferedReader) Close() error {
	return nil
}
