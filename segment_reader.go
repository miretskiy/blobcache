package blobcache

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
)

// SegmentReader reads blobs from segment files with file handle caching
type SegmentReader struct {
	basePath     string
	cache        sync.Map // segmentID (int64) -> *os.File
	index        index.Indexer
	checksumHash func() hash.Hash32
	verifyOnRead bool
}

// NewSegmentReader creates a segment reader
func NewSegmentReader(basePath string, idx index.Indexer, checksumHash func() hash.Hash32, verifyOnRead bool) *SegmentReader {
	return &SegmentReader{
		basePath:     basePath,
		index:        idx,
		checksumHash: checksumHash,
		verifyOnRead: verifyOnRead,
	}
}

// Get reads a blob from a segment file at the specified position
func (r *SegmentReader) Get(key base.Key) (io.Reader, bool) {
	// Lookup position from index
	var record index.Record
	if err := r.index.Get(context.TODO(), key, &record); err != nil {
		return nil, false
	}

	// Get or open segment file
	file, err := r.getSegmentFile(record.SegmentID)
	if err != nil {
		return nil, false
	}

	// Read blob data from segment
	data := make([]byte, record.Size)
	n, err := file.ReadAt(data, record.Pos)
	if err != nil && err != io.EOF {
		return nil, false
	}
	if n != record.Size {
		return nil, false
	}

	reader := bytes.NewReader(data)

	// Wrap with checksum verification if enabled
	if r.verifyOnRead && r.checksumHash != nil && record.HasChecksum {
		return newChecksumVerifyingReader(reader, r.checksumHash, record.Checksum), true
	}

	return reader, true
}

// getSegmentFile returns cached file or opens it
func (r *SegmentReader) getSegmentFile(segmentID int64) (*os.File, error) {
	// Check cache
	if cached, ok := r.cache.Load(segmentID); ok {
		return cached.(*os.File), nil
	}

	// Find segment file with workerID suffix
	pattern := filepath.Join(r.basePath, "segments", fmt.Sprintf("%d-*.seg", segmentID))
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return nil, fmt.Errorf("segment file not found for ID %d", segmentID)
	}

	segmentPath := matches[0] // Take first match
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, err
	}

	// Store in cache (LoadOrStore handles race)
	actual, _ := r.cache.LoadOrStore(segmentID, file)
	if actual != file {
		// Another goroutine opened it first, close ours and use theirs
		file.Close()
		return actual.(*os.File), nil
	}

	return file, nil
}

// Close closes all cached file handles
func (r *SegmentReader) Close() error {
	r.cache.Range(func(key, value any) bool {
		if file, ok := value.(*os.File); ok {
			file.Close()
		}
		r.cache.Delete(key)
		return true
	})
	return nil
}

// RemoveSegment closes and removes a segment from cache (called when segment is deleted)
func (r *SegmentReader) RemoveSegment(segmentID int64) {
	if cached, ok := r.cache.LoadAndDelete(segmentID); ok {
		if file, ok := cached.(*os.File); ok {
			file.Close()
		}
	}
}
