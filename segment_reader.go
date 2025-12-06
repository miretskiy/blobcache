package blobcache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
)

// SegmentReader reads blobs from segment files with file handle caching
type SegmentReader struct {
	basePath string

	mu    sync.RWMutex
	cache map[int64]*os.File // segmentID -> open file handle
}

// NewSegmentReader creates a segment reader
func NewSegmentReader(basePath string) *SegmentReader {
	return &SegmentReader{
		basePath: basePath,
		cache:    make(map[int64]*os.File),
	}
}

// Get reads a blob from a segment file at the specified position
func (r *SegmentReader) Get(key base.Key, entry *index.Entry) (io.Reader, bool) {
	// Get or open segment file
	file, err := r.getSegmentFile(entry.SegmentID)
	if err != nil {
		return nil, false
	}

	// Return SectionReader for the blob's range in the file
	// SectionReader doesn't close the underlying cached file
	return io.NewSectionReader(file, entry.Pos, int64(entry.Size)), true
}

// getSegmentFile returns cached file or opens it
func (r *SegmentReader) getSegmentFile(segmentID int64) (*os.File, error) {
	// Check cache with read lock
	r.mu.RLock()
	if file, ok := r.cache[segmentID]; ok {
		r.mu.RUnlock()
		return file, nil
	}
	r.mu.RUnlock()

	// Open file with write lock
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if file, ok := r.cache[segmentID]; ok {
		return file, nil
	}

	// Open segment file
	segmentPath := filepath.Join(r.basePath, "segments", fmt.Sprintf("%d.seg", segmentID))
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, err
	}

	r.cache[segmentID] = file
	return file, nil
}

// Close closes all cached file handles
func (r *SegmentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, file := range r.cache {
		file.Close()
	}
	r.cache = make(map[int64]*os.File)
	return nil
}
