package blobcache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/miretskiy/blobcache/index"
)

type Storage struct {
	config
	index *index.Index
	cache sync.Map // segmentID (int64) -> SegmentFile
}

func NewStorage(cfg config, idx *index.Index) *Storage {
	s := Storage{config: cfg, index: idx}
	return &s
}

// Close closes all cached segment mu
func (s *Storage) Close() error {
	var errs []error
	s.cache.Range(func(key, value any) bool {
		if closer, ok := value.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		s.cache.Delete(key)
		return true
	})
	return errors.Join(errs...)
}

// ReadBlob returns an io.Reader for the specified index entry.
// It handles segment file lookup, kernel prefetching hints, and checksum verification.
func (s *Storage) ReadBlob(e index.Entry) (io.Reader, error) {
	sf, err := s.getSegmentFile(e.SegmentID)
	if err != nil {
		return nil, fmt.Errorf("storage: segment %d not found: %w", e.SegmentID, err)
	}

	// 1. Kernel Hinting (Hybrid I/O)
	// Since reads are buffered, we tell the kernel exactly what we are about to read.
	if s.IO.Fadvise {
		_ = Fadvise(sf.file.Fd(), Offset_t(e.Pos), e.LogicalSize, FadvSequential)
	}

	// 2. Base Reader
	// SectionReader provides a view into the segment file without moving the file pointer.
	var reader io.Reader = io.NewSectionReader(sf, e.Pos, e.LogicalSize)

	// 3. Optional Integrity Layer
	// We wrap the reader so the checksum is verified as the user consumes the data.
	if s.Resilience.VerifyOnRead && e.HasChecksum() {
		reader = newChecksumVerifyingReader(reader, s.Resilience.ChecksumHasher, e.Checksum())
	}

	return reader, nil
}

// getSegmentPath returns the path for a segment file
func getSegmentPath(basePath string, numShards int, segmentID int64) string {
	shardNo := segmentID % int64(max(1, numShards))
	return filepath.Join(basePath, "segments",
		fmt.Sprintf("%04d", shardNo),
		fmt.Sprintf("%d.seg", segmentID),
	)
}

// getSegmentFile returns cached SegmentFile or opens it
func (s *Storage) getSegmentFile(segmentID int64) (*segmentFile, error) {
	// 1. Check the LRU/Map handle cache
	if cached, ok := s.cache.Load(segmentID); ok {
		return cached.(*segmentFile), nil
	}

	// 2. Open the file
	path := getSegmentPath(s.Path, s.Shards, segmentID)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// 3. Verify the Index knows this segment exists
	if _, ok := s.index.GetSegmentRecord(segmentID); !ok {
		_ = f.Close()
		return nil, fmt.Errorf("storage: segment %d unknown to index", segmentID)
	}

	sf := &segmentFile{file: f, segID: segmentID}

	// 4. Cache the handle
	actual, loaded := s.cache.LoadOrStore(segmentID, sf)
	if loaded {
		_ = sf.Close()
		return actual.(*segmentFile), nil
	}

	return sf, nil
}

// tryReadFooterFromFile attempts to read and validate segment record from file footer
func (s *Storage) HolePunchBlob(segmentID int64, offset, size int64) error {
	sf, err := s.getSegmentFile(segmentID)
	if err != nil {
		return err
	}
	return sf.PunchHole(offset, size)
}
