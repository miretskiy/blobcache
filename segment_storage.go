package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/compression"
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
// It handles segment file lookup, kernel prefetching hints, decompression, and checksum verification.
// The caller MUST call the returned Releaser when done with the reader.
func (s *Storage) ReadBlob(e index.Entry) (io.Reader, Releaser, error) {
	sf, err := s.getSegmentFile(e.SegmentID)
	if err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: segment %d not found: %w", e.SegmentID, err)
	}

	// 1. Kernel Hinting (Hybrid I/O)
	// Use PhysicalSize - this is the actual bytes stored on disk.
	if s.IO.Fadvise {
		_ = Fadvise(sf.file.Fd(), Offset_t(e.Pos), e.PhysicalSize, FadvSequential)
	}

	// 2. Read data/decompress if needed.
	var reader io.Reader = io.NewSectionReader(sf, e.Pos, e.PhysicalSize)
	var releaser Releaser
	if e.IsCompressed() {
		// Acquire buffer for compressed data
		compressedHandle := AcquireBuffer(int(e.PhysicalSize), int(e.PhysicalSize))
		defer compressedHandle.Release() // Release after decompression

		// Read all compressed bytes
		if n, err := io.ReadFull(reader, compressedHandle.Bytes()); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, Releaser{}, &base.TruncatedError{Expected: e.PhysicalSize, Got: int64(n)}
			}
			return nil, Releaser{}, err // IO error, returned as-is
		}

		// Acquire buffer for decompressed data
		decompressedHandle := AcquireBuffer(int(e.LogicalSize), int(e.LogicalSize))
		if err := compression.Decompress(e.Compression(), decompressedHandle.Bytes(), compressedHandle.Bytes()); err != nil {
			decompressedHandle.Release()
			return nil, Releaser{}, err // CompressionError, returned as-is
		}

		reader = bytes.NewReader(decompressedHandle.Bytes())
		releaser = Releaser{buffer: decompressedHandle}
	}

	// 3. Optional Integrity Layer
	// Checksum is computed on ORIGINAL (uncompressed) data.
	if s.Resilience.VerifyOnRead && e.HasChecksum() {
		reader = newChecksumVerifyingReader(reader, s.Resilience.ChecksumHasher, e.Checksum())
	}

	return reader, releaser, nil
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
func (s *Storage) HolePunchBlob(segmentID int64, offset, size int64) (int64, error) {
	sf, err := s.getSegmentFile(segmentID)
	if err != nil {
		return 0, err
	}
	return sf.PunchHole(offset, size)
}
