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
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// Archivist manages read-only access to persisted segments.
// It uses the Index Item contract: Offset points to Magic, PhysicalLen = 42 + KeyLen + PhysSize.
type Archivist struct {
	config
	index *index.DurableIndex
	cache sync.Map // segmentID (uint32) -> SegmentFile
}

func NewArchivist(cfg config, idx *index.DurableIndex) *Archivist {
	return &Archivist{config: cfg, index: idx}
}

// Close closes all cached segment mu
func (a *Archivist) Close() error {
	var errs []error
	a.cache.Range(func(key, value any) bool {
		if closer, ok := value.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		a.cache.Delete(key)
		return true
	})
	return errors.Join(errs...)
}

// ReadBlob returns an io.Reader for the specified index entry.
// It handles segment file lookup, kernel prefetching hints, decompression, and checksum verification.
// The caller MUST call the returned Releaser when done with the reader.
//
// The lean Item only stores coordinates; full metadata (LogicalSize, Checksum) is
// read from the on-disk record.Header.
//
// expectedKey is used to verify the stored key matches (detects 128-bit hash collisions).
func (a *Archivist) ReadBlob(e index.Item, expectedKey []byte) (io.Reader, Releaser, error) {
	sf, err := a.getSegmentFile(e.SegmentID)
	if err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: segment %d not found: %w", e.SegmentID, err)
	}

	// PhysicalLen in Item is header+key+value total. For value-only, subtract header.
	// Item.PhysicalLen = record.HeaderSize + keyLen + physicalValueSize
	// We need to read the header to get keyLen and physicalValueSize.
	headerBuf := make([]byte, record.HeaderSize)
	if _, err := sf.file.ReadAt(headerBuf, int64(e.Offset)); err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: failed to read header: %w", err)
	}

	hdr, err := record.DecodeHeader(headerBuf)
	if err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: invalid header: %w", err)
	}

	// Key verification: detect 128-bit hash collisions (birthday paradox, ~10^-22 probability).
	// Records must have keys stored - reject records without keys.
	if hdr.KeyLen == 0 {
		return nil, Releaser{}, fmt.Errorf("storage: record has no key (KeyLen=0)")
	}
	storedKey := make([]byte, hdr.KeyLen)
	keyPos := int64(e.Offset) + int64(record.HeaderSize)
	if _, err := sf.file.ReadAt(storedKey, keyPos); err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: failed to read key: %w", err)
	}
	if !bytes.Equal(storedKey, expectedKey) {
		return nil, Releaser{}, record.ErrKeyMismatch
	}

	// 1. Kernel Hinting (Hybrid I/O)
	// Prefetch header + value. Fadvise is advisory - errors are logged but not fatal.
	if a.IO.Fadvise {
		totalSize := int64(e.PhysicalLen)
		if err := sys.Fadvise(sf.file.Fd(), sys.Offset_t(e.Offset), totalSize, sys.FadvSequential); err != nil {
			log.Warn("fadvise failed", "segID", e.SegmentID, "err", err)
		}
	}

	// 2. Read data/decompress if needed.
	// Skip past the record header and key to read just the value.
	valuePos := int64(e.Offset) + int64(record.HeaderSize) + int64(hdr.KeyLen)
	var reader io.Reader = io.NewSectionReader(sf, valuePos, hdr.PhysicalSize)
	var releaser Releaser
	if e.IsCompressed() {
		// Acquire buffer for compressed data
		compressedHandle := AcquireBuffer(int(hdr.PhysicalSize), int(hdr.PhysicalSize))
		defer compressedHandle.Release() // Release after decompression

		// Read all compressed bytes
		if n, err := io.ReadFull(reader, compressedHandle.Bytes()); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, Releaser{}, &base.TruncatedError{Expected: hdr.PhysicalSize, Got: int64(n)}
			}
			return nil, Releaser{}, err // IO error, returned as-is
		}

		// Acquire buffer for decompressed data (using LogicalSize from header)
		decompressedHandle := AcquireBuffer(int(hdr.LogicalSize), int(hdr.LogicalSize))
		if err := compression.Decompress(e.Compression(), decompressedHandle.Bytes(), compressedHandle.Bytes()); err != nil {
			decompressedHandle.Release()
			return nil, Releaser{}, err // CompressionError, returned as-is
		}

		reader = bytes.NewReader(decompressedHandle.Bytes())
		releaser = Releaser{bh: decompressedHandle}
	}

	// 3. Optional Integrity Layer
	// Checksum is computed on ORIGINAL (uncompressed) data.
	if a.Resilience.VerifyOnRead && hdr.HasValidCRC() && a.Resilience.ChecksumHasher != nil {
		reader = newChecksumVerifyingReader(reader, a.Resilience.ChecksumHasher, hdr.CRC())
	}

	return reader, releaser, nil
}

// getSegmentPath returns the path for a segment file
func getSegmentPath(basePath string, numShards int, segmentID uint32) string {
	shardNo := segmentID % uint32(max(1, numShards))
	return filepath.Join(basePath, "segments",
		fmt.Sprintf("%04d", shardNo),
		fmt.Sprintf("%d.seg", segmentID),
	)
}

// GetFooterPath returns the path for a segment's footer file (.iseg).
func GetFooterPath(basePath string, numShards int, segmentID uint32) string {
	return getSegmentPath(basePath, numShards, segmentID) + IndexSegmentExtension
}

// getSegmentFile returns cached SegmentFile or opens it
func (a *Archivist) getSegmentFile(segmentID uint32) (*segmentFile, error) {
	// 1. Check the LRU/Map handle cache
	if cached, ok := a.cache.Load(segmentID); ok {
		return cached.(*segmentFile), nil
	}

	// 2. Open the file
	path := getSegmentPath(a.Path, a.Shards, segmentID)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// 3. Verify the Index knows this segment exists
	if _, ok := a.index.GetSegmentManifest(segmentID); !ok {
		_ = f.Close()
		return nil, fmt.Errorf("storage: segment %d unknown to index", segmentID)
	}

	sf := &segmentFile{file: f, segID: segmentID}

	// 4. Cache the handle
	actual, loaded := a.cache.LoadOrStore(segmentID, sf)
	if loaded {
		_ = sf.Close()
		return actual.(*segmentFile), nil
	}

	return sf, nil
}

// HolePunchBlob releases disk space for an evicted blob.
func (a *Archivist) HolePunchBlob(
	segmentID uint32, offset uint32, physicalLen uint32,
) (int64, error) {
	sf, err := a.getSegmentFile(segmentID)
	if err != nil {
		return 0, err
	}
	return sf.PunchHole(int64(offset), int64(physicalLen))
}
