package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

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
	cache sync.Map // segmentID (uint32) -> *os.File
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

// ReadBlobRaw reads raw record bytes from a segment. No interpretation.
// Caller is responsible for any validation or parsing.
// Used by Compactor for copying blobs during compaction.
func (a *Archivist) ReadBlobRaw(e index.Item) ([]byte, Releaser, error) {
	sf, err := a.getSegmentFile(e.SegmentID)
	if err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: segment %d not found: %w", e.SegmentID, err)
	}

	handle := AcquireBuffer(int(e.PhysicalLen), int(e.PhysicalLen))
	if _, err := sf.ReadAt(handle.Bytes(), int64(e.Offset)); err != nil {
		handle.Release()
		return nil, Releaser{}, fmt.Errorf("storage: read failed: %w", err)
	}

	return handle.Bytes(), Releaser{bh: &handle}, nil
}

// ReadBlob returns the value bytes for the specified index entry.
// It handles decompression and checksum verification.
// The caller MUST call the returned Releaser when done with the data.
//
// expectedKey is used to verify the stored key matches (detects 128-bit hash collisions).
func (a *Archivist) ReadBlob(e index.Item, expectedKey []byte) ([]byte, Releaser, error) {
	sf, err := a.getSegmentFile(e.SegmentID)
	if err != nil {
		return nil, Releaser{}, fmt.Errorf("storage: segment %d not found: %w", e.SegmentID, err)
	}

	// Single read of entire record
	handle := AcquireBuffer(int(e.PhysicalLen), int(e.PhysicalLen))
	buf := handle.Bytes()
	if _, err := sf.ReadAt(buf, int64(e.Offset)); err != nil {
		handle.Release()
		return nil, Releaser{}, fmt.Errorf("storage: read failed: %w", err)
	}

	// Parse header to locate key and value
	hdr, err := record.DecodeHeader(buf[:record.HeaderSize])
	if err != nil {
		handle.Release()
		return nil, Releaser{}, fmt.Errorf("storage: invalid header: %w", err)
	}

	// Verify key matches (skip if expectedKey is nil - TrustHash mode)
	keyEnd := record.HeaderSize + int(hdr.KeyLen)
	if expectedKey != nil && !bytes.Equal(buf[record.HeaderSize:keyEnd], expectedKey) {
		handle.Release()
		return nil, Releaser{}, record.ErrKeyMismatch
	}

	// Extract value
	valueData := buf[keyEnd:]
	releaser := Releaser{bh: &handle}

	if e.IsCompressed() {
		decompressedHandle := AcquireBuffer(int(hdr.LogicalSize), int(hdr.LogicalSize))
		dstBuf := decompressedHandle.Bytes()

		if err := compression.Decompress(e.Compression(), dstBuf, valueData); err != nil {
			log.Error("decompression failed",
				"codec", e.Compression(),
				"logical_size", hdr.LogicalSize,
				"physical_size", hdr.PhysicalSize,
				"value_data_len", len(valueData),
				"dst_buf_len", len(dstBuf),
				"dst_buf_cap", cap(dstBuf),
				"error", err)
			handle.Release()
			decompressedHandle.Release()
			return nil, Releaser{}, err
		}
		handle.Release()
		valueData = decompressedHandle.Bytes()
		releaser = Releaser{bh: &decompressedHandle}
	}

	// Optional Integrity Layer
	if a.Resilience.VerifyOnRead && hdr.HasValidCRC() && a.Resilience.ChecksumHasher != nil {
		if err := verifyChecksum(valueData, a.Resilience.ChecksumHasher, hdr.CRC()); err != nil {
			releaser.Release()
			return nil, Releaser{}, err
		}
	}

	return valueData, releaser, nil
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
func (a *Archivist) getSegmentFile(segmentID uint32) (*os.File, error) {
	// 1. Check the LRU/Map handle cache
	if cached, ok := a.cache.Load(segmentID); ok {
		return cached.(*os.File), nil
	}

	// 2. OpenIndex the file
	path := getSegmentPath(a.Path, a.Shards, segmentID)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if a.IO.Fadvise {
		if err := sys.Fadvise(f.Fd(), 0, 0, sys.FadvRandom); err != nil {
			log.Warn("fadvise failed", "segID", segmentID, "err", err)
		}
	}

	// 2. Cache the handle
	actual, loaded := a.cache.LoadOrStore(segmentID, f)
	if loaded {
		_ = f.Close()
		return actual.(*os.File), nil
	}

	return f, nil
}

// HolePunchBlob releases disk space for an evicted blob.
func (a *Archivist) HolePunchBlob(
		segmentID uint32, offset uint32, physicalLen uint32,
) (int64, error) {
	sf, err := a.getSegmentFile(segmentID)
	if err != nil {
		return 0, err
	}
	return sys.PunchHole(sf, int64(offset), int64(physicalLen))
}

// DropSegmentCache closes and removes a segment's cached file handle.
// Called before deleting segment files during compaction.
func (a *Archivist) DropSegmentCache(segmentID uint32) {
	if val, ok := a.cache.LoadAndDelete(segmentID); ok {
		_ = val.(*os.File).Close()
	}
}
