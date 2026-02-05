package blobcache

import (
	"bytes"
	"context"
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
	"golang.org/x/time/rate"
)

// Archivist manages read-only access to persisted segments.
// It uses the Index Item contract: Offset points to Magic, PhysicalLen = 42 + KeyLen + PhysSize.
type Archivist struct {
	config
	index         *index.DurableIndex
	cache         sync.Map      // segmentID (uint32) -> *os.File
	punchLimiter  *rate.Limiter // Rate limiter for hole punch syscalls
}

func NewArchivist(cfg config, idx *index.DurableIndex) *Archivist {
	return &Archivist{
		config: cfg,
		index:  idx,
		// Rate limit hole punching to 2000 syscalls/sec with burst of 100.
		// Protects foreground read throughput from "Metadata Storms" during heavy eviction.
		punchLimiter: rate.NewLimiter(rate.Limit(2000), 100),
	}
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

// GetFooterPath returns the path for a segment's metadata file (.meta).
func GetFooterPath(basePath string, numShards int, segmentID uint32) string {
	return SegmentMetaPath(getSegmentPath(basePath, numShards, segmentID))
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

// HoleRange represents a contiguous range to punch within a segment.
// Used by CoalesceVictims to merge adjacent evicted blobs into single syscalls.
type HoleRange struct {
	SegmentID uint32
	Offset    int64
	Length    int64
}

// HolePunchRange releases disk space for a pre-coalesced range.
// This is the batched version of HolePunchBlob, used after CoalesceVictims
// merges adjacent holes to reduce filesystem journal commits.
//
// Rate-limited to 2000 syscalls/sec to protect foreground read throughput
// from "Metadata Storms" during heavy eviction.
func (a *Archivist) HolePunchRange(ctx context.Context, segmentID uint32, offset, length int64) (int64, error) {
	// Rate limit to protect foreground reads
	if err := a.punchLimiter.Wait(ctx); err != nil {
		return 0, err
	}
	sf, err := a.getSegmentFile(segmentID)
	if err != nil {
		return 0, err
	}
	return sys.PunchHole(sf, offset, length)
}

// DropSegmentCache closes and removes a segment's cached file handle.
// Called before deleting segment files during compaction.
func (a *Archivist) DropSegmentCache(segmentID uint32) {
	if val, ok := a.cache.LoadAndDelete(segmentID); ok {
		_ = val.(*os.File).Close()
	}
}
