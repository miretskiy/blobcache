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
	return &Archivist{
		config: cfg,
		index:  idx,
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

	if a.IO.DirectIORead {
		return a.readBlobDirect(sf, e, expectedKey)
	}
	return a.readBlobBuffered(sf, e, expectedKey)
}

// readBlobBuffered reads a record using buffered I/O (kernel page cache).
func (a *Archivist) readBlobBuffered(sf *os.File, e index.Item, expectedKey []byte) ([]byte, Releaser, error) {
	handle := AcquireBuffer(int(e.PhysicalLen), int(e.PhysicalLen))
	buf := handle.Bytes()
	if _, err := sf.ReadAt(buf, int64(e.Offset)); err != nil {
		handle.Release()
		return nil, Releaser{}, fmt.Errorf("storage: read failed: %w", err)
	}

	return a.parseRecord(buf, e, expectedKey, Releaser{bh: &handle}, func() { handle.Release() })
}

// readBlobDirect reads a record using Direct I/O with aligned buffers.
func (a *Archivist) readBlobDirect(sf *os.File, e index.Item, expectedKey []byte) ([]byte, Releaser, error) {
	alignedOff, alignedLen := sys.AlignRange(int64(e.Offset), int(e.PhysicalLen))
	handle := AcquireAlignedBuffer(int(alignedLen), int(alignedLen))
	buf := handle.Bytes()

	if _, err := sys.PreadAligned(sf, buf, alignedOff, sys.FlDirectIO); err != nil {
		handle.Release()
		return nil, Releaser{}, fmt.Errorf("storage: direct read failed: %w", err)
	}

	lead := int(int64(e.Offset) - alignedOff)
	rec := buf[lead : lead+int(e.PhysicalLen)]

	return a.parseRecord(rec, e, expectedKey, Releaser{bh: &handle}, func() { handle.Release() })
}

// parseRecord parses a record buffer, handles decompression and checksum verification.
// owner is the Releaser that owns the buffer backing rec. onError is called to free
// the buffer on failure paths (before decompression replaces it).
func (a *Archivist) parseRecord(rec []byte, e index.Item, expectedKey []byte, owner Releaser, onError func()) ([]byte, Releaser, error) {
	hdr, err := record.DecodeHeader(rec[:record.HeaderSize])
	if err != nil {
		onError()
		return nil, Releaser{}, fmt.Errorf("storage: invalid header: %w", err)
	}

	keyEnd := record.HeaderSize + int(hdr.KeyLen)
	if expectedKey != nil && !bytes.Equal(rec[record.HeaderSize:keyEnd], expectedKey) {
		onError()
		return nil, Releaser{}, record.ErrKeyMismatch
	}

	valueData := rec[keyEnd:]
	releaser := owner

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
			onError()
			decompressedHandle.Release()
			return nil, Releaser{}, err
		}
		owner.Release() // Free the read buffer; decompressed data is independent.
		valueData = decompressedHandle.Bytes()
		releaser = Releaser{bh: &decompressedHandle}
	}

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
	if cached, ok := a.cache.Load(segmentID); ok {
		return cached.(*os.File), nil
	}

	path := getSegmentPath(a.Path, a.Shards, segmentID)
	var flags sys.OpenFlag
	if a.IO.DirectIORead {
		flags |= sys.FlDirectIO
	}
	f, err := sys.OpenFileForRead(path, flags)
	if err != nil {
		return nil, err
	}

	// fadvise is meaningless with Direct I/O (no page cache).
	if a.IO.Fadvise && !a.IO.DirectIORead {
		if err := sys.Fadvise(f.Fd(), 0, 0, sys.FadvRandom); err != nil {
			log.Warn("fadvise failed", "segID", segmentID, "err", err)
		}
	}

	actual, loaded := a.cache.LoadOrStore(segmentID, f)
	if loaded {
		_ = f.Close()
		return actual.(*os.File), nil
	}

	return f, nil
}

// DropSegmentCache closes and removes a segment's cached file handle.
// Called before deleting segment files during compaction.
func (a *Archivist) DropSegmentCache(segmentID uint32) {
	if val, ok := a.cache.LoadAndDelete(segmentID); ok {
		_ = val.(*os.File).Close()
	}
}
