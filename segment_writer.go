package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// poolProvider allows SegmentWriter to acquire hardware-aligned buffers
// for final footer serialization without heap allocations.
type poolProvider interface {
	AcquireAligned(size int64) *MmapBuffer
}

// SegmentWriter manages long-running, sequential writes to a large segment file.
// It ingests multiple MemTable slabs (e.g., 128MB each) and accumulates
// footer entries until the segment is full or explicitly sealed.
type SegmentWriter struct {
	id         int64
	file       *os.File
	currentPos int64
	pool       poolProvider
	entries    []record.FooterEntry
	syncData   bool
}

// NewSegmentWriter initializes a large-scale segment file. If directIO is true,
// uses O_DIRECT (Linux) or F_NOCACHE (Darwin) to bypass the OS page cache,
// ensuring that massive sequential writes do not "pollute" RAM.
func NewSegmentWriter(
	id int64, path string, segmentSize int64, pool poolProvider, syncData bool, directIO bool,
) (*SegmentWriter, error) {
	// 1. Ensure the parent directory structure exists.
	// This creates "segments/0000/" recursively if they are missing.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory %s: %w", dir, err)
	}

	// 2. Now open the file
	f, err := sys.OpenDirect(path, directIO)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %d: %w", id, err)
	}

	// 3. Pre-allocate
	if err := sys.Fallocate(f, segmentSize); err != nil {
		log.Error("warning: fallocate failed", "segID", id, "err", err)
	}

	return &SegmentWriter{
		id:       id,
		file:     f,
		pool:     pool,
		syncData: syncData,
	}, nil
}

// WriteSlab appends a memory-aligned slab of data to the segment.
// 'data' MUST be 4KB-aligned (via MmapBuffer.AlignedBytes()) or the write
// will fail with EINVAL on Linux systems.
// Returns the entries with Pos transformed from relative to absolute positions.
func (sw *SegmentWriter) WriteSlab(
	data []byte, entries []record.FooterEntry,
) ([]record.FooterEntry, error) {
	if len(data) == 0 {
		return entries, nil
	}

	// If some previous operation left us unaligned, we must fail fast.
	if sw.currentPos%4096 != 0 {
		return nil, fmt.Errorf("segment offset %d is not 4KB-aligned; O_DIRECT write will fail", sw.currentPos)
	}

	// Safety check for alignment (abstracted helper)
	if !sys.IsAligned(data) {
		return nil, fmt.Errorf("buffer address %p is not hardware-aligned", &data[0])
	}

	// 1. Capture the start of this block in the file
	slabStart := sw.currentPos

	// 2. Write the bytes to disk at the absolute offset
	if _, err := sw.file.WriteAt(data, slabStart); err != nil {
		return nil, err
	}

	if sw.syncData {
		if err := sys.Fdatasync(sw.file); err != nil {
			return nil, fmt.Errorf("fdatasync failed: %w", err)
		}
	}

	// 3. Advance the global file pointer by the size of the data written
	sw.currentPos += int64(len(data))

	// 4. Transform to absolute positions (create copy, don't mutate input)
	numEntries := len(sw.entries)
	for i := range entries {
		entry := entries[i]
		entry.Pos += slabStart
		sw.entries = append(sw.entries, entry)
	}

	return sw.entries[numEntries:], nil
}

// Close finalizes and "seals" the segment. It appends the immutable
// "Birth Snapshot" footer block, which includes the index for every blob
// written since the file was opened. Once closed, the segment is read-only.
func (sw *SegmentWriter) Close() error {
	if sw.file == nil {
		return nil
	}

	// 1. Construct the immutable Segment Footer
	sf := record.SegmentFooter{
		Entries:   sw.entries,
		SegmentID: sw.id,
		CTime:     time.Now().Unix(),
	}

	// 2. Serialize Footer into an Aligned Buffer.
	// We use the slabPool to satisfy O_DIRECT alignment and avoid GC pressure.
	physicalMetaSize := record.SegmentFooterPhysicalSize(len(sw.entries))
	tmpBuf := sw.pool.AcquireAligned(physicalMetaSize)
	defer tmpBuf.Unpin()

	// AppendSegmentFooterWithTail ensures the 20-byte legacy tail is pinned
	// to the absolute end of the 4KB-aligned block.
	paddedMetadata := record.AppendSegmentFooterWithTail(tmpBuf.Bytes(), sf)

	// 3. Final hardware write for the metadata block.
	if _, err := sw.file.WriteAt(paddedMetadata, sw.currentPos); err != nil {
		_ = sw.file.Close()
		return fmt.Errorf("failed to write segment metadata: %w", err)
	}
	finalSize := sw.currentPos + int64(len(paddedMetadata))

	// 4. Truncate to actual size so footer is at file end.
	// fallocate pre-allocates more space; ReadSegmentFooterFromFile expects
	// the footer at fileSize - footerSize.
	if err := sw.file.Truncate(finalSize); err != nil {
		_ = sw.file.Close()
		return fmt.Errorf("failed to truncate segment: %w", err)
	}

	// 5. Persistence Handshake.
	// fdatasync uses F_FULLFSYNC on Darwin to ensure it clears the drive cache.
	if sw.syncData {
		if err := sys.Fdatasync(sw.file); err != nil {
			_ = sw.file.Close()
			return fmt.Errorf("fdatasync failed on segment close: %w", err)
		}
	}

	err := sw.file.Close()
	sw.file = nil
	return err
}

// CurrentPos returns the total bytes written to the segment so far.
func (sw *SegmentWriter) CurrentPos() int64 {
	return sw.currentPos
}

// Fd exposes the file descriptor for fadvise calls.
func (sw *SegmentWriter) Fd() uintptr {
	if sw.file == nil {
		return 0
	}
	return sw.file.Fd()
}
