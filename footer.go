package blobcache

import (
	"path/filepath"
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// IndexSegmentExtension is the file extension for segment metadata files.
// These files contain the Base Manifest (SegmentFooter struct at offset 0)
// followed by a stream of Tombstone Batches appended on delete operations.
// The base manifest is written by WriteFooter during segment flush.
const IndexSegmentExtension = ".meta"

// SegmentMetaPath converts a segment data path to its metadata path.
// Example: "/data/segments/0001/123.seg" -> "/data/segments/0001/123.meta"
func SegmentMetaPath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	return segmentPath[:len(segmentPath)-len(ext)] + IndexSegmentExtension
}

// poolProvider allows WriteFooter to acquire hardware-aligned buffers
// for footer serialization without heap allocations.
type poolProvider interface {
	AcquireAligned(size int64) *MmapBuffer
}

// WriteFooter writes a segment footer to a separate .meta file.
// This is a stateless function that:
// 1. Computes min/max SeqID from entries
// 2. Builds SegmentFooter struct
// 3. Serializes to 4KB-aligned buffer
// 4. Writes atomically using WriteFile
//
// The footer file path is derived from dataPath by replacing .seg extension with .meta.
// Example: /data/segments/0001/123.seg -> /data/segments/0001/123.meta
//
// Note: O_DIRECT is stripped for metadata writes. The complexity/risk of alignment
// constraints on small metadata files (~2-8MB) outweighs any performance benefit
// from bypassing the page cache. Buffered I/O is robust and fast for this scale.
func WriteFooter(
	segmentID uint32,
	entries []record.FooterEntry,
	dataPath string,
	pool poolProvider,
	flags sys.OpenFlag,
) error {
	// Strip O_DIRECT for metadata - buffered I/O is safer and sufficient for small files.
	flags &^= sys.FlDirectIO
	// 1. Compute min/max SeqID
	var minSeq, maxSeq uint64
	if len(entries) > 0 {
		minSeq = entries[0].SeqID
		maxSeq = entries[0].SeqID
		for i := 1; i < len(entries); i++ {
			if entries[i].SeqID < minSeq {
				minSeq = entries[i].SeqID
			}
			if entries[i].SeqID > maxSeq {
				maxSeq = entries[i].SeqID
			}
		}
	}

	// 2. Construct SegmentFooter
	sf := record.SegmentFooter{
		SegmentID:   int64(segmentID),
		CTime:       time.Now().Unix(),
		MinSeqID:    minSeq,
		MaxSeqID:    maxSeq,
		RecordCount: int64(len(entries)),
		Entries:     entries,
	}

	// 3. Serialize into aligned buffer
	physicalSize := record.SegmentFooterAlignedSize(len(entries))
	buf := pool.AcquireAligned(physicalSize)
	defer buf.Unpin()

	data := record.AppendFooterBlock(buf.Bytes(), sf)

	// 4. Write atomically to .meta file
	indexPath := SegmentMetaPath(dataPath)
	return sys.WriteFile(indexPath, data, flags)
}
