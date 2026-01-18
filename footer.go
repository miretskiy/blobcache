package blobcache

import (
	"time"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
)

// IndexSegmentExtension is the file extension for segment index files.
// These contain the SegmentFooter data written separately from the data segment.
const IndexSegmentExtension = ".iseg"

// poolProvider allows WriteFooter to acquire hardware-aligned buffers
// for footer serialization without heap allocations.
type poolProvider interface {
	AcquireAligned(size int64) *MmapBuffer
}

// WriteFooter writes a segment footer to a separate .iseg file.
// This is a stateless function that:
// 1. Computes min/max SeqID from entries
// 2. Builds SegmentFooter struct
// 3. Serializes to 4KB-aligned buffer
// 4. Writes atomically using WriteBulkAligned
//
// The footer file path is derived from dataPath by adding .iseg extension.
// Example: /data/segments/0001/00000001.seg -> /data/segments/0001/00000001.seg.iseg
func WriteFooter(
	segmentID uint32,
	entries []record.FooterEntry,
	dataPath string,
	pool poolProvider,
	flags sys.OpenFlag,
) error {
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

	// 4. Write atomically to .iseg file
	indexPath := dataPath + IndexSegmentExtension
	return sys.WriteBulkAligned(indexPath, data, flags)
}
