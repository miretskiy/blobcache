package index

import (
	"path/filepath"

	"go.mills.io/bitcask/v2"

	"github.com/miretskiy/blobcache/metadata"
)

// make it explicit that the values should be smaller than this.
const maxValueSize = 64 << 10

// when storing segment records, this is how many records we can store per segment.
const maxBlobsPerSegment = int(maxValueSize-metadata.SegmentRecordHeaderSize) / metadata.EncodedBlobRecordSize

// openOrCreateBitcask creates or opens bitcask segments rooted at basePath.
func openOrCreateBitcask(basePath string) (*bitcask.Bitcask, error) {
	dbPath := filepath.Join(basePath, "db")
	return bitcask.Open(dbPath, bitcask.WithMaxValueSize(maxValueSize))
}
