package blobcache

import (
	"io"
)

// WritePosition tracks where data was written (for index)
type WritePosition struct {
	SegmentID int64 // Segment ID (0 for per-blob mode)
	Pos       int64 // Position within segment (0 for per-blob mode)
}

// BlobWriter abstracts blob file writing strategies
type BlobWriter interface {
	io.Closer

	// Write writes a blob
	Write(key Key, value []byte) error

	// Pos returns the current write position (for index tracking)
	// Called after Write() to get position for the last written blob
	Pos() WritePosition
}
