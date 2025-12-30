package blobcache

import (
	"io"
)

// WritePosition tracks where data was written (for index)
type WritePosition struct {
	SegmentID int64 // Unique segment file identifier
	Pos       int64 // Byte offset within segment file
}

// BlobWriter abstracts blob file writing strategies
type BlobWriter interface {
	io.Closer

	// Write writes a blob
	// checksum is the blob's checksum (0 if not computed)
	Write(key Key, value []byte, checksum uint64) error

	// Pos returns the current write position (for index tracking)
	// Called after Write() to get position for the last written blob
	Pos() WritePosition

	// Fd returns segment file descriptor.
	Fd() uintptr
}
