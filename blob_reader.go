package blobcache

import (
	"io"

	"github.com/miretskiy/blobcache/base"
)

// BlobReader abstracts blob reading strategies
type BlobReader interface {
	io.Closer

	// Get retrieves a blob by key
	// Returns (reader, true) if found, (nil, false) if not found or checksum mismatch
	// Readers handle checksum verification internally if enabled
	Get(key base.Key) (io.Reader, bool)
}
