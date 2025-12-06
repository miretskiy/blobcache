package blobcache

import (
	"io"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/index"
)

// BlobReader abstracts blob reading strategies
type BlobReader interface {
	io.Closer

	// Get retrieves a blob as a Reader
	// Returns (reader, true) if found, (nil, false) if not found
	// For file-backed readers, may return io.Closer that caller should close
	Get(key base.Key, entry *index.Entry) (io.Reader, bool)
}
