package blobcache

import (
	"github.com/miretskiy/blobcache/base"
)

// BlobWriter abstracts blob file writing strategies
type BlobWriter interface {
	// Write writes a blob atomically
	// Returns error if write fails
	Write(key base.Key, value []byte) error
}
