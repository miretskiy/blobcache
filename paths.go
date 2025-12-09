package blobcache

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"
)

// BlobPaths encapsulates basePath and sharding configuration for per-blob storage
type BlobPaths struct {
	basePath string
	shards   int
}

// NewBlobPaths creates a path generator with sharding configuration
func NewBlobPaths(basePath string, shards int) BlobPaths {
	return BlobPaths{basePath: basePath, shards: shards}
}

// hash computes shardID and fileID from key
func (p BlobPaths) hash(key []byte) (shardID int, fileID uint64) {
	h := xxhash.Sum64(key)
	return int(h % uint64(p.shards)), h >> 32
}

// BlobPath returns the path for a blob file
func (p BlobPaths) BlobPath(key Key) string {
	shardID, fileID := p.hash(key)
	return filepath.Join(p.basePath, "blobs",
		fmt.Sprintf("shard-%03d", shardID),
		fmt.Sprintf("%d.blob", fileID))
}

// TempBlobPath returns a temporary path for atomic writes
func (p BlobPaths) TempBlobPath(key Key) string {
	shardID, fileID := p.hash(key)
	shardDir := filepath.Join(p.basePath, "blobs", fmt.Sprintf("shard-%03d", shardID))
	return filepath.Join(shardDir, fmt.Sprintf(".tmp-%d-%d.blob", fileID, time.Now().UnixNano()))
}

// ShardDir returns the shard directory path
func (p BlobPaths) ShardDir(shardID int) string {
	return filepath.Join(p.basePath, "blobs", fmt.Sprintf("shard-%03d", shardID))
}

// SegmentPaths is a basePath for segment storage with path generation methods
type SegmentPaths string

// SegmentPath returns the path for a segment file
func (p SegmentPaths) SegmentPath(segmentID int64, workerID int) string {
	return filepath.Join(string(p), "segments", fmt.Sprintf("%d-%02d.seg", segmentID, workerID))
}

// SegmentPattern returns glob pattern for finding segment files by ID
func (p SegmentPaths) SegmentPattern(segmentID int64) string {
	return filepath.Join(string(p), "segments", fmt.Sprintf("%d-*.seg", segmentID))
}

// SegmentsDir returns the segments directory path
func (p SegmentPaths) SegmentsDir() string {
	return filepath.Join(string(p), "segments")
}
