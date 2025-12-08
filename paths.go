package blobcache

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/miretskiy/blobcache/base"
)

// BlobPaths is a basePath for per-blob storage with path generation methods
type BlobPaths string

// BlobPath returns the path for a blob file
func (p BlobPaths) BlobPath(key base.Key) string {
	return filepath.Join(string(p), "blobs",
		fmt.Sprintf("shard-%03d", key.ShardID()),
		fmt.Sprintf("%d.blob", key.FileID()))
}

// TempBlobPath returns a temporary path for atomic writes
func (p BlobPaths) TempBlobPath(key base.Key) string {
	shardDir := filepath.Join(string(p), "blobs", fmt.Sprintf("shard-%03d", key.ShardID()))
	return filepath.Join(shardDir, fmt.Sprintf(".tmp-%d-%d.blob", key.FileID(), time.Now().UnixNano()))
}

// ShardDir returns the shard directory path
func (p BlobPaths) ShardDir(shardID int) string {
	return filepath.Join(string(p), "blobs", fmt.Sprintf("shard-%03d", shardID))
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
