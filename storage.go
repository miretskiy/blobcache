package blobcache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/miretskiy/blobcache/index"
)

// StorageStrategy handles storage-specific operations (eviction, path generation)
type StorageStrategy interface {
	// Evict removes oldest data until targetBytes freed
	// Returns actual bytes evicted
	Evict(ctx context.Context, targetBytes int64) (int64, error)
}

// BlobStorage implements per-blob storage strategy
type BlobStorage struct {
	paths  BlobPaths
	shards int
	index  index.Indexer
}

// NewBlobStorage creates a per-blob storage strategy
func NewBlobStorage(basePath string, shards int, idx index.Indexer) *BlobStorage {
	return &BlobStorage{
		paths:  NewBlobPaths(basePath, shards),
		shards: shards,
		index:  idx,
	}
}

// Evict removes oldest blobs until targetBytes freed
func (s *BlobStorage) Evict(ctx context.Context, targetBytes int64) (int64, error) {
	// Range all records and collect them
	var records []index.KeyValue
	if err := s.index.Range(ctx, func(rec index.KeyValue) bool {
		records = append(records, rec)
		return true
	}); err != nil {
		return 0, fmt.Errorf("failed to scan index: %w", err)
	}

	// Sort by ctime (oldest first)
	sort.Slice(records, func(i, j int) bool {
		return records[i].Val.CTime < records[j].Val.CTime
	})

	// Evict oldest until target reached
	evictedBytes := int64(0)
	evictedCount := 0

	for _, record := range records {
		if evictedBytes >= targetBytes {
			break
		}

		// Delete blob file using centralized path generation
		blobPath := s.paths.BlobPath(record.Key)
		if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: failed to delete blob %s: %v\n", blobPath, err)
		}

		// Delete from index
		if err := s.index.Delete(ctx, record.Key); err != nil {
			fmt.Printf("Warning: failed to delete key from index: %v\n", err)
		}

		evictedBytes += int64(record.Val.Size)
		evictedCount++
	}

	fmt.Printf("Evicted %d blobs (%d MB)\n", evictedCount, evictedBytes/(1024*1024))
	return evictedBytes, nil
}

// SegmentStorage implements segment-based storage strategy
type SegmentStorage struct {
	paths SegmentPaths
	index index.Indexer
}

// NewSegmentStorage creates a segment storage strategy
func NewSegmentStorage(basePath string, idx index.Indexer) *SegmentStorage {
	return &SegmentStorage{
		paths: SegmentPaths(basePath),
		index: idx,
	}
}

// Evict removes oldest segments until targetBytes freed
func (s *SegmentStorage) Evict(ctx context.Context, targetBytes int64) (int64, error) {
	// Range index to build segment size map
	segmentSizes := make(map[int64][]index.KeyValue)

	if err := s.index.Range(ctx, func(rec index.KeyValue) bool {
		segmentSizes[rec.Val.SegmentID] = append(segmentSizes[rec.Val.SegmentID], rec)
		return true
	}); err != nil {
		return 0, fmt.Errorf("failed to scan index: %w", err)
	}

	// Sort segments by minimum CTime (oldest record in segment)
	type segmentInfo struct {
		id      int64
		minTime int64
		records []index.KeyValue
		bytes   int64
	}

	var segments []segmentInfo
	for segID, records := range segmentSizes {
		minTime := records[0].Val.CTime
		totalBytes := int64(0)
		for _, rec := range records {
			if rec.Val.CTime < minTime {
				minTime = rec.Val.CTime
			}
			totalBytes += int64(rec.Val.Size)
		}
		segments = append(segments, segmentInfo{
			id:      segID,
			minTime: minTime,
			records: records,
			bytes:   totalBytes,
		})
	}

	// Sort by minTime (oldest segments first)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].minTime < segments[j].minTime
	})

	// Evict segments until target reached
	evictedBytes := int64(0)
	evictedSegments := 0
	evictedRecords := 0

	for _, seg := range segments {
		if evictedBytes >= targetBytes {
			break
		}

		// Find and delete segment file using centralized path generation
		matches, _ := filepath.Glob(s.paths.SegmentPattern(seg.id))
		for _, segPath := range matches {
			if err := os.Remove(segPath); err != nil && !os.IsNotExist(err) {
				fmt.Printf("Warning: failed to delete segment %s: %v\n", segPath, err)
			}
		}

		// Delete all records for this segment
		for _, record := range seg.records {
			if err := s.index.Delete(ctx, record.Key); err != nil {
				fmt.Printf("Warning: failed to delete key from index: %v\n", err)
			}
		}

		evictedBytes += seg.bytes
		evictedSegments++
		evictedRecords += len(seg.records)

		fmt.Printf("Evicted segment %d: %d records (%d MB)\n",
			seg.id, len(seg.records), seg.bytes/(1024*1024))
	}

	fmt.Printf("Evicted %d segments with %d records (%d MB total)\n",
		evictedSegments, evictedRecords, evictedBytes/(1024*1024))
	return evictedBytes, nil
}
