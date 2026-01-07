package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

// RecoverIndex scans all segment mu and rebuilds the index from scratch.
// It bypasses Cache-level orchestration (eviction/bloom) to ensure a clean rebuild.
func RecoverIndex(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	log.Info("starting index recovery", "path", path)

	segmentsDir := filepath.Join(path, "segments")
	dbPath := filepath.Join(path, "db")
	tempParentPath := path + "_recovery"
	tempDBPath := filepath.Join(tempParentPath, "db")

	// 1. Prepare Workspace
	_ = os.RemoveAll(tempParentPath)
	if err := os.MkdirAll(tempParentPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create recovery directory: %w", err)
	}

	// Create a raw Index. We talk to this directly, bypassing c.PutBatch().
	recoveryIdx, err := index.NewIndex(tempParentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create recovery index: %w", err)
	}

	corruptCount := 0
	validCount := 0

	// 2. Scan Shards & Segments
	numShards := max(1, cfg.Shards)
	for shard := 0; shard < numShards; shard++ {
		shardDir := filepath.Join(segmentsDir, fmt.Sprintf("%04d", shard))
		if _, err := os.Stat(shardDir); os.IsNotExist(err) {
			continue
		}

		entries, _ := os.ReadDir(shardDir)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
				continue
			}

			segmentPath := filepath.Join(shardDir, entry.Name())
			segmentID := extractSegmentID(entry.Name())

			segment, err := readSegmentFooter(segmentPath, segmentID)
			if err != nil {
				log.Warn("corrupt segment file, removing", "path", segmentPath, "error", err)
				_ = os.Remove(segmentPath)
				corruptCount++
				continue
			}

			// USE THE LOWER LEVEL PRIMITIVE:
			// IngestBatch directly updates the Skipmap/Sieve metadata.
			if err := recoveryIdx.IngestBatch(segment.SegmentID, segment.Records); err != nil {
				recoveryIdx.Close()
				return nil, fmt.Errorf("recovery ingestion failed for seg %d: %w", segment.SegmentID, err)
			}
			validCount++
		}
	}

	recoveryIdx.Close()

	// 3. Swap Index Folders
	_ = os.RemoveAll(dbPath)
	if err := os.Rename(tempDBPath, dbPath); err != nil {
		return nil, fmt.Errorf("failed to swap recovery index: %w", err)
	}
	_ = os.RemoveAll(tempParentPath)

	// 4. Final Assembly
	// Re-open the index at the proper path
	idx, err := index.NewIndex(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open recovered index: %w", err)
	}

	c := &Cache{
		config:  cfg,
		index:   idx,
		storage: NewStorage(cfg, idx),
		stopCh:  make(chan struct{}),
	}
	c.memTable = NewMemTable(c.config, c, c)

	// Build Bloom Filter synchronously
	log.Info("rebuilding bloom filter from recovered segments...")
	if err := c.rebuildBloom(); err != nil {
		return nil, fmt.Errorf("failed to build bloom filter: %w", err)
	}

	// Set starting size by scanning the index truth
	var totalSize int64
	c.index.ForEachBlob(func(e index.Entry) bool {
		totalSize += e.LogicalSize
		return true
	})
	c.approxSize.Store(totalSize)

	log.Info("recovery complete", "valid", validCount, "corrupt", corruptCount, "total_mb", totalSize/(1024*1024))
	return c, nil
}

// readSegmentFooter reads and validates a segment file's footer
// Returns the SegmentRecord if valid, or an error if corrupt/incomplete
func readSegmentFooter(path string, segmentID int64) (metadata.SegmentRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return metadata.SegmentRecord{}, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return metadata.SegmentRecord{}, err
	}

	segment, _, err := metadata.ReadSegmentFooterFromFile(file, stat.Size(), segmentID)
	return segment, err
}

// extractSegmentID extracts the segment SegmentID from a filename like "123456.seg"
func extractSegmentID(filename string) int64 {
	var id int64
	// Parse filename: "123456.seg" -> 123456
	fmt.Sscanf(filename, "%d.seg", &id)
	return id
}
