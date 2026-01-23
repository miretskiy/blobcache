package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
)

// TODO: Code is sloopy and probably wrong; ignroing errors is a no-no.
// Iterating over all blobs to compute total size -- not needed, I think.

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
	recoveryIdx, err := index.OpenIndex(tempParentPath, 100000)
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

			// Convert FooterEntry to lean Items for index
			items := make([]index.Item, len(segment.Entries))
			for i, e := range segment.Entries {
				physicalLen := uint32(record.HeaderSize) + uint32(e.KeyLen) + uint32(e.PhysicalSize)
				items[i] = index.Item{
					Key:         e.Key, // Full 128-bit XXH3 hash
					SegmentID:   segmentID,
					Offset:      uint32(e.Pos),
					PhysicalLen: physicalLen,
				}
				items[i].SetCompression(e.Compression())
			}

			// USE THE LOWER LEVEL PRIMITIVE:
			// IngestBatch directly updates the Skipmap/Sieve metadata.
			// Use MaxSeqID from segment envelope
			if err := recoveryIdx.IngestBatch(segmentID, items, segment.MaxSeqID); err != nil {
				recoveryIdx.Close()
				return nil, fmt.Errorf("recovery ingestion failed for seg %d: %w", segmentID, err)
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
	idx, err := index.OpenIndex(path, 100000)
	if err != nil {
		return nil, fmt.Errorf("failed to open recovered index: %w", err)
	}

	c := &Cache{
		config:    cfg,
		index:     idx,
		archivist: NewArchivist(cfg, idx),
		segIDs:    newSegmentIDProvider(cfg.Path, cfg.Shards),
		stopCh:    make(chan struct{}),
	}
	c.librarian = NewLibrarian(cfg.MaxCachedSlabs, c)
	c.Knobs = cfg.knobs
	c.memTable = NewMemTable(c.config, c, c, c.librarian, nil, c.segIDs) // No WAL during recovery
	c.memTable.Knobs = c.Knobs

	// Build Bloom Filter synchronously
	log.Info("rebuilding bloom filter from recovered segments...")
	if err := c.rebuildBloom(); err != nil {
		return nil, fmt.Errorf("failed to build bloom filter: %w", err)
	}

	// Set starting size by scanning the index truth
	// Note: PhysicalLen is total record size (header + key + value)
	var totalSize int64
	c.index.ForEachBlob(func(e index.Item) bool {
		totalSize += int64(e.PhysicalLen)
		return true
	})
	c.approxSize.Store(totalSize)

	log.Info("recovery complete", "valid", validCount, "corrupt", corruptCount, "total_mb", totalSize/(1024*1024))
	return c, nil
}

// readSegmentFooter reads and validates a segment's footer from its .iseg file.
// Returns the SegmentFooter if valid, or an error if corrupt/missing.
func readSegmentFooter(segmentPath string, segmentID uint32) (record.SegmentFooter, error) {
	indexPath := segmentPath + IndexSegmentExtension
	file, err := os.Open(indexPath)
	if err != nil {
		return record.SegmentFooter{}, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return record.SegmentFooter{}, err
	}

	// ReadFooterBlock uses int64 segmentID for backward compatibility
	segment, _, err := record.ReadFooterBlock(file, stat.Size(), int64(segmentID))
	return segment, err
}

// extractSegmentID extracts the segment SegmentID from a filename like "123456.seg"
func extractSegmentID(filename string) uint32 {
	var id uint32
	// Parse filename: "123456.seg" -> 123456
	fmt.Sscanf(filename, "%d.seg", &id)
	return id
}
