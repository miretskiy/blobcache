package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

// RecoverIndex scans all segment files and rebuilds the index from scratch.
// It removes corrupt segment files or segments without complete/valid footers.
// This should be called instead of New() when recovery is needed.
func RecoverIndex(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	log.Info("starting index recovery", "path", path)

	// Ensure directory structure exists
	segmentsDir := filepath.Join(path, "segments")
	if _, err := os.Stat(segmentsDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("segments directory does not exist: %s", segmentsDir)
	}

	// Create temporary recovery directory that will contain the index
	// Note: index.NewIndex() creates a "db" subdirectory inside the path we provide
	// So we create a temporary parent directory, and the index will be at tempPath/db
	tempParentPath := path + "_recovery"
	dbPath := filepath.Join(path, "db")
	tempDBPath := filepath.Join(tempParentPath, "db")

	// Clean up any leftover recovery directory
	if err := os.RemoveAll(tempParentPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to clean recovery directory: %w", err)
	}
	if err := os.MkdirAll(tempParentPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create recovery directory: %w", err)
	}

	// Create temporary index for recovery
	// This will create tempParentPath/db internally
	recoveryIdx, err := index.NewIndex(tempParentPath)
	if err != nil {
		os.RemoveAll(tempParentPath)
		return nil, fmt.Errorf("failed to create recovery index: %w", err)
	}

	// Scan and rebuild index from segment files
	corruptCount := 0
	validCount := 0
	totalBlobs := 0

	// Scan all shard directories
	numShards := max(1, cfg.Shards)
	for shard := 0; shard < numShards; shard++ {
		shardDir := filepath.Join(segmentsDir, fmt.Sprintf("%04d", shard))

		// Check if shard directory exists
		if _, err := os.Stat(shardDir); os.IsNotExist(err) {
			continue // Skip non-existent shards
		}

		// Scan all segment files in this shard
		entries, err := os.ReadDir(shardDir)
		if err != nil {
			recoveryIdx.Close()
			os.RemoveAll(tempParentPath)
			return nil, fmt.Errorf("failed to read shard directory %s: %w", shardDir, err)
		}

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
				continue
			}

			segmentPath := filepath.Join(shardDir, entry.Name())
			segmentID := extractSegmentID(entry.Name())

			// Try to read and validate segment footer
			segment, err := readSegmentFooter(segmentPath, segmentID)
			if err != nil {
				log.Warn("corrupt or incomplete segment file, removing",
					"path", segmentPath,
					"error", err)

				// Remove corrupt segment file
				if removeErr := os.Remove(segmentPath); removeErr != nil {
					log.Error("failed to remove corrupt segment",
						"path", segmentPath,
						"error", removeErr)
				}
				corruptCount++
				continue
			}

			// Add valid segment records to recovery index
			// Skip deleted blobs (marked with Deleted flag)
			var kvs []index.KeyValue
			for _, rec := range segment.Records {
				if rec.IsDeleted() {
					continue // Skip deleted blobs
				}
				kvs = append(kvs, index.KeyValue{
					Key: index.Key(rec.Hash),
					Val: index.NewValueFrom(rec.Pos, rec.Size, rec.Flags, segment.SegmentID),
				})
			}

			if err := recoveryIdx.PutBatch(kvs); err != nil {
				recoveryIdx.Close()
				os.RemoveAll(tempParentPath)
				return nil, fmt.Errorf("failed to add segment %d to recovery index: %w", segment.SegmentID, err)
			}

			validCount++
			totalBlobs += len(kvs)
		}
	}

	// Close recovery index before renaming
	if err := recoveryIdx.Close(); err != nil {
		os.RemoveAll(tempParentPath)
		return nil, fmt.Errorf("failed to close recovery index: %w", err)
	}

	log.Info("segment scan completed",
		"valid_segments", validCount,
		"corrupt_segments", corruptCount,
		"total_blobs", totalBlobs)

	// Replace old index with recovery index
	// The recovery index is at tempParentPath/db
	// We want to move it to path/db

	// 1. Remove old index
	if err := os.RemoveAll(dbPath); err != nil && !os.IsNotExist(err) {
		os.RemoveAll(tempParentPath)
		return nil, fmt.Errorf("failed to remove old index: %w", err)
	}

	// 2. Rename recovery index (tempParentPath/db -> path/db)
	if err := os.Rename(tempDBPath, dbPath); err != nil {
		os.RemoveAll(tempParentPath)
		return nil, fmt.Errorf("failed to rename recovery index: %w", err)
	}

	// 3. Clean up temporary parent directory
	os.RemoveAll(tempParentPath)

	// Now open the recovered index normally
	idx, err := index.NewIndex(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open recovered index: %w", err)
	}

	log.Info("index recovery completed, building bloom filter")

	// Create bloom filter and populate from recovered index
	var totalSize int64
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	if err := idx.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			filter.Add(rec.Hash)
			totalSize += rec.Size
		}
		return true
	}); err != nil {
		return nil, fmt.Errorf("failed to build bloom filter: %w", err)
	}

	c := &Cache{
		config:  cfg,
		index:   idx,
		storage: NewStorage(cfg, idx),
		stopCh:  make(chan struct{}),
	}
	c.bloom.Store(filter)
	c.approxSize.Store(totalSize)
	c.memTable = c.newMemTable(c.config, c.storage)

	log.Info("recovery completed successfully")
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

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return metadata.SegmentRecord{}, err
	}
	fileSize := stat.Size()

	// File must be at least large enough for a footer
	if fileSize < metadata.SegmentFooterSize {
		return metadata.SegmentRecord{}, fmt.Errorf("file too small for footer: %d bytes", fileSize)
	}

	// Read footer from end of file
	footerBuf := make([]byte, metadata.SegmentFooterSize)
	footerPos := fileSize - metadata.SegmentFooterSize
	if _, err := file.ReadAt(footerBuf, footerPos); err != nil {
		return metadata.SegmentRecord{}, fmt.Errorf("failed to read footer: %w", err)
	}

	// Parse and validate footer
	footer, err := metadata.DecodeSegmentFooter(footerBuf)
	if err != nil {
		return metadata.SegmentRecord{}, fmt.Errorf("invalid footer: %w", err)
	}

	// Validate segment record length
	if footer.Len <= 0 || footer.Len > fileSize-metadata.SegmentFooterSize {
		return metadata.SegmentRecord{}, fmt.Errorf("invalid segment record length: %d", footer.Len)
	}

	// Read segment record
	segmentRecordPos := footerPos - footer.Len
	if segmentRecordPos < 0 {
		return metadata.SegmentRecord{}, fmt.Errorf("segment record position negative: %d", segmentRecordPos)
	}

	segmentRecordBuf := make([]byte, footer.Len)
	if _, err := file.ReadAt(segmentRecordBuf, segmentRecordPos); err != nil {
		return metadata.SegmentRecord{}, fmt.Errorf("failed to read segment record: %w", err)
	}

	// Decode and validate segment record with checksum
	segment, err := metadata.DecodeSegmentRecordWithChecksum(segmentRecordBuf, footer.Checksum)
	if err != nil {
		return metadata.SegmentRecord{}, fmt.Errorf("segment record validation failed: %w", err)
	}

	// Validate segment ID matches filename
	if segment.SegmentID != segmentID {
		return metadata.SegmentRecord{}, fmt.Errorf("segment ID mismatch: file=%d, record=%d", segmentID, segment.SegmentID)
	}

	// Validate all blob records reference positions within the segment
	dataEnd := segmentRecordPos
	for _, rec := range segment.Records {
		if rec.Pos < 0 || rec.Pos >= dataEnd {
			return metadata.SegmentRecord{}, fmt.Errorf("blob at invalid position: %d (segment data ends at %d)", rec.Pos, dataEnd)
		}
		if rec.Size <= 0 || rec.Pos+rec.Size > dataEnd {
			return metadata.SegmentRecord{}, fmt.Errorf("blob extends beyond segment: pos=%d size=%d end=%d", rec.Pos, rec.Size, dataEnd)
		}
	}

	return segment, nil
}

// extractSegmentID extracts the segment ID from a filename like "123456.seg"
func extractSegmentID(filename string) int64 {
	var id int64
	// Parse filename: "123456.seg" -> 123456
	fmt.Sscanf(filename, "%d.seg", &id)
	return id
}
