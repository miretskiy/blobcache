package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// segmentIDProvider allocates unique segment IDs.
// Used by both MemTable (normal writes) and Compaction to avoid conflicts.
type segmentIDProvider struct {
	counter atomic.Uint32
}

// newSegmentIDProvider creates a provider initialized from the highest existing segment.
func newSegmentIDProvider(basePath string, shards int) *segmentIDProvider {
	p := &segmentIDProvider{}
	p.counter.Store(scanMaxSegmentID(basePath, shards))
	return p
}

// NextSegmentID atomically allocates the next segment ID.
func (p *segmentIDProvider) NextSegmentID() uint32 {
	return p.counter.Add(1)
}

// scanMaxSegmentID scans the segments directory and returns the highest segment ID found.
// Returns 0 if no segments exist.
func scanMaxSegmentID(basePath string, shards int) uint32 {
	segmentsDir := filepath.Join(basePath, "segments")
	numShards := max(1, shards)
	var maxID uint32

	for shard := range numShards {
		shardDir := filepath.Join(segmentsDir, fmt.Sprintf("%04d", shard))
		entries, err := os.ReadDir(shardDir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
				continue
			}
			var id uint32
			if _, err := fmt.Sscanf(entry.Name(), "%d.seg", &id); err == nil {
				if id > maxID {
					maxID = id
				}
			}
		}
	}
	return maxID
}
