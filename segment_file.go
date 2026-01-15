package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/miretskiy/blobcache/internal/sys"
)

// maxSegmentID scans the segments directory and returns the highest segment ID found.
// Returns 0 if no segments exist. Used to initialize the segment counter on startup.
func maxSegmentID(basePath string, shards int) uint32 {
	segmentsDir := filepath.Join(basePath, "segments")
	numShards := max(1, shards)
	var maxID uint32

	for shard := 0; shard < numShards; shard++ {
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

type segmentFile struct {
	file  *os.File
	segID uint32
}

func (s *segmentFile) ReadAt(p []byte, off int64) (int, error) {
	return s.file.ReadAt(p, off)
}

func (s *segmentFile) PunchHole(offset, length int64) (int64, error) {
	// Calls the OS-specific implementation (fallocate on Linux, F_PUNCHHOLE on Darwin)
	return sys.PunchHole(s.file, offset, length)
}

func (s *segmentFile) Close() error {
	return s.file.Close()
}

func (s *segmentFile) SegmentID() uint32 {
	return s.segID
}
