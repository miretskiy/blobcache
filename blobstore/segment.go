package blobstore

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
)

// IDs are monotonic counters, independent of wall-clock adjustments. Files
// are <low byte, 2 hex digits>/<ID, 16 hex digits>.seg.
const (
	shardCount = 256
	extSegment = ".seg"
)

func shardName(shard int) string   { return fmt.Sprintf("%02x", shard) }
func segmentName(id uint64) string { return fmt.Sprintf("%016x%s", id, extSegment) }
func segmentPath(root string, id uint64) string {
	return filepath.Join(root, shardName(int(id%shardCount)), segmentName(id))
}

// segment contains lifecycle metadata, not a persistent in-memory record
// index. entries belongs to the active writer and is released at sealing.
// All fields except done are guarded by activeSegment's lock.
type segment struct {
	id        uint64
	slot      uint32
	pos, size int64
	entries   []footerEntry
	done      atomic.Bool // all seal operations have completed, successfully or not
}
