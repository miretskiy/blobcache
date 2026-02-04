package blobcache

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
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

// CurrentSegmentID returns the most recently allocated segment ID.
// Used by compaction to determine the "cooling period" boundary.
func (p *segmentIDProvider) CurrentSegmentID() uint32 {
	return p.counter.Load()
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

// DeleteSegmentFiles removes segment and footer files for the given segment ID.
// Returns nil if files don't exist.
func DeleteSegmentFiles(basePath string, shards int, segmentID uint32) error {
	segPath := getSegmentPath(basePath, shards, segmentID)
	var errs []error

	if err := os.Remove(segPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("delete segment %d file: %w", segmentID, err))
	}
	if err := os.Remove(SegmentMetaPath(segPath)); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("delete segment %d footer: %w", segmentID, err))
	}

	return errors.Join(errs...)
}

// ErrEntryTooLarge is returned when a footer entry exceeds uint32 limits.
var ErrEntryTooLarge = errors.New("segment: entry size exceeds uint32 limit")

// footerEntriesToIndexItems converts footer entries to index items.
// Returns error if any entry has sizes that exceed uint32 limits.
func footerEntriesToIndexItems(segmentID uint32, entries []record.FooterEntry) ([]index.Item, error) {
	items := make([]index.Item, 0, len(entries))
	for i := range entries {
		entry := &entries[i]

		// Validate sizes fit in uint32
		physicalLen := int64(record.HeaderSize) + int64(entry.KeyLen) + entry.PhysicalSize
		if physicalLen > math.MaxUint32 || entry.Pos > math.MaxUint32 {
			return nil, fmt.Errorf("%w: entry %d has physicalLen=%d, pos=%d",
				ErrEntryTooLarge, i, physicalLen, entry.Pos)
		}

		item := index.Item{
			Key:         entry.Key,
			SegmentID:   segmentID,
			Offset:      uint32(entry.Pos),
			PhysicalLen: uint32(physicalLen),
		}
		item.SetCompression(entry.Compression())
		items = append(items, item)
	}
	return items, nil
}

// segmentWriter manages I/O for a single segment file.
// It encapsulates file creation, footer writing, and index updates.
type segmentWriter struct {
	segmentID  uint32
	basePath   string
	shards     int
	ioFlags    sys.OpenFlag
	footerPool poolProvider
	file       *os.File
}

// CreateSegmentWriter creates a segment file and returns a writer for it.
// The file is created with DirectIO and fallocated to the given size.
func CreateSegmentWriter(
	segmentID uint32,
	basePath string,
	shards int,
	ioFlags sys.OpenFlag,
	footerPool poolProvider,
	size int64,
) (*segmentWriter, error) {
	path := getSegmentPath(basePath, shards, segmentID)
	f, err := sys.CreateAndAllocateFile(path, ioFlags, size)
	if err != nil {
		return nil, err
	}
	return &segmentWriter{
		segmentID:  segmentID,
		basePath:   basePath,
		shards:     shards,
		ioFlags:    ioFlags,
		footerPool: footerPool,
		file:       f,
	}, nil
}

// Path returns the path for this segment's data file.
func (w *segmentWriter) Path() string {
	return getSegmentPath(w.basePath, w.shards, w.segmentID)
}

// FooterPath returns the path for this segment's metadata file (.meta).
func (w *segmentWriter) FooterPath() string {
	return SegmentMetaPath(w.Path())
}

// File returns the underlying file handle for writing.
func (w *segmentWriter) File() *os.File {
	return w.file
}

// WriteHeader writes the segment file header.
func (w *segmentWriter) WriteHeader() error {
	_, err := sys.WriteAligned(record.FileHeaderBytes[:], w.file, w.ioFlags)
	return err
}

// WriteFooter writes this segment's footer (.meta) file for crash recovery.
func (w *segmentWriter) WriteFooter(entries []record.FooterEntry) error {
	return WriteFooter(w.segmentID, entries, w.Path(), w.footerPool, w.ioFlags)
}

// Close syncs and closes the segment file.
func (w *segmentWriter) Close() error {
	if w.file == nil {
		return nil
	}
	err := errors.Join(sys.SyncFile(w.file, w.ioFlags), w.file.Close())
	w.file = nil
	return err
}

// Finalize writes the footer file and updates the index for a completed segment.
func (w *segmentWriter) Finalize(
	entries []record.FooterEntry,
	maxSeqID uint64,
	indexWriter Batcher,
) error {
	// Build index items from footer entries
	items, err := footerEntriesToIndexItems(w.segmentID, entries)
	if err != nil {
		return err
	}

	// Update index (RAM + Bitcask)
	if err := indexWriter.PutBatch(w.segmentID, items, maxSeqID); err != nil {
		return fmt.Errorf("index update: %w", err)
	}

	// Write footer file for crash recovery
	if err := w.WriteFooter(entries); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}

	return nil
}
