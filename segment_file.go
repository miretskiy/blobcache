package blobcache

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/metadata"
)

// SegmentFile represents a segment file with hole punching capability
type SegmentFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	PunchHole(offset, length int64) error
}

// segmentFile implements SegmentFile for both active and sealed segments
type segmentFile struct {
	file   *os.File
	mu     sync.Mutex
	sealed atomic.Bool // True after footer written

	// For active segments: track meta (deletion tracked via Deleted flag)
	meta metadata.SegmentRecord

	// Segment statistics
	totalBytes   atomic.Int64 // Total bytes (sum of all blob sizes, grows during writes)
	deletedBytes atomic.Int64 // Bytes deleted via hole punching

	// For sealed segments: track footer position for rewrites
	footerPos int64
}

// newSegmentFile creates a SegmentFile with explicit parameters
// footerPos: offset where footer is/will be written (0 for new/partial, >0 for finalized)
// meta: segment record (from footer or bitcask)
func newSegmentFile(f *os.File, footerPos int64, meta metadata.SegmentRecord) *segmentFile {
	sf := &segmentFile{
		file:      f,
		footerPos: footerPos,
		meta:      meta,
	}

	// Initialize stats from meta
	var totalBytes, deletedBytes int64
	for _, rec := range meta.Records {
		totalBytes += rec.Size
		if rec.IsDeleted() {
			deletedBytes += rec.Size
		}
	}
	sf.totalBytes.Store(totalBytes)
	sf.deletedBytes.Store(deletedBytes)

	sf.sealed.Store(footerPos > 0)

	return sf
}

// ReadAt implements io.ReaderAt
func (s *segmentFile) ReadAt(p []byte, off int64) (int, error) {
	return s.file.ReadAt(p, off)
}

// WriteAt implements io.WriterAt
func (s *segmentFile) WriteAt(p []byte, off int64) (int, error) {
	if s.sealed.Load() {
		return 0, fmt.Errorf("cannot write to sealed segment")
	}
	return s.file.WriteAt(p, off)
}

// Close implements io.Closer
func (s *segmentFile) Close() error {
	return s.file.Close()
}

// PunchHole removes a blob's data and updates metadata
func (s *segmentFile) PunchHole(offset, length int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find and mark blob as deleted
	found := false
	for i := range s.meta.Records {
		if s.meta.Records[i].Pos == offset && s.meta.Records[i].Size == length {
			s.meta.Records[i].SetDeleted()
			s.deletedBytes.Add(length)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("blob not found at offset=%d size=%d", offset, length)
	}

	// Write footer if sealed (active segments write footer on close)
	if s.sealed.Load() {
		footerBytes := metadata.AppendSegmentRecordWithFooter(nil, s.meta)
		if _, err := s.file.WriteAt(footerBytes, s.footerPos); err != nil {
			return fmt.Errorf("failed to write footer: %w", err)
		}
	}

	// Punch the hole in the file
	return PunchHole(s.file, offset, length)
}

// addRecord tracks a blob record (for active segments)
func (s *segmentFile) addRecord(rec metadata.BlobRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta.Records = append(s.meta.Records, rec)
	s.totalBytes.Add(rec.Size)
}

// seal marks the segment as finalized and meta footer position
func (s *segmentFile) seal(footerPos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.footerPos = footerPos
	s.sealed.Store(true)
}

// getLiveRecords returns all meta (including deleted ones with Deleted flag set)
// Deleted blobs are included in footer with flag - allows recovery and stats rebuild
func (s *segmentFile) getLiveRecords() []metadata.BlobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.Records
}

// LiveBytes returns bytes of live (non-deleted) blobs
func (s *segmentFile) LiveBytes() int64 {
	return s.totalBytes.Load() - s.deletedBytes.Load()
}

// DeletedBytes returns bytes of deleted blobs
func (s *segmentFile) DeletedBytes() int64 {
	return s.deletedBytes.Load()
}

// TotalBytes returns total bytes (live + deleted)
func (s *segmentFile) TotalBytes() int64 {
	return s.totalBytes.Load()
}

// FullnessPct returns percentage of live data (0.0 to 1.0)
func (s *segmentFile) FullnessPct() float64 {
	total := s.TotalBytes()
	if total == 0 {
		return 1.0 // Empty segment is "fully live" (don't compact)
	}
	return float64(s.LiveBytes()) / float64(total)
}
