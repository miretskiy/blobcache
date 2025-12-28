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

	// For active segments: track records (deletion tracked via Deleted flag)
	records []metadata.BlobRecord

	// Segment statistics
	totalBytes   atomic.Int64 // Total bytes (sum of all blob sizes, grows during writes)
	deletedBytes atomic.Int64 // Bytes deleted via hole punching

	// For sealed segments: track footer position for rewrites
	footerPos int64
	segmentID int64
}

// newSegmentFile creates a SegmentFile from an os.File
func newSegmentFile(f *os.File, segmentID int64) *segmentFile {
	return &segmentFile{
		file:      f,
		segmentID: segmentID,
	}
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

	if s.sealed.Load() {
		return s.punchHoleSealed(offset, length)
	}
	return s.punchHoleActive(offset, length)
}

// punchHoleActive marks a blob as deleted in active segment
// Sets Deleted flag which persists to footer
func (s *segmentFile) punchHoleActive(offset, length int64) error {
	// Find and mark record as deleted
	for i := range s.records {
		if s.records[i].Pos == offset && s.records[i].Size == length {
			s.records[i].SetDeleted()
			s.deletedBytes.Add(length)
			break
		}
	}

	// Punch the hole in the file
	return PunchHole(s.file, offset, length)
}

// punchHoleSealed marks blob as deleted and rewrites footer
func (s *segmentFile) punchHoleSealed(offset, length int64) error {
	// Read current footer
	footerBuf := make([]byte, metadata.SegmentFooterSize)
	if _, err := s.file.ReadAt(footerBuf, s.footerPos); err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}

	footer, err := metadata.DecodeSegmentFooter(footerBuf)
	if err != nil {
		return fmt.Errorf("invalid footer: %w", err)
	}

	// Read segment record
	segmentRecordBuf := make([]byte, footer.Len)
	segmentRecordPos := s.footerPos - footer.Len
	if _, err := s.file.ReadAt(segmentRecordBuf, segmentRecordPos); err != nil {
		return fmt.Errorf("failed to read segment record: %w", err)
	}

	segment, err := metadata.DecodeSegmentRecordWithChecksum(segmentRecordBuf, footer.Checksum)
	if err != nil {
		return fmt.Errorf("segment record validation failed: %w", err)
	}

	// Mark the blob as deleted
	found := false
	for i := range segment.Records {
		if segment.Records[i].Pos == offset && segment.Records[i].Size == length {
			segment.Records[i].SetDeleted()
			s.deletedBytes.Add(length)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("blob not found in footer at offset=%d size=%d", offset, length)
	}

	// Rewrite footer with updated record (includes Deleted flag)
	newFooterBytes := metadata.AppendSegmentRecordWithFooter(nil, segment)
	if _, err := s.file.WriteAt(newFooterBytes, segmentRecordPos); err != nil {
		return fmt.Errorf("failed to rewrite footer: %w", err)
	}

	// Punch the hole in the file
	return PunchHole(s.file, offset, length)
}

// addRecord tracks a blob record (for active segments)
func (s *segmentFile) addRecord(rec metadata.BlobRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	s.totalBytes.Add(rec.Size)
}

// seal marks the segment as finalized and records footer position
func (s *segmentFile) seal(footerPos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.footerPos = footerPos
	s.sealed.Store(true)
}

// getLiveRecords returns all records (including deleted ones with Deleted flag set)
// Deleted blobs are included in footer with flag - allows recovery and stats rebuild
func (s *segmentFile) getLiveRecords() []metadata.BlobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records
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
