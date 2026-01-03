package blobcache

import (
	"os"
)

type segmentFile struct {
	file  *os.File
	segID int64
}

func (s *segmentFile) ReadAt(p []byte, off int64) (int, error) {
	return s.file.ReadAt(p, off)
}

func (s *segmentFile) PunchHole(offset, length int64) error {
	// Calls the OS-specific implementation (fallocate on Linux)
	return PunchHole(s.file, offset, length)
}

func (s *segmentFile) Close() error {
	return s.file.Close()
}

func (s *segmentFile) SegmentID() int64 {
	return s.segID
}
