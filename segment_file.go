package blobcache

import (
	"os"

	"github.com/miretskiy/blobcache/internal/sys"
)

type segmentFile struct {
	file  *os.File
	segID int64
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

func (s *segmentFile) SegmentID() int64 {
	return s.segID
}
