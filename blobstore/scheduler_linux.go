package blobstore

import "github.com/miretskiy/dio/v2/iosched"

// newScheduler returns the io_uring scheduler with vfiles virtual descriptor
// slots. There is no fallback: synchronous I/O would put disk waits inside
// Write, so without io_uring Open fails.
func newScheduler(cfg config, vfiles int) (iosched.Scheduler, error) {
	return iosched.NewURingScheduler(iosched.WithRingDepth(cfg.ringDepth), iosched.WithVFiles(uint32(vfiles)))
}
