//go:build !linux

package blobstore

import "github.com/miretskiy/dio/v2/iosched"

// newScheduler returns the POSIX scheduler, the only one off Linux, for
// development and tests; it emulates virtual descriptor slots with ordinary
// files. It runs each operation inside Submit, so Write waits for the disk,
// and completions run inside Submit too; correctness does not depend on
// either.
func newScheduler(config, int) (iosched.Scheduler, error) {
	return iosched.NewPOSIXScheduler(), nil
}
