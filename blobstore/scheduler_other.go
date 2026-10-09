//go:build !linux

package blobstore

import "github.com/miretskiy/dio/v2/iosched"

// Development fallback: each queue gets its own emulated virtual-file table.
func newScheduler(...iosched.Option) (iosched.Scheduler, error) {
	return iosched.NewPOSIXScheduler(), nil
}

func queueCPUs(cfg config) ([]int, error) {
	cpus := make([]int, cfg.rings)
	for i := range cpus {
		cpus[i] = -1
	}
	return cpus, nil
}
