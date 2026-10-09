package blobstore

import (
	"fmt"

	"github.com/miretskiy/dio/v2/iosched"
	"golang.org/x/sys/unix"
)

// Each scheduler owns one ring. Routing is explicit in the common Store.
func newScheduler(opts ...iosched.Option) (iosched.Scheduler, error) {
	return iosched.NewURingScheduler(opts...)
}

func queueCPUs(cfg config) ([]int, error) {
	var allowed unix.CPUSet
	if err := unix.SchedGetaffinity(0, &allowed); err != nil {
		return nil, fmt.Errorf("blobstore: get CPU affinity: %w", err)
	}
	if cfg.rings > allowed.Count() {
		return nil, fmt.Errorf("blobstore: %d rings need distinct CPUs; %d allowed", cfg.rings, allowed.Count())
	}
	if len(cfg.cpus) != 0 {
		for _, cpu := range cfg.cpus {
			if !allowed.IsSet(cpu) {
				return nil, fmt.Errorf("blobstore: CPU %d is not allowed", cpu)
			}
		}
		return cfg.cpus, nil
	}
	cpus := make([]int, 0, cfg.rings)
	for cpu := 0; len(cpus) < cfg.rings; cpu++ {
		if allowed.IsSet(cpu) {
			cpus = append(cpus, cpu)
		}
	}
	return cpus, nil
}
