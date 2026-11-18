package blobstore

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

// maxRegisteredEntries is the kernel's limit on one ring's registered buffers.
const maxRegisteredEntries = 1 << 14

// openSchedulers creates the store's io_uring schedulers, which share one
// device model and admission policy.
func (st *Store) openSchedulers() error {
	st.disk = st.cfg.disk
	if st.disk == nil {
		st.disk = iosched.NewDiskPolicy(iosched.WithLatencyGoal(st.cfg.ioBudget), iosched.WithoutReadIOBudget(),
			iosched.WithWriteBacklogLimit(st.cfg.writeBacklog), iosched.WithWriteBacklogRecovery(st.cfg.writeBacklogRecovery),
			iosched.WithReadBacklogLimit(st.cfg.readBacklog))
	}
	opts := []iosched.Option{iosched.WithDiskPolicy(st.disk), iosched.WithRingDepth(st.cfg.ringDepth),
		iosched.WithNVMeCommands(st.device)}
	if st.cfg.registerMemory {
		region := st.memory.Region()
		if len(region) > maxRegisteredEntries*alloc.MaxAllocationSize {
			return fmt.Errorf("blobstore: %d bytes of memory exceed the %d registrable per ring",
				len(region), maxRegisteredEntries*alloc.MaxAllocationSize)
		}
		entries := make([][]byte, 0, len(region)/alloc.MaxAllocationSize)
		for start := 0; start < len(region); start += alloc.MaxAllocationSize {
			entries = append(entries, region[start:min(len(region), start+alloc.MaxAllocationSize)])
		}
		opts = append(opts, iosched.WithDMABuffers(entries...))
	}
	for i := range st.cfg.writeSchedulers + st.cfg.readSchedulers {
		scheduler, err := iosched.NewURingScheduler(opts...)
		if err != nil {
			// Open's deferred Close drains any schedulers already created.
			return fmt.Errorf("blobstore: scheduler %d: %w", i, err)
		}
		st.schedulers = append(st.schedulers, scheduler)
	}
	return nil
}

func pickScheduler(pool []iosched.Scheduler, next *atomic.Uint64) iosched.Scheduler {
	if len(pool) == 1 {
		return pool[0]
	}
	return pool[(next.Add(1)-1)%uint64(len(pool))]
}

func (st *Store) readScheduler() iosched.Scheduler {
	return pickScheduler(st.schedulers[st.cfg.writeSchedulers:], &st.nextRead)
}

// writeScheduler picks a write ring; they come first in st.schedulers.
func (st *Store) writeScheduler() iosched.Scheduler {
	return pickScheduler(st.schedulers[:st.cfg.writeSchedulers], &st.nextWrite)
}

// SchedulerStats describes the role of one ring: "read" or "write".
type SchedulerStats struct {
	Role          string
	WriteIOBudget time.Duration // shared disk allowance; zero for read rings or disabled budgeting
}

// IOSchedulerStats lists schedulers in creation order, write rings first.
func (st *Store) IOSchedulerStats() []SchedulerStats {
	stats := make([]SchedulerStats, len(st.schedulers))
	for i := range stats {
		stats[i] = SchedulerStats{Role: "read"}
		if i < st.cfg.writeSchedulers {
			stats[i] = SchedulerStats{Role: "write", WriteIOBudget: st.disk.LatencyGoal(iosched.OpWrite)}
		}
	}
	return stats
}

// IODiskStats reports outstanding admission charges across all rings.
// Disabled limits report zeros.
func (st *Store) IODiskStats() iosched.DiskStats { return st.disk.Stats() }
