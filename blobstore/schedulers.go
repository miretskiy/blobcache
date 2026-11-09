package blobstore

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
)

func (c *config) schedulerWriteBudget(i int) time.Duration {
	if c.writeSchedulers > 0 && i >= c.writeSchedulers {
		return 0
	}
	return c.ioBudget
}

func (st *Store) openSchedulers() error {
	st.disk = iosched.NewDiskPolicy(iosched.WithLatencyGoal(st.cfg.ioBudget), iosched.WithoutReadIOBudget(),
		iosched.WithWriteBacklogLimit(st.cfg.writeBacklog), iosched.WithWriteBacklogRecovery(st.cfg.writeBacklogRecovery),
		iosched.WithReadBacklogLimit(st.cfg.readBacklog))
	for i, cpu := range st.cfg.schedulerCPUs {
		opts := []iosched.Option{iosched.WithDiskPolicy(st.disk),
			iosched.WithRingDepth(st.cfg.ringDepth), iosched.WithVFiles(1024)}
		// Preserve the default's POSIX fallback. Explicit multi-ring or pinned
		// configurations must not silently run on a synchronous backend.
		if len(st.cfg.schedulerCPUs) == 1 && cpu == -1 {
			st.schedulers = append(st.schedulers, iosched.NewDefaultScheduler(opts...))
			continue
		}
		scheduler, err := iosched.NewURingScheduler(append(opts, iosched.WithCoordinatorCPU(cpu))...)
		if err != nil {
			// Open's deferred Close drains any schedulers already created.
			return fmt.Errorf("blobstore: scheduler %d (CPU %d): %w", i, cpu, err)
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

func (st *Store) writeScheduler() iosched.Scheduler {
	n := st.cfg.writeSchedulers
	if n == 0 {
		n = len(st.schedulers)
	}
	return pickScheduler(st.schedulers[:n], &st.nextWrite)
}

// SchedulerStats describes the configured role and backend of one ring.
// CPU is the requested affinity (-1 means unpinned), not an observed CPU.
// Role is "shared", "read", or "write". Backend is "uring" or "posix".
type SchedulerStats struct {
	CPU           int
	Role          string
	Backend       string
	WriteIOBudget time.Duration // shared disk allowance; zero for read-only rings or disabled budgeting
}

// IOSchedulerStats lists schedulers in WithSchedulerCPUs order.
func (st *Store) IOSchedulerStats() []SchedulerStats {
	stats := make([]SchedulerStats, len(st.schedulers))
	for i, scheduler := range st.schedulers {
		s := &stats[i]
		s.CPU, s.Role, s.Backend = st.cfg.schedulerCPUs[i], "shared", "posix"
		s.WriteIOBudget = st.cfg.schedulerWriteBudget(i)
		if st.cfg.writeSchedulers > 0 {
			s.Role = "read"
			if i < st.cfg.writeSchedulers {
				s.Role = "write"
			}
		}
		if _, ok := scheduler.(*iosched.URingScheduler); ok {
			s.Backend = "uring"
		}
	}
	return stats
}

// IODiskStats reports outstanding admission charges across all rings.
// Disabled limits and the POSIX backend report zeros.
func (st *Store) IODiskStats() iosched.DiskStats { return st.disk.Stats() }
