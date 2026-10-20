package blobcache

// The worker retains only the payload reserve needed while reclamation runs.
// The initial reserve is 5% of the target; completed passes replace that estimate
// with observed publications, without waiting for external unlinks.
type diskReclaimPolicy struct {
	headroom uint64
}

func (p *diskReclaimPolicy) observe(incoming, target uint64) {
	// Saturate before adding the 25% margin. Headroom may exceed a single
	// batch, but cannot exceed the cache target and underflow the trigger.
	incoming = min(incoming, target)
	p.headroom = max(min(1<<20, target/20), incoming+min(incoming/4, target-incoming))
}

func (p diskReclaimPolicy) reclaimBytes(live, target uint64) uint64 {
	trigger := target - p.headroom
	if live == 0 || live < trigger {
		return 0
	}
	// At the trigger, request another headroom-sized batch plus any excess.
	// Bound each request at 5%; the worker keeps making passes if needed.
	// Subtracting from a retention goal avoids overflowing on extreme usage.
	retain := trigger - min(p.headroom, trigger)
	return min(target/20, live-retain)
}

// Put signals pressure using the disk index's live-byte counter. One pending
// signal is sufficient: the worker reads current usage when it processes it.
func (c *Cache) startDiskReclaim() {
	trigger := c.diskReclaimTrigger.Load()
	if c.store.Size() < trigger {
		return
	}
	select {
	case c.diskReclaimRequests <- struct{}{}:
	default:
	}
}

func (c *Cache) diskReclaimLoop() {
	target := c.store.TargetSize()
	policy := diskReclaimPolicy{headroom: target / 20}
	for range c.diskReclaimRequests {
		for {
			live, before := c.store.WriteStats()
			bytes := policy.reclaimBytes(live, target)
			if bytes == 0 {
				break
			}
			if c.store.Reclaim(bytes) == 0 {
				break // Unfinished writers can defer this best-effort attempt.
			}
			_, after := c.store.WriteStats()
			policy.observe(after-before, target)
			c.diskReclaimTrigger.Store(target - policy.headroom)
			// Resample after publishing the new trigger. A concurrent Put may
			// have used the previous trigger and left no signal. Continue here
			// until usage is below the new trigger, even if all writers stop.
		}
	}
}
