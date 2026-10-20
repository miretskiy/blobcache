package blobcache

type reclaimRequest struct {
	size int64
	done chan struct{} // nil for a best-effort request
}

// startCacheReclaim coalesces best-effort signals in a single queued request.
// Required requests always reach the worker and wait for their pass to finish.
// As with allocations, callers must finish these calls before Cache.Close.
func (c *Cache) startCacheReclaim(size int64, bestEffort bool) {
	request := reclaimRequest{size: size}
	if bestEffort {
		select {
		case c.reclaimRequests <- request:
		default:
		}
		return
	}
	request.done = make(chan struct{})
	c.reclaimRequests <- request
	<-request.done
}

func (c *Cache) reclaimLoop() {
	for request := range c.reclaimRequests {
		capacity := c.memory.Capacity()
		// Leave at least 25% free to separate the 80% trigger from the next
		// reclamation. For large requests, keep another request's worth free.
		target := min(capacity, max(capacity/4, request.size))
		if request.done != nil {
			// The required allocation has not happened yet. Include its space
			// as well as the headroom, capped without overflowing.
			target += min(request.size, capacity-target)
		}
		if needed := target - c.memory.Available(); needed > 0 {
			c.values.reclaimCacheMemory(needed)
		}
		if request.done != nil {
			close(request.done)
		}
	}
}
