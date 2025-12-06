# TODO Items

## Memtable Backpressure

**Issue:** Current backpressure is incomplete. While `doRotateUnderLock` waits when inflight >= MaxInflightBatches, this only prevents rotation. Put() calls can still add data to the active memfile even when the system is overloaded.

**Current behavior:**
- Thread A: Put() adds to active, triggers rotation
- doRotateUnderLock waits if inflight >= 6
- Thread B: Put() continues adding to NEW active (no backpressure!)
- Result: New active can grow unbounded while waiting

**Desired behavior:**
- When inflight is at capacity, Put() should block or apply backpressure
- Prevents unlimited memory growth in active memfile
- True flow control from flush workers back to writers

**Possible solutions:**
1. Add semaphore/counter tracking total memory in flight
2. Block Put() when active.size exceeds threshold AND inflight is full
3. Use separate backpressure channel/mechanism
4. Return error from Put() when system is overloaded (currently returns nothing)

**Priority:** Medium - only matters under sustained high write load
