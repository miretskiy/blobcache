package blobcache

import "sync"

// inflightGroup is a sharded coalescing map for thundering herd protection.
//
// When thousands of goroutines experience a cache miss for the same disk region
// simultaneously, inflightGroup ensures exactly ONE goroutine performs the
// disk I/O. The others block on a per-shard sync.Cond and are woken when the
// fetch completes.
//
// Unlike singleflight.Group (which uses a single mutex), this uses 64 shards
// for minimal contention under high concurrency.
//
// Unlike the channel-based approach, this uses a single sync.Cond per shard
// (allocated once at init time) instead of allocating a channel per flight.
// With 64 shards and ~50-100 active flights, each shard has ~1-2 concurrent
// flights, so Broadcast wakes at most a few goroutines per shard.
type inflightGroup struct {
	shards [numFlightShards]inflightShard
}

const numFlightShards = 64

type inflightShard struct {
	mu      sync.Mutex
	cond    *sync.Cond
	flights map[uint64]struct{} // zero-size values
}

func newInflightGroup() *inflightGroup {
	g := &inflightGroup{}
	for i := range g.shards {
		g.shards[i].flights = make(map[uint64]struct{})
		g.shards[i].cond = sync.NewCond(&g.shards[i].mu)
	}
	return g
}

// flightKey packs (segmentID, alignedOffset) into a uint64.
// The upper 32 bits are the segmentID. The lower 32 bits are the
// chunk index (alignedOffset / prefetchChunkSize), supporting up to
// 256TB per segment at 64KB chunk granularity.
func flightKey(segID uint32, alignedOff int64) uint64 {
	chunkIdx := uint32(alignedOff / prefetchChunkSize)
	return uint64(segID)<<32 | uint64(chunkIdx)
}

// DoOnce ensures exactly one fetch per (segID, alignedOffset) key.
//
// If no flight is in progress for this key, the caller becomes the "leader":
// it runs fn(), and when fn returns, all blocked waiters are unblocked.
//
// If a flight is already in progress, the caller blocks on the shard's Cond
// until the leader finishes. Spurious wakes are handled by re-checking the
// flight map in a loop.
//
// Returns true if this goroutine was the leader (ran fn).
func (g *inflightGroup) DoOnce(key uint64, fn func()) bool {
	shard := &g.shards[key%numFlightShards]

	shard.mu.Lock()
	if _, inflight := shard.flights[key]; inflight {
		// Another goroutine is already fetching. Wait for it.
		for {
			shard.cond.Wait()
			if _, still := shard.flights[key]; !still {
				break
			}
		}
		shard.mu.Unlock()
		return false
	}

	// We are the leader. Register our flight.
	shard.flights[key] = struct{}{}
	shard.mu.Unlock()

	// Perform the disk read + cache population.
	fn()

	// Wake all waiters and clean up.
	shard.mu.Lock()
	delete(shard.flights, key)
	shard.cond.Broadcast()
	shard.mu.Unlock()

	return true
}
