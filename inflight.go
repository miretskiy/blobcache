package blobcache

import "sync"

// inflightGroup is a sharded coalescing map for thundering herd protection.
//
// When thousands of goroutines experience a cache miss for the same disk region
// simultaneously, inflightGroup ensures exactly ONE goroutine performs the
// disk I/O. The others block on a channel and are woken when the fetch completes.
//
// Unlike singleflight.Group (which uses a single mutex), this uses 64 shards
// for minimal contention under high concurrency.
type inflightGroup struct {
	shards [numFlightShards]inflightShard
}

const numFlightShards = 64

type inflightShard struct {
	mu      sync.Mutex
	flights map[uint64]*flight
}

// flight represents a single in-progress disk fetch.
// The done channel is closed when the fetch completes, unblocking all waiters.
type flight struct {
	done chan struct{}
}

func newInflightGroup() *inflightGroup {
	g := &inflightGroup{}
	for i := range g.shards {
		g.shards[i].flights = make(map[uint64]*flight)
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
// If a flight is already in progress, the caller blocks on the flight's done
// channel until the leader finishes.
//
// Returns true if this goroutine was the leader (ran fn).
func (g *inflightGroup) DoOnce(key uint64, fn func()) bool {
	shard := &g.shards[key%numFlightShards]

	shard.mu.Lock()
	if f, ok := shard.flights[key]; ok {
		// Another goroutine is already fetching. Wait for it.
		shard.mu.Unlock()
		<-f.done
		return false
	}

	// We are the leader. Register our flight.
	f := &flight{done: make(chan struct{})}
	shard.flights[key] = f
	shard.mu.Unlock()

	// Perform the disk read + cache population.
	fn()

	// Wake all waiters and clean up.
	close(f.done)

	shard.mu.Lock()
	delete(shard.flights, key)
	shard.mu.Unlock()

	return true
}
