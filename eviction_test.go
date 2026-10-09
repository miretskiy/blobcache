package blobcache

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestEvictionRemovesOnlyRetiredLocations(t *testing.T) {
	c := openCache(t, t.TempDir(), WithMaxSize(3*testSegment))
	defer closeCache(t, c)
	for i := 0; i < 24; i++ {
		put(t, c, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	require.Eventually(t, func() bool {
		_, present := c.index.get(blobstore.HashKey([]byte("0")))
		return !present && c.Stats().Items < 12
	}, 5*time.Second, time.Millisecond)
	requireMissing(t, c, "0")
	requireValue(t, c, "23", randomBytes(23, 100<<10))
	require.LessOrEqual(t, c.Stats().DiskBytes, int64(3*testSegment))
	// The index must not grow with the complete write history.
	require.Less(t, c.Stats().Items, 12)
}

func TestEvictionIndexBatchAndLateCompletion(t *testing.T) {
	// Use real locations in two segments, with enough keys to span shards and
	// to exercise conditional deletion of an overwritten key.
	store, err := blobstore.Open(t.TempDir(), blobstore.WithSegmentSize(64<<10), blobstore.WithDirectWrites(false), blobstore.WithDirectReads(false))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	x := newIndex(0)
	var records []struct {
		hash Key
		loc  blobstore.Location
	}
	var next blobstore.Location
	for i := 0; ; i++ {
		h := blobstore.HashKey([]byte(fmt.Sprint(i)))
		ticket, err := store.Write([]byte(fmt.Sprint(i)), make([]byte, 100), 100)
		require.NoError(t, err)
		loc, err := ticket.Wait()
		require.NoError(t, err)
		if loc.Segment() != 0 {
			next = loc
			break
		}
		x.loaded(h, loc)
		records = append(records, struct {
			hash Key
			loc  blobstore.Location
		}{h, loc})
	}
	overwritten := records[0].hash
	x.loaded(overwritten, next)
	pending := blobstore.HashKey([]byte("pending"))
	mem := memoryValue{block: &memoryBlock{keys: []Key{pending}}}
	x.put(pending, mem, next)
	x.evicted(blobstore.Eviction{0})
	require.Eventually(t, func() bool { return x.len() == 2 }, 5*time.Second, time.Millisecond)
	it, ok := x.get(overwritten)
	require.True(t, ok)
	require.Equal(t, next, it.loc)
	for _, record := range records {
		if record.hash == overwritten {
			continue
		}
		_, ok := x.get(record.hash)
		require.False(t, ok)
	}
	x.put(pending, mem, records[0].loc) // delayed publication of an evicted record
	it, ok = x.get(pending)
	require.True(t, ok)
	require.Equal(t, next, it.loc)
	x.evictMemory([]*memoryBlock{mem.block})
	it, ok = x.get(pending)
	require.True(t, ok)
	require.Nil(t, it.mem.block)
	x.evicted(blobstore.Eviction{next.Segment()})
	require.Eventually(t, func() bool { return x.len() == 0 }, 5*time.Second, time.Millisecond)
	// An overlapping retry must never move a shard's boundary backwards.
	x.evicted(blobstore.Eviction{0})
	x.put(pending, mem, next)
	_, ok = x.get(pending)
	require.False(t, ok)
	x.loaded(overwritten, next)
	_, ok = x.get(overwritten)
	require.False(t, ok)
}

func TestFailedWriteStaysInMemoryUntilReclaimed(t *testing.T) {
	failed := openCache(t, t.TempDir(), WithMemory(256<<10), withPreSubmit(func(op iosched.Op) iosched.Op {
		return iosched.VWriteOp(15, make([]byte, 4096), 0)
	}))
	value := randomBytes(1, 100<<10)
	ticket, err := failed.Put([]byte("failed"), alloc(t, failed, "failed", value), len(value))
	require.NoError(t, err)
	require.Error(t, ticket.Wait())
	failed.Drain()
	requireValue(t, failed, "failed", value)
	for i := 0; i < 4; i++ {
		b, err := failed.Alloc(100 << 10)
		require.NoError(t, err)
		require.NoError(t, failed.Free(b))
	}
	requireMissing(t, failed, "failed")
	require.Zero(t, failed.Stats().Items)
	closeCache(t, failed)
}

func TestMemoryCallbackPanicReleasesPin(t *testing.T) {
	c := openCache(t, t.TempDir(), WithMemory(256<<10))
	defer closeCache(t, c)
	put(t, c, "k", randomBytes(0, 100<<10))
	require.Panics(t, func() { _ = c.Get([]byte("k"), func([]byte) error { panic("caller") }) })
	for i := 0; i < 4; i++ {
		buf, err := c.Alloc(100 << 10)
		require.NoError(t, err)
		require.NoError(t, c.Free(buf))
	}
}

func TestDefaultValueChecksum(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	value := randomBytes(1, 10000)
	put(t, c, "default-checksum-key", value)
	closeCache(t, c)
	path, start, _ := findRecord(t, dir, "default-checksum-key", len(value))
	flipByte(t, path, start+3)
	c = openCache(t, dir)
	defer closeCache(t, c)
	err := c.Get([]byte("default-checksum-key"), func([]byte) error { t.Fatal("corrupt value delivered"); return nil })
	require.True(t, errors.Is(err, blobstore.ErrCorrupt))
}
