package blobcache

import (
	"bytes"
	"errors"
	"github.com/miretskiy/dio/v2/iosched"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func TestMemoryBlocks(t *testing.T) {
	m, err := newMemory(4 << 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, m.close()) }()
	require.Zero(t, m.used(), "no eager mmap")
	// Fill all four blocks, leaving the oldest pinned.
	var values []memoryValue
	for i := 0; i < 4; i++ {
		v, buf, err := m.alloc(m.blockSize, false, nil)
		require.NoError(t, err)
		require.True(t, align.IsAligned(buf))
		values = append(values, v)
		if i != 0 {
			v.unpin()
		}
	}
	old := values[1]
	fresh, _, err := m.alloc(m.blockSize, false, nil)
	require.NoError(t, err, "skip the pinned oldest block")
	require.False(t, old.pin(), "retired descriptors cannot pin reused storage")
	require.True(t, values[0].pin())
	values[0].unpin()
	values[0].unpin()
	fresh.unpin()
	require.LessOrEqual(t, m.used(), m.limit)
}

func TestMemorySmallRecordsFillBlocks(t *testing.T) {
	m, err := newMemory(1 << 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, m.close()) }()
	first, _, err := m.alloc(4096, false, nil)
	require.NoError(t, err)
	first.unpin()
	for i := 1; i < 256; i++ {
		v, _, err := m.alloc(4096, false, nil)
		require.NoError(t, err)
		v.unpin()
	}
	require.True(t, first.pin(), "metadata must not force early eviction of small records")
	first.unpin()
	require.Equal(t, int64(1<<20), m.used())
}

func TestMemoryOversizedBudget(t *testing.T) {
	m, err := newMemory(4 << 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, m.close()) }()
	// Map both chunks, then reclaim their slots and mappings for a large record.
	for i := 0; i < 4; i++ {
		v, _, err := m.alloc(m.blockSize, false, nil)
		require.NoError(t, err)
		v.unpin()
	}
	large, buf, err := m.alloc(3<<20, false, nil)
	require.NoError(t, err)
	require.Len(t, buf, 3<<20)
	require.Nil(t, large.block.chunk)
	require.Equal(t, int64(3<<20), m.used())
	_, _, err = m.alloc(2<<20, false, nil)
	require.ErrorIs(t, err, ErrBusy, "a pinned dedicated mapping counts against the budget")
	large.unpin()
	normal, _, err := m.alloc(m.blockSize, false, nil)
	require.NoError(t, err)
	normal.unpin()
	require.LessOrEqual(t, m.used(), m.limit)
}

func TestMemoryEvictionClearsIndexBeforeReuse(t *testing.T) {
	m, err := newMemory(4 << 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, m.close()) }()
	x := newIndex(0)
	m.onEvict = x.evictMemory
	key := blobstore.HashKey([]byte("cached"))
	loc := locations(t, 1)[0]
	old, _, err := m.alloc(m.blockSize, false, nil)
	require.NoError(t, err)
	old.block.keys = append(old.block.keys, key)
	x.put(key, old)
	require.True(t, x.landed(key, old, loc))
	snapshot, _ := x.get(key) // reader paused before pinning
	old.unpin()
	for i := 0; i < 4; i++ {
		v, _, err := m.alloc(m.blockSize, false, nil)
		require.NoError(t, err)
		v.unpin()
	}
	it, ok := x.get(key)
	require.True(t, ok)
	require.Equal(t, loc, it.loc)
	require.Nil(t, it.mem.block)
	require.Nil(t, it.mem.data)
	require.False(t, snapshot.mem.pin())
	// New publications survive cleanup of an older block's key list.
	newer, _, err := m.alloc(m.blockSize, false, nil)
	require.NoError(t, err)
	newer.block.keys = append(newer.block.keys, key)
	x.put(key, newer)
	x.evictMemory([]*memoryBlock{old.block})
	it, _ = x.get(key)
	require.Same(t, newer.block, it.mem.block)
	require.NotSame(t, old.block, newer.block)
	newer.unpin()
}

func TestPutAndGetKeepDownloadBuffer(t *testing.T) {
	c := openCache(t, t.TempDir())
	defer closeCache(t, c)
	buf, err := c.Alloc(c.RecordSize(1, 1000))
	require.NoError(t, err)
	ptr := unsafe.SliceData(buf)
	ticket, err := c.Put([]byte("k"), buf, 1000)
	require.NoError(t, err)
	require.NoError(t, ticket.Wait())
	require.NoError(t, c.Get([]byte("k"), func(value []byte) error {
		require.Equal(t, ptr, unsafe.SliceData(value))
		return nil
	}))
}

func TestColdReadersSharePinnedBuffer(t *testing.T) {
	const readers = 16
	dir := t.TempDir()
	c := openCache(t, dir)
	value := randomBytes(1, 100<<10)
	put(t, c, "shared", value)
	closeCache(t, c)
	gate := newGate()
	var submits atomic.Int64
	c = openCache(t, dir, WithMemory(1<<20), withoutMemoryHits(), withPreSubmit(func(op iosched.Op) iosched.Op {
		submits.Add(1)
		return gate.hook(op)
	}))
	defer closeCache(t, c)
	gate.armed.Store(true)
	entered := make(chan *byte, readers)
	done := make(chan error, readers)
	releaseCallbacks := make(chan struct{})
	start := func() {
		go func() {
			done <- c.Get([]byte("shared"), func(buf []byte) error {
				entered <- unsafe.SliceData(buf)
				<-releaseCallbacks
				if !bytes.Equal(buf, value) {
					return errors.New("shared memory changed during callback")
				}
				return nil
			})
		}()
	}
	start()
	<-gate.blocked
	for i := 1; i < readers; i++ {
		start()
	}
	require.Eventually(t, func() bool {
		c.reads.Lock()
		defer c.reads.Unlock()
		for _, f := range c.reads.pending {
			return f.users == readers
		}
		return false
	}, 5*time.Second, time.Millisecond)
	close(gate.release)
	first := <-entered
	for i := 1; i < readers; i++ {
		require.Equal(t, first, <-entered)
	}
	require.Equal(t, int64(1), submits.Load(), "one open/read submission for all readers")
	// Exhaust memory while every callback is using the shared record.
	var held [][]byte
	for {
		buf, err := c.Alloc(4096)
		if errors.Is(err, ErrBusy) {
			break
		}
		require.NoError(t, err)
		held = append(held, buf)
	}
	close(releaseCallbacks)
	for range readers {
		require.NoError(t, <-done)
	}
	for _, buf := range held {
		require.NoError(t, c.Free(buf))
	}
	c.reads.Lock()
	require.Empty(t, c.reads.pending)
	c.reads.Unlock()
}

func TestPutRequiresFramingSpace(t *testing.T) {
	c := openCache(t, t.TempDir())
	defer closeCache(t, c)
	buf, err := c.Alloc(4096)
	require.NoError(t, err)
	_, err = c.Put([]byte("k"), buf, len(buf))
	require.ErrorContains(t, err, "framing space")
	require.ErrorIs(t, c.Free(buf), ErrForeignMemory)
	require.Zero(t, c.Stats().Items)
}
