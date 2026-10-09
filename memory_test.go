package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
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
	require.NoError(t, err, "retire the oldest blocks, even while pinned")
	require.False(t, old.pin(), "retired descriptors cannot pin reused storage")
	require.False(t, values[0].pin(), "eviction immediately refuses new readers")
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
	x.put(key, old, loc)
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
	x.put(key, newer, loc)
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
		require.True(t, ptr == unsafe.SliceData(value), "Get must lend the original download buffer")
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
		require.True(t, first == <-entered, "readers must share the same buffer address")
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
	err := c.Get([]byte("shared"), func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrBusy, "a completed shared read cannot lend retired memory to new callers")
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

// An old callback may finish safely, but it must not keep a retired block
// available to new readers. Those readers use the disk location instead.
func TestEvictionWhileReaderHoldsBlock(t *testing.T) {
	c := openCache(t, t.TempDir(), WithMemory(4<<20))
	defer closeCache(t, c)
	value := randomBytes(23, 100<<10)
	put(t, c, "held", value)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.Get([]byte("held"), func(buf []byte) error {
			close(entered)
			<-release
			if !bytes.Equal(buf, value) {
				return errors.New("eviction reused a reader's buffer")
			}
			return nil
		})
	}()
	<-entered
	defer func() { close(release); require.NoError(t, <-done) }()
	old, ok := c.index.get(blobstore.HashKey([]byte("held")))
	require.True(t, ok)
	for range 6 {
		mem, _, err := c.mem.alloc(c.mem.blockSize, false, nil)
		require.NoError(t, err)
		mem.unpin()
	}
	require.False(t, old.mem.pin(), "new readers must not extend a retired block's life")
	it, ok := c.index.get(blobstore.HashKey([]byte("held")))
	require.True(t, ok)
	require.Nil(t, it.mem.block)
	require.False(t, requireValue(t, c, "held", value), "read from disk while the old callback still holds memory")
	// A delayed disk read's publication must not restore retired memory.
	c.index.cache(blobstore.HashKey([]byte("held")), old.loc, old.mem)
	it, _ = c.index.get(blobstore.HashKey([]byte("held")))
	require.NotSame(t, old.mem.block, it.mem.block)
}

func TestPutBusyPreservesDownloadBuffer(t *testing.T) {
	c := openCache(t, t.TempDir())
	defer closeCache(t, c)
	value := randomBytes(7, 10000)
	buf := alloc(t, c, "retry", value)
	ptr := unsafe.SliceData(buf)
	// Exhaust admission independently of disk speed, then return those slots.
	for range maxWritesInFlight {
		require.True(t, c.inflight.reserve())
	}
	_, err := c.Put([]byte("retry"), buf, len(value))
	require.ErrorIs(t, err, ErrBusy)
	require.Equal(t, value, buf[:len(value)])
	for range maxWritesInFlight {
		c.inflight.unreserve()
	}
	ticket, err := c.Put([]byte("retry"), buf, len(value))
	require.NoError(t, err)
	require.NoError(t, ticket.Wait())
	require.NoError(t, c.Get([]byte("retry"), func(got []byte) error {
		require.True(t, ptr == unsafe.SliceData(got), "retry must keep the original buffer")
		require.Equal(t, value, got)
		return nil
	}))
}

func TestRetiredBorrowerCanStillPublishDiskLocation(t *testing.T) {
	c := openCache(t, t.TempDir(), WithMemory(4<<20))
	defer closeCache(t, c)
	value := randomBytes(8, 10000)
	buf := alloc(t, c, "late", value)
	for range 6 {
		v, _, err := c.mem.alloc(c.mem.blockSize, false, nil)
		require.NoError(t, err)
		v.unpin()
	}
	ticket, err := c.Put([]byte("late"), buf, len(value))
	require.NoError(t, err)
	require.NoError(t, ticket.Wait())
	c.Drain()
	require.False(t, requireValue(t, c, "late", value), "retired download memory cannot be republished")
}

func TestRetiredReadFlightCannotRemoveReplacement(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, WithMemory(4<<20))
	value := randomBytes(12, 100<<10)
	put(t, c, "shared", value)
	closeCache(t, c)
	c = openCache(t, dir, WithMemory(4<<20))
	defer closeCache(t, c)
	var wg sync.WaitGroup
	release1, release2 := make(chan struct{}), make(chan struct{})
	finish1 := sync.OnceFunc(func() { close(release1) })
	finish2 := sync.OnceFunc(func() { close(release2) })
	defer func() { finish1(); finish2(); wg.Wait() }()
	start := func(release <-chan struct{}) (<-chan *byte, <-chan error) {
		entered, done := make(chan *byte, 1), make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			done <- c.Get([]byte("shared"), func(buf []byte) error {
				entered <- unsafe.SliceData(buf)
				<-release
				if !bytes.Equal(buf, value) {
					return errors.New("retired flight storage was reused early")
				}
				return nil
			})
		}()
		return entered, done
	}
	entered1, done1 := start(release1)
	ptr1 := <-entered1
	for range 6 {
		v, _, err := c.mem.alloc(c.mem.blockSize, false, nil)
		require.NoError(t, err)
		v.unpin()
	}
	entered2, done2 := start(release2)
	ptr2 := <-entered2
	require.True(t, ptr1 != ptr2, "new readers cannot join the retired completed flight")
	finish1()
	require.NoError(t, <-done1)
	c.reads.Lock()
	remaining := len(c.reads.pending)
	c.reads.Unlock()
	require.Equal(t, 1, remaining, "old cleanup must preserve the replacement flight")
	finish2()
	require.NoError(t, <-done2)
}

// Rotation may reject a write after memory ownership was claimed. Its buffer
// must still return to the caller, just as it does for full admission.
func TestRotationBusyPreservesDownloadBuffer(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("need two coordinator CPUs")
	}
	c := openCache(t, t.TempDir(), WithMemory(4<<20), WithSegmentSize(64<<10), WithRings(2), WithMaxSegments(4))
	defer closeCache(t, c)
	value := randomBytes(19, 8192)
	keyFor := func(n, writer int) string {
		for {
			key := fmt.Sprintf("writer-%d-%d", writer, n)
			if blobstore.HashKey([]byte(key)).Lo%2 == uint64(writer) {
				return key
			}
			n++
		}
	}
	put(t, c, keyFor(0, 0), value) // quiet writer blocks prefix eviction
	for i := range 100 {
		key := keyFor(i, 1)
		buf := alloc(t, c, key, value)
		ticket, err := c.Put([]byte(key), buf, len(value))
		if errors.Is(err, ErrBusy) {
			require.Equal(t, value, buf[:len(value)])
			require.NoError(t, c.Free(buf), "rotation returns the existing borrower pin")
			return
		}
		require.NoError(t, err)
		require.NoError(t, ticket.Wait())
		c.Drain()
	}
	t.Fatal("quiet writer never forced a rotation retry")
}
