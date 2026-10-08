package blobcache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

const (
	testSegment = 256 << 10
	testMemory  = 16 << 20
)

func openCache(t *testing.T, dir string, opts ...Option) *Cache {
	t.Helper()
	c, err := New(dir, append([]Option{WithSegmentSize(testSegment), WithMemory(testMemory)}, opts...)...)
	require.NoError(t, err)
	return c
}

func closeCache(t *testing.T, c *Cache) {
	t.Helper()
	require.NoError(t, c.Close())
}

// alloc lends cache memory holding value, as a download would fill it,
// retrying ErrBusy: with tiny test segments, rotation can briefly outrun
// seals, and the oldest memory may be briefly in use.
func alloc(t *testing.T, c *Cache, key string, value []byte) []byte {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		buf, err := c.Alloc(c.RecordSize(len(key), len(value)))
		if errors.Is(err, ErrBusy) && time.Now().Before(deadline) {
			continue
		}
		require.NoError(t, err)
		copy(buf, value)
		return buf
	}
}

// put stores value, waits for the write, and waits for the completer to
// record where it landed.
func put(t *testing.T, c *Cache, key string, value []byte) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		ticket, err := c.Put([]byte(key), alloc(t, c, key, value), len(value))
		if errors.Is(err, ErrBusy) && time.Now().Before(deadline) {
			continue
		}
		require.NoError(t, err)
		err = ticket.Wait()
		require.NoError(t, err)
		c.Drain()
		return
	}
}

// lookup returns a copy of key's value and whether it came from memory.
func lookup(t *testing.T, c *Cache, key string) (value []byte, fromMemory, ok bool) {
	t.Helper()
	fromMemory, err := c.get([]byte(key), func(v []byte) error {
		value = bytes.Clone(v)
		return nil
	})
	if errors.Is(err, ErrNotFound) {
		return nil, false, false
	}
	require.NoError(t, err)
	return value, fromMemory, true
}

func requireValue(t *testing.T, c *Cache, key string, want []byte) (fromMemory bool) {
	t.Helper()
	got, fromMemory, ok := lookup(t, c, key)
	require.True(t, ok, "key %q missing", key)
	require.Equal(t, want, got, "key %q", key)
	return fromMemory
}

func requireMissing(t *testing.T, c *Cache, key string) {
	t.Helper()
	_, _, ok := lookup(t, c, key)
	require.False(t, ok, "key %q unexpectedly present", key)
}

func randomBytes(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := 0; i < n; i += 8 {
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], rng.Uint64())
		copy(b[i:], w[:])
	}
	return b
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*", "*.seg"))
	require.NoError(t, err)
	return files
}

// findRecord returns the segment file holding key's record, written with a
// value of valueLen bytes with direct I/O, and the record's bounds in it. The
// key is stored just before the record's trailer, which ends the record.
func findRecord(t *testing.T, dir, key string, valueLen int) (path string, start, end int64) {
	t.Helper()
	for _, path := range segmentFiles(t, dir) {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		if i := bytes.Index(data, []byte(key)); i >= 0 {
			end := int64(i + len(key) + trailerSize)
			return path, end - align.PageAlign(int64(valueLen+len(key)+trailerSize)), end
		}
	}
	t.Fatalf("no record of %q", key)
	return "", 0, 0
}

// trailerSize is the record trailer's size (see blobstore's format).
const trailerSize = 20

// locations writes n records to a store of its own and returns their
// Locations, in write order.
func locations(t *testing.T, n int) []blobstore.Location {
	t.Helper()
	st, err := blobstore.Open(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	locs := make([]blobstore.Location, n)
	for i := range locs {
		ticket, err := st.Write([]byte(fmt.Sprint(i)), make([]byte, 100), 10)
		require.NoError(t, err)
		locs[i], err = ticket.Wait()
		require.NoError(t, err)
	}
	return locs
}

func TestIndexShardAlignment(t *testing.T) {
	require.NoError(t, xmap.VerifyAlignment[item, xmap.Pad32]())
}

// TestPutGetRoundTrip checks values of many sizes: served from memory once
// Put returns, from disk after a restart, and from memory again after that
// read.
func TestPutGetRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, WithSegmentSize(4<<20))
	sizes := []int{0, 1, 100, 4048, 4049, 4096, 4097, 64<<10 - 48 - 3, 64 << 10, 3*64<<10 + 17, 1 << 20}
	values := map[string][]byte{}
	for i, size := range sizes {
		key := fmt.Sprintf("key-%d", i)
		values[key] = randomBytes(uint64(i), size)
		buf := alloc(t, c, key, values[key])
		ticket, err := c.Put([]byte(key), buf, size)
		require.NoError(t, err)
		require.True(t, requireValue(t, c, key, values[key]), "served from Put's memory")
		err = ticket.Wait()
		require.NoError(t, err)
	}
	requireMissing(t, c, "absent")
	require.Equal(t, len(sizes), c.Stats().Items)
	closeCache(t, c)

	c = openCache(t, dir, WithSegmentSize(4<<20))
	defer closeCache(t, c)
	for key, value := range values {
		require.False(t, requireValue(t, c, key, value), "first read after restart is from disk")
		require.True(t, requireValue(t, c, key, value), "a read from disk stays in memory")
	}
}

func TestOverwrite(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	put(t, c, "k", []byte("one"))
	put(t, c, "k", []byte("two"))
	requireValue(t, c, "k", []byte("two"))
	closeCache(t, c)

	c = openCache(t, dir)
	defer closeCache(t, c)
	requireValue(t, c, "k", []byte("two"))
}

func TestReopenAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	values := map[string][]byte{}
	for i := range 40 {
		key := fmt.Sprintf("key-%d", i)
		values[key] = randomBytes(uint64(i), 20<<10+i*997)
		put(t, c, key, values[key])
	}
	closeCache(t, c)
	require.Greater(t, len(segmentFiles(t, dir)), 2, "test must span several segments")

	c = openCache(t, dir)
	defer closeCache(t, c)
	for key, value := range values {
		requireValue(t, c, key, value)
	}
	require.Equal(t, len(values), c.Stats().Items)
}

// TestMemoryReclaimedOldestFirst writes more than the cache's memory: the
// newest values stay in memory, the oldest are read back from disk.
func TestMemoryReclaimedOldestFirst(t *testing.T) {
	const memory = 1 << 20
	c := openCache(t, t.TempDir(), WithMemory(memory))
	defer closeCache(t, c)
	value := func(i int) []byte { return randomBytes(uint64(i), 100<<10) }
	const n = 30 // about 3 MB
	for i := range n {
		put(t, c, fmt.Sprintf("key-%d", i), value(i))
	}
	require.LessOrEqual(t, c.Stats().MemoryUsed, int64(memory))
	require.True(t, requireValue(t, c, fmt.Sprintf("key-%d", n-1), value(n-1)), "newest in memory")
	require.False(t, requireValue(t, c, "key-0", value(0)), "oldest reclaimed, read from disk")
}

// TestBusyWhenMemoryInUse checks that memory in use is never reclaimed:
// Alloc and reads from disk return ErrBusy, without waiting, until it is
// given back.
func TestBusyWhenMemoryInUse(t *testing.T) {
	const memory = 1 << 20
	c := openCache(t, t.TempDir(), WithMemory(memory))
	defer closeCache(t, c)
	put(t, c, "on-disk", randomBytes(1, 100<<10))
	// Hold all the memory: large buffers, then pages until none is left.
	var held [][]byte
	for _, size := range []int{200 << 10, 4096} {
		for {
			buf, err := c.Alloc(size)
			if errors.Is(err, ErrBusy) {
				break
			}
			require.NoError(t, err)
			held = append(held, buf)
		}
	}
	// "on-disk" was reclaimed to make room; reading it back needs memory.
	err := c.Get([]byte("on-disk"), func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrBusy)

	for _, buf := range held {
		require.NoError(t, c.Free(buf))
	}
	requireValue(t, c, "on-disk", randomBytes(1, 100<<10))
}

// TestMemoryRules covers handing memory back: Put and Free each take memory
// from Alloc exactly once, whatever the outcome, and refuse other memory.
func TestMemoryRules(t *testing.T) {
	c := openCache(t, t.TempDir())
	defer closeCache(t, c)

	foreign := align.AllocAligned(64 << 10)
	defer align.FreeAligned(foreign)
	_, err := c.Put([]byte("k"), foreign, 10)
	require.ErrorIs(t, err, ErrForeignMemory)
	require.ErrorIs(t, c.Free(foreign), ErrForeignMemory)

	buf, err := c.Alloc(c.RecordSize(1, 10000))
	require.NoError(t, err)
	_, err = c.Put([]byte("k"), buf[4096:], 10)
	require.ErrorIs(t, err, ErrForeignMemory, "only the memory Alloc returned")
	_, err = c.Put(nil, buf, 10)
	require.ErrorIs(t, err, ErrEmptyKey)
	require.ErrorIs(t, c.Free(buf), ErrForeignMemory, "a failed Put still took the memory")

	buf, err = c.Alloc(c.RecordSize(1, 10))
	require.NoError(t, err)
	require.NoError(t, c.Free(buf))
	require.ErrorIs(t, c.Free(buf), ErrForeignMemory, "freed twice")

	buf = alloc(t, c, "k", []byte("value"))
	_, err = c.Put([]byte("k"), buf, 5)
	require.NoError(t, err)
	_, err = c.Put([]byte("k"), buf, 5)
	require.ErrorIs(t, err, ErrForeignMemory, "Put twice")
	c.Drain()

	_, err = c.Alloc(testMemory + 1)
	require.ErrorIs(t, err, ErrValueTooLarge)

	// fn's error is Get's.
	refused := errors.New("refused")
	require.ErrorIs(t, c.Get([]byte("k"), func([]byte) error { return refused }), refused)
}

// TestCloseReportsHeldMemory checks that Close refuses to unmap memory a
// caller still holds from Alloc.
func TestCloseReportsHeldMemory(t *testing.T) {
	c := openCache(t, t.TempDir())
	_, err := c.Alloc(4096)
	require.NoError(t, err)
	require.ErrorContains(t, c.Close(), "still held")
}

// gate holds the store's next submission while armed, before it reaches the
// scheduler.
type gate struct {
	armed   atomic.Bool
	blocked chan struct{}
	release chan struct{}
}

func newGate() *gate {
	return &gate{blocked: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) hook(op iosched.Op) iosched.Op {
	if g.armed.CompareAndSwap(true, false) {
		g.blocked <- struct{}{}
		<-g.release
	}
	return op
}

// putHeld starts a Put that the gate holds before submission, and returns a
// channel that delivers Put's error when it returns; its write may still be
// in flight then.
func putHeld(t *testing.T, c *Cache, gate *gate, key string, value []byte) <-chan error {
	t.Helper()
	buf := alloc(t, c, key, value)
	gate.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := c.Put([]byte(key), buf, len(value))
		done <- err
	}()
	<-gate.blocked
	return done
}

// TestPutVisibleOnReturn checks that a key is absent until Put returns, and is
// served from Put's memory from then on, before and after its write lands.
func TestPutVisibleOnReturn(t *testing.T) {
	gate := newGate()
	c := openCache(t, t.TempDir(), withPreSubmit(gate.hook))
	defer closeCache(t, c)

	value := randomBytes(1, 100<<10)
	done := putHeld(t, c, gate, "k", value) // inside Put, not yet submitted
	requireMissing(t, c, "k")

	close(gate.release)
	require.NoError(t, <-done)
	require.True(t, requireValue(t, c, "k", value))
	c.Drain()
	require.True(t, requireValue(t, c, "k", value))
}

// TestOverwriteVisibleOnReturn checks that an overwrite replaces the value
// for Get when Put returns.
func TestOverwriteVisibleOnReturn(t *testing.T) {
	gate := newGate()
	c := openCache(t, t.TempDir(), withPreSubmit(gate.hook))
	defer closeCache(t, c)

	put(t, c, "k", []byte("old"))
	done := putHeld(t, c, gate, "k", []byte("new"))
	requireValue(t, c, "k", []byte("old"))
	close(gate.release)
	require.NoError(t, <-done)
	requireValue(t, c, "k", []byte("new"))
}

// TestWithoutMemoryHits checks the disk-only mode used to compare the memory
// tier with a disk-only cache: every hit is a read from disk.
func TestWithoutMemoryHits(t *testing.T) {
	c := openCache(t, t.TempDir(), withoutMemoryHits())
	defer closeCache(t, c)
	put(t, c, "k", []byte("value"))
	require.False(t, requireValue(t, c, "k", []byte("value")))
	require.False(t, requireValue(t, c, "k", []byte("value")))
	require.Zero(t, c.Stats().MemoryHits)
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestCorruptionIsDetected(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, WithChecksum())
	put(t, c, "value-corrupt", randomBytes(1, 10000))
	put(t, c, "trailer-corrupt", randomBytes(2, 10000))
	closeCache(t, c)

	path, start, _ := findRecord(t, dir, "value-corrupt", 10000)
	flipByte(t, path, start+100)
	path, _, end := findRecord(t, dir, "trailer-corrupt", 10000)
	flipByte(t, path, end-10)

	c = openCache(t, dir, WithChecksum())
	defer closeCache(t, c)
	err := c.Get([]byte("value-corrupt"), func([]byte) error { return nil })
	var ce *base.ChecksumError
	require.ErrorAs(t, err, &ce)
	requireMissing(t, c, "value-corrupt") // the entry was dropped

	err = c.Get([]byte("trailer-corrupt"), func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, uint64(2), c.Stats().Corrupt)
}

func TestBufferedReads(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, WithDirectReads(false))
	value := randomBytes(1, 10000)
	put(t, c, "k", value)
	closeCache(t, c)
	c = openCache(t, dir, WithDirectReads(false))
	defer closeCache(t, c)
	require.False(t, requireValue(t, c, "k", value))
}

func TestPutRejects(t *testing.T) {
	c := openCache(t, t.TempDir())
	for _, key := range [][]byte{nil, make([]byte, MaxKeyLen+1)} {
		buf, err := c.Alloc(4096)
		require.NoError(t, err)
		_, err = c.Put(key, buf, 1)
		require.Error(t, err)
	}
	buf, err := c.Alloc(testSegment)
	require.NoError(t, err)
	_, err = c.Put([]byte("huge"), buf, testSegment-4096)
	require.ErrorIs(t, err, ErrValueTooLarge)
	require.Equal(t, 0, c.Stats().Items)

	closeCache(t, c)
	require.NoError(t, c.Close(), "a second Close is a no-op")
	_, err = c.Alloc(4096)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, c.Get([]byte("k"), func([]byte) error { return nil }), ErrClosed)
}

// TestDrain checks that Drain waits for every write accepted before it.
func TestDrain(t *testing.T) {
	c := openCache(t, t.TempDir())
	defer closeCache(t, c)
	var tickets []Ticket
	for i := range 50 {
		key := fmt.Sprint(i)
		value := randomBytes(uint64(i), 30<<10)
		for {
			ticket, err := c.Put([]byte(key), alloc(t, c, key, value), len(value))
			if errors.Is(err, ErrBusy) {
				runtime.Gosched()
				continue
			}
			require.NoError(t, err)
			tickets = append(tickets, ticket)
			break
		}
	}
	c.Drain()
	require.Zero(t, c.Stats().WritesInFlight)
	for i := range 50 {
		it, ok := c.index.get(blobstore.HashKey([]byte(fmt.Sprint(i))))
		require.True(t, ok)
		require.True(t, it.onDisk(), "Drain returned before the completer recorded every write")
	}
	for _, ticket := range tickets {
		err := ticket.Wait()
		require.NoError(t, err)
	}
	c.Drain() // nothing in flight: returns at once
}

// TestConcurrentStress mixes writes, overwrites and reads across rotating
// segments and memory reclaimed under pressure. Every value names
// its key, so any hit can be checked: a read must never return another key's
// data.
func TestConcurrentStress(t *testing.T) {
	dir := t.TempDir()
	opts := []Option{WithChecksum(), WithMemory(2 << 20)}
	c := openCache(t, dir, opts...)
	const keys = 200
	value := func(key string, version uint64, size int) []byte {
		return append([]byte(key+"|"), randomBytes(version, size)...)
	}
	var wg sync.WaitGroup
	var hits atomic.Int64
	for g := range 8 {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed))
			for range 400 {
				key := fmt.Sprintf("key-%d", rng.IntN(keys))
				switch op := rng.IntN(10); {
				case op < 5:
					v := value(key, rng.Uint64(), rng.IntN(40<<10))
					buf, err := c.Alloc(c.RecordSize(len(key), len(v)))
					if errors.Is(err, ErrBusy) {
						continue // a caller skips caching
					}
					require.NoError(t, err)
					copy(buf, v)
					ticket, err := c.Put([]byte(key), buf, len(v))
					if errors.Is(err, ErrBusy) {
						continue
					}
					require.NoError(t, err)
					if rng.IntN(2) == 0 {
						err = ticket.Wait()
						require.NoError(t, err)
					}
				default:
					err := c.Get([]byte(key), func(got []byte) error {
						if !bytes.HasPrefix(got, []byte(key+"|")) {
							return fmt.Errorf("read of %s returned another key's data", key)
						}
						hits.Add(1)
						return nil
					})
					if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrBusy) {
						require.NoError(t, err)
					}
				}
			}
		}(uint64(g))
	}
	wg.Wait()
	require.Greater(t, c.Stats().Segments, 3, "stress must span several segments")
	require.Positive(t, hits.Load())
	closeCache(t, c)

	c = openCache(t, dir, opts...)
	defer closeCache(t, c)
	for i := range keys {
		key := fmt.Sprintf("key-%d", i)
		if got, _, ok := lookup(t, c, key); ok {
			require.True(t, bytes.HasPrefix(got, []byte(key+"|")))
		}
	}
}

// TestIndexPutLifecycle covers a Put's entry: in memory until its write
// lands, superseded by a later Put, removed if its write fails.
func TestIndexPutLifecycle(t *testing.T) {
	locs := locations(t, 3)
	x := newIndex(0)
	h := blobstore.HashKey([]byte("k"))
	loc := locs[0]
	values := make([]memoryValue, 12)
	for i := range values {
		values[i] = memoryValue{block: &memoryBlock{keys: []Key{h}}}
	}

	x.put(h, values[7])
	it, _ := x.get(h)
	require.Equal(t, item{mem: values[7]}, it)
	x.landed(h, values[7], loc)
	it, _ = x.get(h)
	require.Equal(t, item{mem: values[7], loc: loc}, it)

	x.put(h, values[8]) // overwrite
	x.put(h, values[9]) // and another, installed later
	x.landed(h, values[8], locs[1])
	x.evictMemory([]*memoryBlock{values[8].block})
	it, _ = x.get(h)
	require.Equal(t, item{mem: values[9]}, it, "a superseded write changes nothing")
	x.evictMemory([]*memoryBlock{values[9].block})
	_, ok := x.get(h)
	require.False(t, ok, "expired memory without a disk copy leaves nothing")

	x.loaded(h, loc)
	x.cache(h, loc, values[10])
	it, _ = x.get(h)
	require.Equal(t, values[10], it.mem, "a read from disk is kept in memory")
	x.cache(h, locs[2], values[11])
	it, _ = x.get(h)
	require.Equal(t, values[10], it.mem, "only for the record the index names")
	x.deleteIfAt(h, locs[2])
	_, ok = x.get(h)
	require.True(t, ok, "only the record the index names is removed")
	x.deleteIfAt(h, loc)
	_, ok = x.get(h)
	require.False(t, ok)
}
