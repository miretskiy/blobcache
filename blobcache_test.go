//go:build ignore

// TODO: Port these tests when the read path and disk eviction are implemented.
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

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/internal/xmap"
	dioalloc "github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

const (
	testSegment = 256 << 10
	testMemory  = 16 << 20
)

func openCache(t *testing.T, dir string, opts ...Option) *Cache {
	t.Helper()
	c, err := New(dir, append([]Option{WithTargetSize(8 << 20), WithMemory(testMemory)}, opts...)...)
	require.NoError(t, err)
	return c
}

func closeCache(t *testing.T, c *Cache) {
	t.Helper()
	require.NoError(t, c.Close())
}

// alloc lends cache memory holding value, as a download would fill it,
// retrying ErrBusy when memory is still in use.
func alloc(t *testing.T, c *Cache, key string, value []byte) memoryValue {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		buf, dst, err := c.allocRecord(len(key), len(value))
		if errors.Is(err, ErrBusy) && time.Now().Before(deadline) {
			continue
		}
		require.NoError(t, err)
		copy(dst, value)
		return buf
	}
}

// waitWrites waits for DATA callbacks to release their I/O references.
func waitWrites(t testing.TB, c *Cache) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for c.stats.WritesInFlight.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("writes did not finish")
		}
		runtime.Gosched()
	}
}

func put(t *testing.T, c *Cache, key string, value []byte) {
	t.Helper()
	require.NoError(t, c.putRecord([]byte(key), alloc(t, c, key, value), len(value)))
	waitWrites(t, c)
	require.NoError(t, c.Err())
}

func TestReplacementAndRecovery(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	for _, step := range []struct {
		key, value string
	}{
		{"key", "first"},
		{"key", "first"},
		{"key", "second"},
		{"other", "third"},
		{"key", "first"},
	} {
		put(t, c, step.key, []byte(step.value))
		require.True(t, requireValue(t, c, step.key, []byte(step.value)))
	}
	closeCache(t, c)
	c = openCache(t, dir, storeOpt(blobstore.TestingWithDirectIO(false)))
	defer closeCache(t, c)
	requireMissing(t, c, "key") // without a persisted cursor, duplicate-key age is ambiguous
	requireValue(t, c, "other", []byte("third"))
	put(t, c, "key", []byte("first"))
	put(t, c, "key", []byte("fourth"))
	requireValue(t, c, "key", []byte("fourth"))
}

func TestConcurrentReplacements(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	var wg sync.WaitGroup
	for i := range 32 {
		value := []byte(fmt.Sprint(i))
		buf := alloc(t, c, "key", value)
		wg.Go(func() { require.NoError(t, c.putRecord([]byte("key"), buf, len(value))) })
	}
	wg.Wait()
	waitWrites(t, c)
	_, fromMemory, ok := lookup(t, c, "key")
	require.True(t, ok)
	require.True(t, fromMemory)
	closeCache(t, c)
	c = openCache(t, dir)
	defer closeCache(t, c)
	requireMissing(t, c, "key") // recovery discards ambiguous duplicate-key candidates
}

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

func dataFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "DATA"))
	require.NoError(t, err)
	return files
}

// findRecord locates a record's header using its full key, then reads its
// physical length from the header. This is independent of I/O open flags.
func findRecord(t *testing.T, dir, key string, valueLen int) (path string, start, end int64) {
	t.Helper()
	for _, path := range dataFiles(t, dir) {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		if i := bytes.Index(data, []byte(key)); i >= blobstore.HeaderSize {
			start := i - blobstore.HeaderSize
			size := binary.LittleEndian.Uint32(data[start+16:])
			return path, int64(start), int64(start) + int64(size)
		}
	}
	t.Fatalf("no record of %q", key)
	return "", 0, 0
}

func alignedStoreBuffer(t *testing.T, st *blobstore.Store, key string, n int) *dioalloc.Buffer {
	t.Helper()
	b := dioalloc.NewBuffer(int64(st.RecordSize(len(key), n)))
	t.Cleanup(b.Unpin)
	return b
}

// locations writes n records to a store of its own and returns their
// Locations, in write order.
func locations(t *testing.T, n int) []blobstore.Descriptor {
	t.Helper()
	st, err := blobstore.Open(filepath.Join(t.TempDir(), "cache"))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	locs := make([]blobstore.Descriptor, n)
	for i := range locs {
		ticket, err := st.Append([]byte(fmt.Sprint(i)), alignedStoreBuffer(t, st, fmt.Sprint(i), 10), 10, nil)
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
	c := openCache(t, dir, WithTargetSize(4<<20))
	sizes := []int{0, 1, 100, 4048, 4049, 4096, 4097, 64<<10 - 48 - 3, 64 << 10, 3*64<<10 + 17, 1 << 20}
	values := map[string][]byte{}
	for i, size := range sizes {
		key := fmt.Sprintf("key-%d", i)
		values[key] = randomBytes(uint64(i), size)
		buf := alloc(t, c, key, values[key])
		err := c.putRecord([]byte(key), buf, size)
		require.NoError(t, err)
		require.True(t, requireValue(t, c, key, values[key]), "served from Put's memory")
		waitWrites(t, c)
	}
	requireMissing(t, c, "absent")
	require.Equal(t, len(sizes), c.Items())
	closeCache(t, c)

	c = openCache(t, dir, WithTargetSize(4<<20))
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
	requireMissing(t, c, "k") // duplicate-key candidates have no persisted age
}

func TestReopenPackedIndex(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	values := map[string][]byte{}
	for i := range 40 {
		key := fmt.Sprintf("key-%d", i)
		values[key] = randomBytes(uint64(i), 20<<10+i*997)
		put(t, c, key, values[key])
	}
	closeCache(t, c)
	require.Len(t, dataFiles(t, dir), 1)

	c = openCache(t, dir)
	defer closeCache(t, c)
	for key, value := range values {
		requireValue(t, c, key, value)
	}
	require.Equal(t, len(values), c.Items())
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
	require.LessOrEqual(t, c.MemoryUsed(), int64(memory))
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
	var held []memoryValue
	for _, size := range []int{200 << 10, 4096} {
		for {
			buf, err := c.alloc(size)
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
		c.free(buf)
	}
	requireValue(t, c, "on-disk", randomBytes(1, 100<<10))
}

func TestOversizedMemoryAndBorrowerError(t *testing.T) {
	c := openCache(t, t.TempDir())
	defer closeCache(t, c)
	_, err := c.alloc(testMemory + 1)
	require.ErrorIs(t, err, ErrValueTooLarge)
	put(t, c, "k", []byte("value"))
	refused := errors.New("refused")
	require.ErrorIs(t, c.Get([]byte("k"), func([]byte) error { return refused }), refused)
}

// TestCloseReportsHeldMemory checks that Close refuses to unmap memory a
// caller still holds from Alloc.
func TestCloseReportsHeldMemory(t *testing.T) {
	c := openCache(t, t.TempDir())
	buf, err := c.alloc(4096)
	require.NoError(t, err)
	require.ErrorContains(t, c.Close(), "still held")
	c.free(buf)
	require.NoError(t, c.Close(), "returning the borrowed buffer permits cleanup")
	require.Zero(t, c.mem.used())
	require.NoError(t, c.Close(), "successful Close remains idempotent")
}

// TestWithoutMemoryHits checks the disk-only mode used to compare the memory
// tier with a disk-only cache: every hit is a read from disk.
func TestWithoutMemoryHits(t *testing.T) {
	c := openCache(t, t.TempDir(), withoutMemoryHits())
	defer closeCache(t, c)
	put(t, c, "k", []byte("value"))
	require.False(t, requireValue(t, c, "k", []byte("value")))
	require.False(t, requireValue(t, c, "k", []byte("value")))
	require.Zero(t, c.Stats().MemoryHits.Load())
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
	c := openCache(t, dir)
	put(t, c, "value-corrupt", randomBytes(1, 10000))
	put(t, c, "header-corrupt", randomBytes(2, 10000))
	closeCache(t, c)

	path, start, _ := findRecord(t, dir, "value-corrupt", 10000)
	flipByte(t, path, start+int64(blobstore.ValueOffset(len("value-corrupt")))+3)
	path, start, _ = findRecord(t, dir, "header-corrupt", 10000)
	flipByte(t, path, start+20)

	c = openCache(t, dir)
	defer closeCache(t, c)
	err := c.Get([]byte("value-corrupt"), func([]byte) error { return nil })
	var ce *blobstore.ChecksumError
	require.ErrorAs(t, err, &ce)
	requireMissing(t, c, "value-corrupt") // the entry was dropped

	err = c.Get([]byte("header-corrupt"), func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, uint64(2), c.Stats().Corrupt.Load())
}

func TestBufferedIO(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := openCache(t, dir, storeOpt(blobstore.TestingWithDirectIO(false)))
	value := randomBytes(1, 10000)
	put(t, c, "k", value)
	closeCache(t, c)
	c = openCache(t, dir, storeOpt(blobstore.TestingWithDirectIO(false)))
	defer closeCache(t, c)
	require.False(t, requireValue(t, c, "k", value))
}

func TestPutRejects(t *testing.T) {
	c := openCache(t, t.TempDir(), WithTargetSize(64<<10))
	for _, key := range [][]byte{nil, make([]byte, MaxKeyLen+1)} {
		buf, err := c.alloc(4096)
		require.NoError(t, err)
		err = c.putRecord(key, buf, 1)
		require.Error(t, err)
	}
	buf, err := c.alloc(testSegment)
	require.NoError(t, err)
	err = c.putRecord([]byte("huge"), buf, testSegment-4096)
	require.ErrorIs(t, err, ErrValueTooLarge)
	require.Equal(t, 0, c.Items())

	closeCache(t, c)
	require.NoError(t, c.Close(), "a second Close is a no-op")
	_, err = c.alloc(4096)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, c.Get([]byte("k"), func([]byte) error { return nil }), ErrClosed)
}

// Close must finish queued writes before returning their memory to the allocator.
func TestCloseDrainsAcceptedWrites(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir)
	for i := range 50 {
		key := fmt.Sprint(i)
		value := randomBytes(uint64(i), 30<<10)
		require.NoError(t, c.putRecord([]byte(key), alloc(t, c, key, value), len(value)))
	}
	closeCache(t, c) // no cache-side drain or per-write waits
	require.Zero(t, c.Stats().WritesInFlight.Load())
	c = openCache(t, dir)
	defer closeCache(t, c)
	for i := range 50 {
		requireValue(t, c, fmt.Sprint(i), randomBytes(uint64(i), 30<<10))
	}
}

func TestConcurrentStress(t *testing.T) {
	dir := t.TempDir()
	opts := []Option{WithMemory(2 << 20)}
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
					buf, dst, err := c.allocRecord(len(key), len(v))
					if errors.Is(err, ErrBusy) {
						continue // a caller skips caching
					}
					require.NoError(t, err)
					copy(dst, v)
					err = c.putRecord([]byte(key), buf, len(v))
					require.NoError(t, err)
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
	require.Positive(t, c.Stats().Wraps.Load(), "stress must wrap the file")
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

// TestIndexPutLifecycle covers immediate publication, conditional memory
// eviction, and replacement by a later record.
func TestIndexPutLifecycle(t *testing.T) {
	locs := locations(t, 3)
	x := newIndex(0)
	h := blobstore.HashKey([]byte("k"))
	values := make([]*resident, 3)
	for i := range values {
		b := &memoryRecord{buffer: dioalloc.NewBuffer(4096), key: h, indexed: true}
		defer b.buffer.Unpin()
		values[i] = &resident{memoryValue: memoryValue{record: b}}
	}
	x.put(h, values[0], locs[0])
	x.put(h, values[1], locs[1])
	x.put(h, values[2], locs[2])
	x.evictMemory(values[1].record)
	it, _ := x.get(h)
	require.Equal(t, item{loc: locs[2], mem: values[2]}, it)
	x.deleteIfAt(h, locs[0])
	it, _ = x.get(h)
	require.Equal(t, locs[2], it.loc)
	x.deleteIfAt(h, locs[2])
	it, ok := x.get(h)
	require.True(t, ok)
	require.False(t, it.onDisk())
	require.Same(t, values[2], it.mem, "disk failure preserves resident memory")
	x.evictMemory(values[2].record)
	_, ok = x.get(h)
	require.False(t, ok)
	x.loaded(h, locs[2])
	x.cache(h, locs[0], values[0])
	it, _ = x.get(h)
	require.Nil(t, it.mem, "old read cannot replace a newer location")
	x.cache(h, locs[2], values[2])
	x.evictMemory(values[2].record)
	it, ok = x.get(h)
	require.True(t, ok)
	require.Equal(t, locs[2], it.loc, "memory eviction preserves disk")
}

func TestDiskCacheEviction(t *testing.T) {
	dir := t.TempDir()
	c := openCache(t, dir, WithTargetSize(3*64<<10), withoutMemoryHits())
	defer func() { closeCache(t, c) }()
	value := randomBytes(42, 20<<10)
	put(t, c, "old", value)
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("key-%d", i)
		put(t, c, key, value)
		require.False(t, requireValue(t, c, key, value), "reads must come from disk")
	}
	require.Eventually(t, func() bool {
		it, ok := c.index.get(blobstore.HashKey([]byte("old")))
		return !ok || !it.onDisk()
	}, 5*time.Second, time.Millisecond)
	require.Positive(t, c.Stats().EvictedRecords.Load())
	closeCache(t, c)
	c = openCache(t, dir, WithTargetSize(3*64<<10))
	requireValue(t, c, "key-29", value)
}
