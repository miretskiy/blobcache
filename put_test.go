package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

func newPutCache(t *testing.T, dir string, opts ...Option) *Cache {
	t.Helper()
	c, err := New(dir, append([]Option{
		WithMemory(128 << 10), WithInlineThreshold(32 << 10), WithStagingSize(8 << 10),
		storeOpt(blobstore.TestingWithDirectIO(false)),
		storeOpt(blobstore.WithDataIntegrity(blobstore.DataIntegrityXXHash128)),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func putValue(t *testing.T, c *Cache, key string, value []byte) {
	t.Helper()
	w, err := c.NewWriter([]byte(key), int64(len(value)))
	require.NoError(t, err)
	defer w.Close()
	n, err := w.Write(value)
	require.NoError(t, err)
	require.Equal(t, len(value), n)
	require.NoError(t, c.Put(w))
}

// Inspect the retained allocation directly for Put lifetime assertions.
func retainedValue(t *testing.T, c *Cache, key []byte) []byte {
	t.Helper()
	s := c.values.entries.Shard(blobstore.HashKey(key))
	s.RLock()
	defer s.RUnlock()
	e, ok := s.Items[blobstore.HashKey(key)]
	require.True(t, ok)
	require.NotZero(t, e.resident)
	var data []byte
	for _, chunk := range s.Extra.list.Value(e.resident).buffer.Chunks() {
		data = append(data, chunk...)
	}
	start := blobstore.RecordHeaderSize + int(e.header.KeyLen)
	require.Equal(t, key, data[blobstore.RecordHeaderSize:start])
	return data[start : start+int(e.header.PhysicalLen)]
}

// Return the buffer object for sequential lifetime assertions, without taking
// another reference that would mask an erroneous retain or release.
func residentBuffer(t *testing.T, c *Cache, key []byte) *alloc.Buffer {
	t.Helper()
	s := c.values.entries.Shard(blobstore.HashKey(key))
	s.RLock()
	defer s.RUnlock()
	e := s.Items[blobstore.HashKey(key)]
	require.NotZero(t, e.resident)
	return s.Extra.list.Value(e.resident).buffer
}

func residentCount(c *Cache) int {
	count := 0
	for i := range c.values.entries.ShardCount() {
		s := c.values.entries.ShardAt(i)
		s.RLock()
		count += s.Extra.list.Len()
		s.RUnlock()
	}
	return count
}

func diskRecords(t *testing.T, dir string) []blobstore.RecordHeader {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "index.db"))
	require.NoError(t, err)
	require.Zero(t, len(data)%blobstore.RecordHeaderSize)
	var records []blobstore.RecordHeader
	for len(data) != 0 {
		raw := (*blobstore.RawHeader)(data)
		if *raw != (blobstore.RawHeader{}) {
			var header blobstore.RecordHeader
			require.NoError(t, raw.Decode(&header, true))
			records = append(records, header)
		}
		data = data[blobstore.RecordHeaderSize:]
	}
	return records
}

func checkDiskValue(t *testing.T, dir string, h blobstore.RecordHeader, key, value []byte) {
	t.Helper()
	name := filepath.Join(dir, "data.db")
	offset := int64(h.Offset)
	if h.Flags.IsExternal() {
		offset = 0
		name = filepath.Join(dir, "objects", strconv.FormatUint(h.ObjectID, 10))
	}
	f, err := os.Open(name)
	require.NoError(t, err)
	defer f.Close()
	data := make([]byte, align.PageAlign(blobstore.RecordHeaderSize+int64(len(key)+len(value))))
	_, err = f.ReadAt(data, offset)
	require.NoError(t, err)
	var actual blobstore.RecordHeader
	require.NoError(t, (*blobstore.RawHeader)(data).Decode(&actual, true))
	require.Equal(t, blobstore.HashKey(key), h.Hash, "index hash identifies the key")
	h.Hash = actual.Hash
	require.Equal(t, h, actual)
	start := blobstore.RecordHeaderSize + len(key)
	require.Equal(t, key, data[blobstore.RecordHeaderSize:start])
	require.Equal(t, value, data[start:start+len(value)])
	require.Equal(t, make([]byte, len(data)-start-len(value)), data[start+len(value):])
	require.Equal(t, uint64(len(value)), h.PhysicalLen)
	require.Equal(t, h.PhysicalLen, h.LogicalLen)
	require.Equal(t, xxh3.Hash128(append(bytes.Clone(key), value...)), h.Hash)
}

func TestPutPersistsAndRetains(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			keyLen   int
			size     int
			promised int64
			external bool
		}{
			{"empty", 3, 0, 0, false},
			{"exact page", 3, align.BlockSize - blobstore.RecordHeaderSize - 3, align.BlockSize - blobstore.RecordHeaderSize - 3, false},
			{"inline", 100, 20000, 21000, false},
			{"max key", MaxKeyLen, 13, 13, false},
			{"object", MaxKeyLen, 300001, 300001, true},
			{"unknown", 3, 300001, SizeUnknown, true},
		} {
			t.Run(fmt.Sprintf("%s/direct=%v", tc.name, direct), func(t *testing.T) {
				dir := t.TempDir()
				c := newPutCache(t, dir, storeOpt(blobstore.TestingWithDirectIO(direct)))
				key := bytes.Repeat([]byte("k"), tc.keyLen)
				value := make([]byte, tc.size)
				for i := range value {
					value[i] = byte(i*37 + i/4096)
				}
				inputKey := bytes.Clone(key)
				w, err := c.NewWriter(inputKey, tc.promised)
				require.NoError(t, err)
				defer w.Close()
				clear(inputKey)
				split := len(value) / 2
				_, err = w.Write(value[:split])
				require.NoError(t, err)
				require.NoError(t, w.Flush())
				n, err := w.ReadFrom(bytes.NewReader(value[split:]))
				require.NoError(t, err)
				require.Equal(t, int64(len(value)-split), n)
				require.Equal(t, int64(len(value)), w.Size())
				require.NoError(t, c.Put(w))
				checkRead(t, c, key, value)
				require.NoError(t, w.Close())
				require.Equal(t, 1, c.values.entries.Len())
				if tc.external {
					require.Zero(t, residentCount(c))
					e, _ := c.values.entries.Get(blobstore.HashKey(key))
					require.True(t, e.header.Flags.IsExternal())
					require.Equal(t, uint64(len(value)), e.header.PhysicalLen)
				} else {
					require.Equal(t, value, retainedValue(t, c, key))
				}
				require.NoError(t, c.Close())
				records := diskRecords(t, dir)
				require.Len(t, records, 1)
				require.Equal(t, tc.external, records[0].Flags.IsExternal())
				checkDiskValue(t, dir, records[0], key, value)
			})
		}
	}
}

func TestPutReplacementReleasesMemory(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	putValue(t, c, "key", []byte("first"))
	first := residentBuffer(t, c, []byte("key"))
	putValue(t, c, "key", []byte("second"))
	require.False(t, first.TryInc())
	require.Equal(t, []byte("second"), retainedValue(t, c, []byte("key")))
	second := residentBuffer(t, c, []byte("key"))
	putValue(t, c, "key", make([]byte, 40000))
	require.Equal(t, 1, c.values.entries.Len(), "object metadata stays indexed")
	require.Zero(t, residentCount(c), "object replacement invalidates the inline buffer")
	require.False(t, second.TryInc())
	putValue(t, c, "key", []byte("inline again"))
	require.Equal(t, 1, residentCount(c))
	require.Equal(t, []byte("inline again"), retainedValue(t, c, []byte("key")))
}

func TestPutMemoryPressure(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	for i := range 100 {
		putValue(t, c, fmt.Sprint(i), []byte("value"))
	}
	require.Equal(t, 100, c.values.entries.Len())
	w, err := c.NewWriter([]byte("stream"), SizeUnknown)
	require.NoError(t, err, "staging reclaims from the same memory budget")
	require.Equal(t, 100, c.values.entries.Len())
	require.NoError(t, w.Close())

	var held []*Writer
	for {
		w, err := c.NewWriter([]byte("held"), 1)
		if errors.Is(err, ErrBusy) {
			break
		}
		require.NoError(t, err)
		held = append(held, w)
	}
	require.Len(t, held, 32)
	require.Zero(t, residentCount(c))
	require.Equal(t, 100, c.values.entries.Len(), "all disk metadata survives pressure")
	_, err = c.NewWriter([]byte("stream"), SizeUnknown)
	require.ErrorIs(t, err, ErrBusy)
	for _, w := range held {
		require.NoError(t, w.Close())
	}
	putValue(t, c, "available again", []byte("value"))
}

func TestPutCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir)
	key, value := []byte("committed"), bytes.Repeat([]byte("v"), 5000)
	w, err := c.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	_, err = w.Write(value)
	require.NoError(t, err)
	require.NoError(t, c.Put(w))
	buffer := residentBuffer(t, c, key)
	var abandoned []*Writer
	for _, size := range []int64{10000, SizeUnknown} {
		w, err := c.NewWriter([]byte("abandoned"), size)
		require.NoError(t, err)
		_, err = w.Write(make([]byte, 9000))
		require.NoError(t, err)
		abandoned = append(abandoned, w)
	}
	// Writer lifetime belongs to the caller, including abandoned downloads.
	for _, w := range append(abandoned, w) {
		require.NoError(t, w.Close())
	}
	require.Equal(t, value, retainedValue(t, c, key), "the cache reference survives writer Close")
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	require.False(t, buffer.TryInc(), "both writer and cache references released")
	objects, err := os.ReadDir(filepath.Join(dir, "objects"))
	require.NoError(t, err)
	require.Empty(t, objects)
	records := diskRecords(t, dir)
	require.Len(t, records, 1)
	checkDiskValue(t, dir, records[0], key, value)

	c = newPutCache(t, dir)
	putValue(t, c, "next", []byte("next value"))
	require.NoError(t, c.Close())
	reopened := diskRecords(t, dir)
	require.Len(t, reopened, 2)
	require.Equal(t, records[0], reopened[0])
	require.Greater(t, reopened[1].Offset, reopened[0].Offset)
	checkDiskValue(t, dir, reopened[0], key, value)
	checkDiskValue(t, dir, reopened[1], []byte("next"), []byte("next value"))
}

func TestPutEvictionKeepsWriterMemoryPinned(t *testing.T) {
	c := newPutCache(t, t.TempDir(), WithInlineThreshold(128<<10))
	key := []byte("key")
	value := bytes.Repeat([]byte("v"), 128<<10-blobstore.RecordHeaderSize-len(key))
	w, err := c.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write(value)
	require.NoError(t, err)
	require.NoError(t, c.Put(w))
	buffer := w.RetainBuffer()
	require.NotNil(t, buffer)
	buffer.Unpin() // Inspect lifetime through the writer's existing reference.
	_, err = c.NewWriter([]byte("next"), 1)
	require.ErrorIs(t, err, ErrBusy)
	require.Zero(t, residentCount(c), "eviction releases only the cache reference")
	require.Equal(t, 1, c.values.entries.Len(), "metadata remains indexed")
	require.True(t, buffer.TryInc(), "the writer still pins its allocation")
	buffer.Unpin()
	require.NoError(t, w.Close())
	require.False(t, buffer.TryInc())
	putValue(t, c, "next", []byte("v"))
}

type putErrorReader struct{ err error }

func (r putErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestPutSourceErrorPreservesValue(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	putValue(t, c, "key", []byte("value"))
	w, err := c.NewWriter([]byte("key"), 100)
	require.NoError(t, err)
	defer w.Close()
	failure := errors.New("download failed")
	_, err = w.ReadFrom(io.MultiReader(bytes.NewReader([]byte("partial")), putErrorReader{failure}))
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, c.Put(w), failure)
	require.ErrorIs(t, w.Close(), failure)
	require.Equal(t, []byte("value"), retainedValue(t, c, []byte("key")))
}

func TestPutOpenErrorLeavesCacheUnchanged(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir)
	putValue(t, c, "key", []byte("previous"))
	// An asynchronous object open can fail after NewWriter has returned.
	require.NoError(t, os.Remove(filepath.Join(dir, "objects")))
	for _, key := range []string{"key", "missing"} {
		w, err := c.NewWriter([]byte(key), SizeUnknown)
		require.NoError(t, err)
		_, err = w.Write([]byte("replacement"))
		require.NoError(t, err)
		err = c.Put(w)
		require.Error(t, err)
		require.ErrorIs(t, w.Close(), err)
	}
	checkRead(t, c, []byte("key"), []byte("previous"))
	r, err := c.Read([]byte("missing"))
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestPutConcurrent(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir, WithMemory(4<<20))
	const workers, iterations = 16, 10
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for iteration := range iterations {
				key := []byte(fmt.Sprintf("key-%d-%d", worker, iteration))
				if iteration%2 == 0 {
					key = []byte("shared")
				}
				w, err := c.NewWriter(key, 5000)
				if err != nil {
					errs <- err
					return
				}
				_, err = w.Write(bytes.Repeat([]byte{byte(worker)}, 5000))
				if err == nil {
					err = c.Put(w)
				}
				err = errors.Join(err, w.Close())
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1+workers*iterations/2, c.values.entries.Len())
	require.NoError(t, c.Close())
	require.Len(t, diskRecords(t, dir), workers*iterations)
}

func TestPutConfigurationAndOpenFailures(t *testing.T) {
	for _, option := range []Option{
		WithMemory(-1), WithMemory(0), WithMemory(128<<10 - 1), WithMemory(128<<10 + 1),
		WithInlineThreshold(-1),
		WithStagingSize(0), WithStagingSize(-1),
		WithIOBudget(-1),
	} {
		c, err := New(t.TempDir(), WithMemory(128<<10), option)
		require.Error(t, err)
		require.Nil(t, c)
	}
	for _, entry := range []string{"objects", "data.db", "index.db"} {
		dir := t.TempDir()
		path := filepath.Join(dir, entry)
		if entry == "objects" {
			require.NoError(t, os.WriteFile(path, nil, 0o600))
		} else {
			require.NoError(t, os.Mkdir(path, 0o700))
		}
		c, err := New(dir, WithMemory(128<<10))
		require.Error(t, err)
		require.Nil(t, c)
		require.NoError(t, os.Remove(path))
		c = newPutCache(t, dir)
		putValue(t, c, "key", []byte("value"))
	}
	c := newPutCache(t, t.TempDir())
	for _, key := range [][]byte{nil, make([]byte, MaxKeyLen+1)} {
		w, err := c.NewWriter(key, 0)
		require.Error(t, err)
		require.Nil(t, w)
	}
	w, err := c.NewWriter([]byte("key"), -2)
	require.Error(t, err)
	require.Nil(t, w)

	// The default staging size must work at the minimum memory budget.
	c, err = New(t.TempDir(), WithMemory(128<<10))
	require.NoError(t, err)
	defer c.Close()
	w, err = c.NewWriter([]byte("key"), SizeUnknown)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write(make([]byte, 300000))
	require.NoError(t, err)
	require.NoError(t, c.Put(w))
}

func TestMemoryQueueSecondChance(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	var keys []string
	for i := 0; len(keys) < 3; i++ {
		key := fmt.Sprint(i)
		if blobstore.HashKey([]byte(key)).Lo%uint64(c.values.entries.ShardCount()) == 0 {
			keys = append(keys, key)
			putValue(t, c, key, []byte(key))
		}
	}
	first := residentBuffer(t, c, []byte(keys[0]))
	second := residentBuffer(t, c, []byte(keys[1]))
	s := c.values.entries.Shard(blobstore.HashKey([]byte(keys[0])))
	s.Lock()
	s.Extra.list.Value(s.Items[blobstore.HashKey([]byte(keys[0]))].resident).visited = 1
	s.Unlock()
	require.Equal(t, int64(align.BlockSize), c.values.reclaimCacheMemory(align.BlockSize))
	require.False(t, second.TryInc())
	require.Equal(t, []byte(keys[0]), retainedValue(t, c, []byte(keys[0])))
	// Replace the next eviction candidate and verify the hand still progresses.
	putValue(t, c, keys[2], []byte("replacement"))
	c.values.reclaimCacheMemory(c.memory.Capacity())
	require.False(t, first.TryInc())
	require.Equal(t, 3, c.values.entries.Len())
	for i := range c.values.entries.ShardCount() {
		s := c.values.entries.ShardAt(i)
		require.Zero(t, s.Extra.list.Len())
		require.Zero(t, s.Extra.hand)
		for _, e := range s.Items {
			require.Zero(t, e.resident)
		}
	}
}
