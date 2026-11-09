package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func newPutCache(t *testing.T, dir string, opts ...Option) *Cache {
	t.Helper()
	c, err := New(dir, append([]Option{
		WithMemory(minMemory), WithInlineThreshold(32 << 10), WithStagingSize(8 << 10),
		storeOpt(blobstore.TestingWithDirectIO(false)),
		storeOpt(blobstore.WithDataIntegrity(blobstore.DataIntegrityCRC32C)),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func putValue(t *testing.T, c *Cache, key string, value []byte) {
	t.Helper()
	w, err := c.NewWriter([]byte(key))
	require.NoError(t, err)
	defer w.Close()
	n, err := w.Write(value)
	require.NoError(t, err)
	require.Equal(t, len(value), n)
	err = c.Put(w)
	require.NoError(t, err)
	// Callers of this setup helper inspect persisted records or force eviction.
	// Put itself only guarantees immediate inline visibility.
	hash := blobstore.HashKey([]byte(key))
	require.Eventually(t, func() bool {
		s := c.values.entries.Shard(hash)
		s.RLock()
		defer s.RUnlock()
		e, ok := s.Items[hash]
		return !ok || e.resident == 0 || !s.Extra.list.Value(e.resident).pending
	}, 5*time.Second, time.Millisecond)
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
	for chunk := range s.Extra.list.Value(e.resident).buffer.Chunks() {
		data = append(data, chunk...)
	}
	start := blobstore.RecordHeaderSize + int(e.header.KeyLen)
	require.Equal(t, key, data[blobstore.RecordHeaderSize:start])
	return data[start : start+int(e.header.PhysicalLen)]
}

// Return the buffer object for sequential lifetime assertions, without taking
// another reference that would mask an erroneous retain or release.
func residentBuffer(t *testing.T, c *Cache, key []byte) blobstore.Memory {
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
	require.Equal(t, blobstore.HashKey(key), h.KeyHash(), "index hash identifies the key")
	if h.Flags&blobstore.FlagFooter != 0 {
		prefix := h
		prefix.PhysicalLen, prefix.LogicalLen, prefix.Hash = 0, 0, 0
		require.Equal(t, prefix, actual)
		var footer blobstore.RawHeader
		_, err = f.ReadAt(footer[:], int64(len(data)))
		require.NoError(t, err)
		require.NoError(t, footer.Decode(&actual, true))
	}
	h.Hash = actual.Hash
	require.Equal(t, h, actual)
	start := blobstore.RecordHeaderSize + len(key)
	require.Equal(t, key, data[blobstore.RecordHeaderSize:start])
	require.Equal(t, value, data[start:start+len(value)])
	require.Equal(t, make([]byte, len(data)-start-len(value)), data[start+len(value):])
	require.Equal(t, uint64(len(value)), h.PhysicalLen)
	require.Equal(t, h.PhysicalLen, h.LogicalLen)
	require.Equal(t, uint64(crc32.Checksum(append(bytes.Clone(key), value...), crc32.MakeTable(crc32.Castagnoli))), h.Hash)
}

func TestPutPersistsAndRetains(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			keyLen   int
			size     int
			external bool
		}{
			{"empty", 3, 0, false},
			{"exact page", 3, align.BlockSize - blobstore.RecordHeaderSize - 3, false},
			{"inline", 100, 20000, false},
			{"max key", MaxKeyLen, 13, false},
			{"object", MaxKeyLen, 300001, true},
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
				w, err := c.NewWriter(inputKey)
				require.NoError(t, err)
				defer w.Close()
				clear(inputKey)
				split := len(value) / 2
				_, err = w.Write(value[:split])
				require.NoError(t, err)
				n, err := w.ReadFrom(bytes.NewReader(value[split:]))
				require.NoError(t, err)
				require.Equal(t, int64(len(value)-split), n)
				err = c.Put(w)
				require.NoError(t, err)

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
	var w *Writer
	require.Eventually(t, func() bool {
		var err error
		w, err = c.NewWriter([]byte("stream"))
		return err == nil
	}, 5*time.Second, time.Millisecond, "staging reclaims from the same memory budget")
	require.Equal(t, 100, c.values.entries.Len())
	require.NoError(t, w.Close())

	var held []*Writer
	for {
		w, err := c.NewWriter([]byte("held"))
		if errors.Is(err, ErrBusy) {
			if residentCount(c) == 0 {
				break
			}
			time.Sleep(time.Millisecond) // Reclamation runs in the background.
			continue
		}
		require.NoError(t, err)
		held = append(held, w)
	}
	require.Zero(t, residentCount(c))
	require.Equal(t, 100, c.values.entries.Len(), "all disk metadata survives pressure")
	_, err := c.NewWriter([]byte("stream"))
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
	w, err := c.NewWriter(key)
	require.NoError(t, err)
	_, err = w.Write(value)
	require.NoError(t, err)
	err = c.Put(w)
	require.NoError(t, err)

	buffer := residentBuffer(t, c, key)
	var abandoned []*Writer
	for range 2 {
		w, err := c.NewWriter([]byte("abandoned"))
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

type putErrorReader struct{ err error }

func (r putErrorReader) Read([]byte) (int, error) { return 0, r.err }

// A source error prevents preparation, leaving the previous value readable.
func TestPutSourceErrorKeepsValue(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	putValue(t, c, "key", []byte("value"))
	w, err := c.NewWriter([]byte("key"))
	require.NoError(t, err)
	defer w.Close()
	failure := errors.New("download failed")
	_, err = w.ReadFrom(io.MultiReader(bytes.NewReader([]byte("partial")), putErrorReader{failure}))
	require.ErrorIs(t, err, failure)
	err = c.Put(w)
	require.ErrorIs(t, err, failure)
	require.NoError(t, w.Close())
	checkRead(t, c, []byte("key"), []byte("value"))
}

func TestPutOpenErrorLeavesCacheUnchanged(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir)
	putValue(t, c, "key", []byte("previous"))
	// An asynchronous object open can fail after NewWriter has returned.
	require.NoError(t, os.Remove(filepath.Join(dir, "objects")))
	for _, key := range []string{"key", "missing"} {
		w, err := c.NewWriter([]byte(key))
		require.NoError(t, err)
		_, err = w.Write(make([]byte, 40000))
		if err == nil {
			err = c.Put(w)
		}
		require.Error(t, err, "streaming or Put reports the object-open failure")
		require.NoError(t, w.Close())
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
				w, err := c.NewWriter(key)
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
		WithMemory(-1), WithMemory(0), WithMemory(minMemory - 1),
		WithInlineThreshold(-1),
		WithStagingSize(0), WithStagingSize(-1),
	} {
		c, err := New(t.TempDir(), WithMemory(minMemory), option)
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
		c, err := New(dir, WithMemory(minMemory))
		require.Error(t, err)
		require.Nil(t, c)
		require.NoError(t, os.Remove(path))
		c = newPutCache(t, dir)
		putValue(t, c, "key", []byte("value"))
	}
	c := newPutCache(t, t.TempDir())
	for _, key := range [][]byte{nil, make([]byte, MaxKeyLen+1)} {
		w, err := c.NewWriter(key)
		require.Error(t, err)
		require.Nil(t, w)
	}
	// The default staging size must work at the minimum memory budget.
	c, err := New(t.TempDir(), WithMemory(minMemory))
	require.NoError(t, err)
	defer c.Close()
	w, err := c.NewWriter([]byte("key"))
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write(make([]byte, 300000))
	require.NoError(t, err)
	err = c.Put(w)
	require.NoError(t, err)

}

func TestMemoryQueueSecondChance(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	var keys []string
	for i := 0; len(keys) < 3; i++ {
		key := fmt.Sprint(i)
		if blobstore.HashKey([]byte(key))%uint64(c.values.entries.ShardCount()) == 0 {
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
	require.Equal(t, second.Size(), c.values.reclaimCacheMemory(align.BlockSize))
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
