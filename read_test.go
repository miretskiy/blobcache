package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

// TestReadKeyIdentity maps other keys to a record's index entry. Resident reads
// compare the full key; disk reads trust the 64-bit hash but check the length.
func TestReadKeyIdentity(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	key, value := bytes.Repeat([]byte("k"), MaxKeyLen), []byte("value")
	putValue(t, c, string(key), value)
	sameLength := bytes.Clone(key)
	sameLength[len(sameLength)-1] ^= 1
	buffer, h, ok := c.values.lookup(blobstore.HashKey(key))
	require.True(t, ok)
	c.values.replace(blobstore.HashKey(sameLength), buffer, h, false) // Transfers lookup's pin.
	requireMiss(t, c, sameLength)

	c.values.reclaimCacheMemory(c.memory.Capacity())
	require.Zero(t, residentCount(c))
	shorter := key[1:]
	c.values.replace(blobstore.HashKey(shorter), nil, h, false)
	requireMiss(t, c, shorter)
	checkRead(t, c, key, value)
}

// TestResidentKeyFitsFirstAllocation fragments the allocator so a frame spans
// many allocations; the header and a maximum-length key still share the first.
func TestResidentKeyFitsFirstAllocation(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	var held []alloc.Memory
	for {
		m, err := c.memory.TryAcquire(align.BlockSize)
		if err != nil {
			require.ErrorIs(t, err, alloc.ErrExhausted)
			break
		}
		held = append(held, m)
	}
	for i := 0; i < len(held); i += 2 {
		held[i].Unpin()
	}
	defer func() {
		for i := 1; i < len(held); i += 2 {
			held[i].Unpin()
		}
	}()
	key, value := bytes.Repeat([]byte("k"), MaxKeyLen), []byte("value")
	putValue(t, c, string(key), value)
	require.Len(t, residentBuffer(t, c, key).First(), align.BlockSize)
	checkRead(t, c, key, value)
}

func TestMemoryReaderRejectsIncompleteFrame(t *testing.T) {
	pool := newTestPool(t, 128<<10)
	header := blobstore.RecordHeader{KeyLen: MaxKeyLen, PhysicalLen: 1}
	r, err := newMemoryReader(bytes.Repeat([]byte("k"), MaxKeyLen), header, acquire(t, pool, align.BlockSize), 0, 1)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrCorrupt)
	require.Equal(t, pool.Capacity(), pool.Available(), "the reader consumes its buffer on error")
}

func TestReadPinsUntilClose(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	value := bytes.Repeat([]byte("original"), 1000)
	putValue(t, c, "key", value)
	charged := used(c)
	a, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer a.Close()
	b, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer b.Close()
	require.Zero(t, c.values.reclaimCacheMemory(charged), "readers keep demoted buffers pinned")
	require.Zero(t, residentCount(c))
	require.Equal(t, charged, used(c))
	putValue(t, c, "key", []byte("replacement"))
	checkRead(t, c, []byte("key"), []byte("replacement"))
	for _, r := range []Reader{a, b} {
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, value, got, "open readers survive replacement and eviction")
	}
	before := used(c)
	require.NoError(t, a.Close())
	require.Equal(t, before, used(c), "EOF and closing one reader do not release the other pin")
	require.NoError(t, b.Close())
	require.Equal(t, before-charged, used(c))
	require.NoError(t, b.Close())
}

func TestReadErrors(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	for _, tc := range []struct {
		key []byte
		err error
	}{
		{nil, ErrEmptyKey},
		{bytes.Repeat([]byte("k"), MaxKeyLen+1), ErrKeyTooLarge},
		{[]byte("absent"), ErrNotFound},
	} {
		r, err := c.Read(tc.key)
		require.Nil(t, r)
		require.ErrorIs(t, err, tc.err)
	}
	_, err := c.Stat([]byte("absent"))
	require.ErrorIs(t, err, ErrNotFound)
}

// TestDiskReaderReleasesMemory closes disk readers early, after EOF, and after
// a consumer error; the I/O's own reference outlives an early Close.
func TestDiskReaderReleasesMemory(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	value := bytes.Repeat([]byte("v"), 12000)
	putValue(t, c, "key", value)
	c.values.reclaimCacheMemory(c.memory.Capacity())
	require.Eventually(t, func() bool { return used(c) == 0 }, time.Second, time.Millisecond)

	r, err := c.Read([]byte("key"))
	require.NoError(t, err)
	require.Positive(t, used(c), "the read buffer is charged at submission")
	require.NoError(t, r.Close())
	require.Eventually(t, func() bool { return used(c) == 0 }, time.Second, time.Millisecond,
		"completion releases the buffer after an early Close")

	r, err = c.Read([]byte("key"))
	require.NoError(t, err)
	n, err := io.Copy(io.Discard, r)
	require.NoError(t, err)
	require.EqualValues(t, len(value), n)
	require.Positive(t, used(c), "EOF keeps the window until Close")
	require.NoError(t, r.Close())
	require.NoError(t, r.Close(), "double Close must not release twice")
	require.Zero(t, used(c))

	r, err = c.Read([]byte("key"))
	require.NoError(t, err)
	failed := errors.New("consumer failed")
	_, err = r.WriteTo(failingWriter{failed})
	require.ErrorIs(t, errors.Join(err, r.Close()), failed)
	require.Zero(t, used(c))
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

// TestDiskReadMethods consumes disk ranges of one and of several read windows
// through each method, and checks that closing returns the disk-read gauges.
func TestDiskReadMethods(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20), WithInlineThreshold(0), WithReadWindow(64<<10))
	for _, size := range []int{12000, 300001} {
		key, value := []byte(fmt.Sprint(size)), bytes.Repeat([]byte("v"), size)
		putValue(t, c, string(key), value)
		for _, method := range []string{"Next", "Read", "WriteTo"} {
			readBytes, readers := metrics.DiskReadBytes.Load(), metrics.DiskReaders.Load()
			r, err := c.Read(key)
			require.NoError(t, err)
			var got []byte
			switch method {
			case "Next":
				for {
					chunk, err := r.Next()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					got = append(got, chunk...)
				}
			case "Read":
				got, err = io.ReadAll(r)
				require.NoError(t, err)
			case "WriteTo":
				var dst bytes.Buffer
				_, err = r.WriteTo(&dst)
				require.NoError(t, err)
				got = dst.Bytes()
			}
			require.True(t, bytes.Equal(value, got), "size %d via %s", size, method)
			require.NoError(t, r.Close())
			require.Equal(t, readBytes, metrics.DiskReadBytes.Load())
			require.Equal(t, readers, metrics.DiskReaders.Load())
		}
	}
	require.Zero(t, residentCount(c))
}

func TestConcurrentDiskReaders(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20), WithInlineThreshold(0))
	key, value := []byte("key"), bytes.Repeat([]byte("v"), 300001)
	putValue(t, c, string(key), value)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			r, err := c.Read(key)
			if err != nil {
				errs <- err
				return
			}
			got, err := io.ReadAll(r)
			if err == nil && !bytes.Equal(value, got) {
				err = errors.New("payload mismatch")
			}
			errs <- errors.Join(err, r.Close())
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, used(c))
}

func TestReadRangeAndStat(t *testing.T) {
	c := newPutCache(t, testDevice(t, 64<<20))
	for _, v := range []struct {
		key      string
		size     int
		resident bool
	}{{"resident", 20 << 10, true}, {"disk", 2 << 20, false}} {
		value := bytes.Repeat([]byte(v.key), v.size/len(v.key))
		putValue(t, c, v.key, value)
		size, err := c.Stat([]byte(v.key))
		require.NoError(t, err)
		require.Equal(t, int64(len(value)), size)

		r, err := c.ReadRange([]byte(v.key), 4097, 10000)
		require.NoError(t, err)
		_, memory := r.(*memoryReader)
		require.Equal(t, v.resident, memory)
		require.Equal(t, int64(10000), r.Size())
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, value[4097:4097+10000], got)
		require.NoError(t, r.Close())

		_, err = c.ReadRange([]byte(v.key), size+1, 1)
		require.ErrorIs(t, err, ErrInvalidRange)
	}
}

func TestOverwrittenRecord(t *testing.T) {
	t.Run("resident", func(t *testing.T) {
		c := newPutCache(t, testDevice(t, 1<<20))
		value := bytes.Repeat([]byte("v"), 16<<10)
		putValue(t, c, "a", value)
		overwrite(t, c, "a")
		checkRead(t, c, []byte("a"), value)
		require.Equal(t, value, retainedValue(t, c, []byte("a")), "the log overwriting disk leaves memory intact")
		c.values.reclaimCacheMemory(c.memory.Capacity())
		requireMiss(t, c, []byte("a"))
		_, _, ok := c.values.lookup(blobstore.HashKey([]byte("a")))
		require.False(t, ok, "dead metadata leaves with its resident buffer")
	})
	t.Run("disk", func(t *testing.T) {
		c := newPutCache(t, testDevice(t, 1<<20), WithInlineThreshold(0))
		putValue(t, c, "key", []byte("value"))
		overwrite(t, c, "key")
		requireMiss(t, c, []byte("key"))
		_, _, ok := c.values.lookup(blobstore.HashKey([]byte("key")))
		require.False(t, ok, "a miss removes the dead entry")
	})
}
