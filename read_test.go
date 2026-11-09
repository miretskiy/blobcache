package blobcache

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/stretchr/testify/require"
)

func checkRead(t *testing.T, c *Cache, key, value []byte) {
	t.Helper()
	r, err := c.Read(key)
	require.NoError(t, err)
	defer r.Close()
	require.Equal(t, int64(len(value)), r.Size())
	// Mix Read and io.Copy/WriteTo on the same reader, across chunk boundaries.
	var got bytes.Buffer
	prefix := make([]byte, min(7, len(value)))
	_, err = io.ReadFull(r, prefix)
	require.NoError(t, err)
	got.Write(prefix)
	n, err := io.Copy(&got, r)
	require.NoError(t, err)
	require.Equal(t, int64(len(value)-len(prefix)), n)
	require.True(t, bytes.Equal(value, got.Bytes()), "payload mismatch")
	_, err = r.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}

func TestReadMemoryDiskAndRecovery(t *testing.T) {
	for name, direct := range map[string]bool{"buffered": false, "direct": true} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			c := newPutCache(t, dir, storeOpt(blobstore.TestingWithDirectIO(direct)), WithVerifyChecksums(true))
			values := []struct {
				key, value []byte
			}{
				{[]byte("empty"), nil},
				{[]byte("inline"), bytes.Repeat([]byte("inline"), 1000)},
				{[]byte("external"), bytes.Repeat([]byte("external"), 5000)},
				{bytes.Repeat([]byte("k"), MaxKeyLen), []byte("long key")},
			}
			putValue(t, c, "inline", []byte("superseded"))
			for _, v := range values {
				w, err := c.NewWriter(v.key)
				require.NoError(t, err)
				defer w.Close()
				_, err = w.Write(v.value)
				require.NoError(t, err)
				err = c.Put(w)
				require.NoError(t, err)

				require.NoError(t, w.Close())
				checkRead(t, c, v.key, v.value)
			}
			require.Eventually(t, func() bool {
				c.values.reclaimCacheMemory(c.memory.Capacity())
				return residentCount(c) == 0
			}, 5*time.Second, time.Millisecond)
			for _, v := range values {
				checkRead(t, c, v.key, v.value)
			}
			require.NoError(t, c.Close())
			c = newPutCache(t, dir, storeOpt(blobstore.TestingWithDirectIO(direct)), WithVerifyChecksums(true))
			require.Equal(t, len(values), c.values.entries.Len(), "recovery restores lookup and replaces older keys")
			for _, v := range values {
				checkRead(t, c, v.key, v.value)
			}
		})
	}
}

func TestRead64BitKeyHash(t *testing.T) {
	for _, external := range []bool{false, true} {
		name := "inline"
		if external {
			name = "external"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := []Option{storeOpt(blobstore.WithDataIntegrity(blobstore.DataIntegrityNone))}
			c := newPutCache(t, dir, opts...)
			key := bytes.Repeat([]byte("k"), 10000) // Exceeds the disk reader's staging size.
			value := []byte("value")
			if external {
				value = bytes.Repeat(value, 8000)
			}
			putValue(t, c, string(key), value)
			require.NoError(t, c.Close())

			c = newPutCache(t, dir, opts...)
			checkRead(t, c, key, value)
			entry, ok := c.values.entries.Get(blobstore.HashKey(key))
			require.True(t, ok, "persisted key hash recovers without reading the data files")
			require.Zero(t, entry.resident, "read the actual disk record")

			// Force a hash collision, with the mismatch beyond the first read.
			wrong := bytes.Clone(key)
			wrong[len(wrong)-1] ^= 1
			c.values.replace(blobstore.HashKey(wrong), blobstore.Memory{}, entry.header, false)
			r, err := c.Read(wrong)
			require.Nil(t, r)
			require.ErrorIs(t, err, ErrNotFound, "full keys must match even without integrity checks")
			checkRead(t, c, key, value)
		})
	}
}

func TestReadPinsUntilClose(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	value := bytes.Repeat([]byte("original"), 1000)
	putValue(t, c, "key", value)
	charged := (c.memory.Capacity() - c.memory.Available())
	a, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer a.Close()
	b, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer b.Close()
	require.Zero(t, c.values.reclaimCacheMemory(charged), "readers keep demoted buffers pinned")
	require.Zero(t, residentCount(c))
	require.Equal(t, charged, (c.memory.Capacity() - c.memory.Available()))
	putValue(t, c, "key", []byte("replacement"))
	checkRead(t, c, []byte("key"), []byte("replacement"))
	for _, r := range []Reader{a, b} {
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, value, got, "open readers survive replacement and eviction")
	}
	before := (c.memory.Capacity() - c.memory.Available())
	require.NoError(t, a.Close())
	require.Equal(t, before, (c.memory.Capacity() - c.memory.Available()), "EOF and closing one reader do not release the other pin")
	require.NoError(t, b.Close())
	require.Equal(t, before-charged, (c.memory.Capacity() - c.memory.Available()))
	require.NoError(t, b.Close())
}

func TestReadDiskEarlyCloseAndErrors(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	putValue(t, c, "key", bytes.Repeat([]byte("data"), 2000))
	c.values.reclaimCacheMemory(c.memory.Capacity())
	require.Zero(t, residentCount(c))
	before := (c.memory.Capacity() - c.memory.Available())
	r, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer r.Close()
	require.Greater(t, (c.memory.Capacity() - c.memory.Available()), before, "disk reads acquire staging memory after eviction")
	require.NoError(t, r.Close())
	require.Equal(t, before, (c.memory.Capacity() - c.memory.Available()), "early Close releases staging without draining")
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
	require.NoError(t, c.Close())
}

func TestUnpublishedObjectsAreMisses(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir, WithInlineThreshold(0))
	key := []byte("key")
	w, err := c.NewWriter(key)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	r, err := c.Read(key)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, c.Close())
	c = newPutCache(t, dir)
	r, err = c.Read(key)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestRecoveryUsesIndexWithoutDataFiles(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir)
	putValue(t, c, "inline", []byte("value"))
	putValue(t, c, "object", bytes.Repeat([]byte("v"), 40000))
	require.NoError(t, c.Close())
	objects, err := os.ReadDir(filepath.Join(dir, "objects"))
	require.NoError(t, err)
	require.Len(t, objects, 1)
	require.NoError(t, os.Remove(filepath.Join(dir, "objects", objects[0].Name())))
	require.NoError(t, os.Truncate(filepath.Join(dir, "data.db"), 0))
	c = newPutCache(t, dir, WithVerifyChecksums(true))
	require.Equal(t, 2, c.values.entries.Len(), "both keys recover from the index without their data")
	for _, key := range []string{"inline", "object"} {
		hash := blobstore.HashKey([]byte(key))
		entry, ok := c.values.entries.Get(hash)
		require.True(t, ok)
		require.Equal(t, hash, entry.header.KeyHash())
		require.Zero(t, entry.resident)
	}
	r, err := c.Read([]byte("object"))
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, 1, c.values.entries.Len(), "the missing object is removed lazily on Read")
}

func TestReadChecksumPolicyAfterRestart(t *testing.T) {
	for _, tc := range []struct {
		name      string
		integrity blobstore.DataIntegrity
		verify    bool
	}{
		{"absent/default", blobstore.DataIntegrityNone, false},
		{"absent/verify", blobstore.DataIntegrityNone, true},
		{"present/default", blobstore.DataIntegrityCRC32C, false},
		{"present/verify", blobstore.DataIntegrityCRC32C, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			c := newPutCache(t, dir, storeOpt(blobstore.WithDataIntegrity(tc.integrity)))
			key, value := []byte("key"), bytes.Repeat([]byte("v"), 10000)
			putValue(t, c, string(key), value)
			require.NoError(t, c.Close())
			f, err := os.OpenFile(filepath.Join(dir, "data.db"), os.O_WRONLY, 0)
			require.NoError(t, err)
			value[len(value)-1] ^= 1
			_, err = f.WriteAt(value[len(value)-1:], int64(blobstore.RecordHeaderSize+len(key)+len(value)-1))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			// Reverse the write policy at restart: verification must use the
			// existing record's flags, independently of new writes' configuration.
			writeIntegrity := blobstore.DataIntegrityCRC32C
			if tc.integrity == writeIntegrity {
				writeIntegrity = blobstore.DataIntegrityNone
			}
			opts := []Option{storeOpt(blobstore.WithDataIntegrity(writeIntegrity))}
			if tc.verify {
				opts = append(opts, WithVerifyChecksums(true))
			}
			c = newPutCache(t, dir, opts...)
			r, err := c.Read(key)
			require.NoError(t, err)
			defer r.Close()
			got, err := io.ReadAll(r)
			if tc.verify && tc.integrity == blobstore.DataIntegrityCRC32C {
				require.ErrorIs(t, err, ErrCorrupt)
			} else {
				require.NoError(t, err)
				require.Equal(t, value, got)
			}
		})
	}
}

func TestReaderCloseReleasesAllocation(t *testing.T) {
	for _, kind := range []string{"resident", "inline-disk", "external"} {
		t.Run(kind, func(t *testing.T) {
			var opts []Option
			if kind == "external" {
				opts = append(opts, WithInlineThreshold(0))
			}
			c := newPutCache(t, t.TempDir(), opts...)
			value := bytes.Repeat([]byte("v"), 12000)
			putValue(t, c, "key", value)
			if kind == "inline-disk" {
				c.values.reclaimCacheMemory(c.memory.Capacity())
			}
			r, err := c.Read([]byte("key"))
			require.NoError(t, err)
			defer r.Close()
			var charged int64
			if resident, ok := r.(*memoryReader); ok {
				charged = resident.memory.Size()
			} else {
				charged = (c.memory.Capacity() - c.memory.Available())
			}
			if kind == "resident" {
				require.Zero(t, c.values.reclaimCacheMemory(c.memory.Capacity()),
					"an unread reader keeps the evicted buffer pinned")
			}
			before := (c.memory.Capacity() - c.memory.Available())
			n, readErr := io.Copy(io.Discard, r)
			require.NoError(t, readErr)
			require.EqualValues(t, len(value), n)
			require.Equal(t, before, (c.memory.Capacity() - c.memory.Available()), "EOF currently retains the allocation")
			require.NoError(t, errors.Join(readErr, r.Close()))
			require.Equal(t, before-charged, (c.memory.Capacity() - c.memory.Available()))
			require.NoError(t, r.Close(), "double Close must not decrement twice")

			// A consumer error still evaluates Close in errors.Join.
			r, err = c.Read([]byte("key"))
			require.NoError(t, err)
			failed := errors.New("consumer failed")
			_, readErr = r.WriteTo(failingReadConsumer{failed})
			require.ErrorIs(t, errors.Join(readErr, r.Close()), failed)
			require.Zero(t, (c.memory.Capacity() - c.memory.Available()))
		})
	}
}

type failingReadConsumer struct{ err error }

func (w failingReadConsumer) Write([]byte) (int, error) { return 0, w.err }
