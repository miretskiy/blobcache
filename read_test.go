package blobcache

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

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
	require.Equal(t, value, got.Bytes())
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
				promised   int64
			}{
				{[]byte("empty"), nil, 0},
				{[]byte("inline"), bytes.Repeat([]byte("inline"), 1000), 6000},
				{[]byte("external"), bytes.Repeat([]byte("external"), 5000), 40000},
				{[]byte("unknown"), []byte("small streamed object"), SizeUnknown},
				{bytes.Repeat([]byte("k"), MaxKeyLen), []byte("long key"), SizeUnknown},
			}
			putValue(t, c, "inline", []byte("superseded"))
			for _, v := range values {
				w, err := c.NewWriter(v.key, v.promised)
				require.NoError(t, err)
				defer w.Close()
				_, err = w.Write(v.value)
				require.NoError(t, err)
				require.NoError(t, c.Put(w))
				c.store.Wait()
				require.NoError(t, w.Close())
				checkRead(t, c, v.key, v.value)
			}
			c.values.reclaimCacheMemory(c.memory.Capacity())
			require.Zero(t, residentCount(c))
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

func TestReadPinsUntilClose(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	value := bytes.Repeat([]byte("original"), 1000)
	putValue(t, c, "key", value)
	charged := c.memory.used.Load()
	a, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer a.Close()
	b, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer b.Close()
	require.Zero(t, c.values.reclaimCacheMemory(charged), "readers keep demoted buffers pinned")
	require.Zero(t, residentCount(c))
	require.Equal(t, charged, c.memory.used.Load())
	putValue(t, c, "key", []byte("replacement"))
	checkRead(t, c, []byte("key"), []byte("replacement"))
	for _, r := range []Reader{a, b} {
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, value, got, "open readers survive replacement and eviction")
	}
	before := c.memory.used.Load()
	require.NoError(t, a.Close())
	require.Equal(t, before, c.memory.used.Load(), "EOF and closing one reader do not release the other pin")
	require.NoError(t, b.Close())
	require.Equal(t, before-charged, c.memory.used.Load())
	require.NoError(t, b.Close())
}

func TestReadDiskEarlyCloseAndErrors(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	putValue(t, c, "key", bytes.Repeat([]byte("data"), 2000))
	c.values.reclaimCacheMemory(c.memory.Capacity())
	require.Zero(t, residentCount(c))
	before := c.memory.used.Load()
	r, err := c.Read([]byte("key"))
	require.NoError(t, err)
	defer r.Close()
	require.Greater(t, c.memory.used.Load(), before, "disk reads acquire staging memory after eviction")
	require.NoError(t, r.Close())
	require.Equal(t, before, c.memory.used.Load(), "early Close releases staging without draining")
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
	w, err := c.NewWriter(key, 5)
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
		require.Equal(t, hash, entry.header.Hash)
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
		{"present/default", blobstore.DataIntegrityXXHash128, false},
		{"present/verify", blobstore.DataIntegrityXXHash128, true},
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
			writeIntegrity := blobstore.DataIntegrityXXHash128
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
			if tc.verify && tc.integrity == blobstore.DataIntegrityXXHash128 {
				require.ErrorIs(t, err, ErrCorrupt)
			} else {
				require.NoError(t, err)
				require.Equal(t, value, got)
			}
		})
	}
}
