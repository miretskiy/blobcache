package blobcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDiskReclaimKeepsResidentValues(t *testing.T) {
	const target = 64 << 10
	dir := t.TempDir()
	c := newPutCache(t, dir, WithTargetSize(target))
	value := bytes.Repeat([]byte("v"), target/4)
	for _, key := range []string{"a", "b", "c", "d"} {
		putValue(t, c, key, value)
	}
	require.Less(t, c.store.Size(), uint64(target))
	require.Equal(t, value, retainedValue(t, c, []byte("a")), "rotation leaves memory references intact")
	putValue(t, c, "e", []byte("replacement"))
	require.Equal(t, value, retainedValue(t, c, []byte("a")), "overwriting disk does not change the retained buffer")
	require.Equal(t, []byte("replacement"), retainedValue(t, c, []byte("e")))
	c.values.reclaimCacheMemory(c.memory.Capacity())
	r, err := c.Read([]byte("a"))
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
	_, _, ok := c.values.lookup(blobstore.HashKey([]byte("a")))
	require.False(t, ok, "retired metadata leaves with its resident buffer")
	require.NoError(t, c.Close())
	info, err := os.Stat(filepath.Join(dir, "data.db"))
	require.NoError(t, err)
	require.EqualValues(t, 3*align.PageAlign(blobstore.RecordHeaderSize+1+int64(len(value))), info.Size())
	for _, h := range diskRecords(t, dir) {
		if h.Flags.IsDeleted() {
			require.Equal(t, blobstore.RecordHeader{Flags: blobstore.FlagDeleted}, h)
		}
	}
}

func TestDiskRetirementRemovesMetadataAndPreservesReplacement(t *testing.T) {
	const target = 64 << 10
	dir := t.TempDir()
	opts := []Option{WithInlineThreshold(0), WithTargetSize(target)}
	c := newPutCache(t, dir, opts...)
	putValue(t, c, "key", []byte("old"))
	putValue(t, c, "key", []byte("replacement"))
	require.NoError(t, c.Close())
	c = newPutCache(t, dir, opts...) // Recovered index entries have no pending I/O.
	// Each put leaves the payload over target by its own size; the next
	// reservation evicts that much from the oldest records.
	evictNext := func() {
		w, err := c.NewWriter([]byte("trigger"))
		require.NoError(t, err)
		require.NoError(t, c.Put(w))
		require.NoError(t, w.Close())
	}
	putValue(t, c, "filler", make([]byte, target-len("replacement")))
	evictNext()
	checkRead(t, c, []byte("key"), []byte("replacement"))
	putValue(t, c, "more", []byte("replacement"))
	evictNext()
	_, _, ok := c.values.lookup(blobstore.HashKey([]byte("key")))
	require.False(t, ok, "retirement removes metadata before any read")
	r, err := c.Read([]byte("key"))
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestPutConcurrentDiskReclamation(t *testing.T) {
	const target = 256 << 10
	dir := t.TempDir()
	c := newPutCache(t, dir, WithMemory(minMemory), WithTargetSize(target), WithInlineThreshold(0))
	group, ctx := errgroup.WithContext(context.Background())
	for worker := range 4 {
		group.Go(func() error {
			value := bytes.Repeat([]byte("v"), 8<<10)
			for i := range 20 {
				if err := ctx.Err(); err != nil {
					return err
				}
				key := []byte(fmt.Sprintf("%d-%d", worker, i))
				w, err := c.NewWriter(key)
				if err != nil {
					return err
				}
				_, err = w.Write(value)
				if err == nil {
					err = c.Put(w)
				}
				if err = errors.Join(err, w.Close()); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	require.NoError(t, c.Close())
	objects, err := os.ReadDir(filepath.Join(dir, "objects"))
	require.NoError(t, err)
	require.NotEmpty(t, objects)
}
