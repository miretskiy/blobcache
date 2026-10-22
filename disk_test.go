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
	dir := t.TempDir()
	c := newPutCache(t, dir, WithInlineThreshold(0))
	putValue(t, c, "key", []byte("old"))
	putValue(t, c, "key", []byte("replacement"))
	require.NoError(t, c.Close())
	c = newPutCache(t, dir, WithInlineThreshold(0)) // Recovered index entries have no pending I/O.
	require.EqualValues(t, 3, c.store.Reclaim(3))
	checkRead(t, c, []byte("key"), []byte("replacement"))
	require.Equal(t, 1, c.values.entries.Len())
	require.EqualValues(t, len("replacement"), c.store.Reclaim(uint64(len("replacement"))))
	require.Zero(t, c.values.entries.Len(), "the retirement callback removes metadata before any read")
	r, err := c.Read([]byte("key"))
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestWriterCloseReclaimsWithoutAnotherPut(t *testing.T) {
	const target = 64 << 10
	dir := t.TempDir()
	c := newPutCache(t, dir, WithMemory(1<<20), WithTargetSize(target), WithInlineThreshold(0))
	value := bytes.Repeat([]byte("v"), 2<<10)
	// Keep the writers open so the FIFO cannot retire them. Closing them must
	// reclaim the excess even though no more Put calls arrive.
	var writers []*Writer
	for i := range 40 {
		key := []byte(fmt.Sprint(i))
		w, err := c.NewWriter(key, int64(len(value)))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, w.Close()) })
		_, err = w.Write(value)
		require.NoError(t, err)
		require.NoError(t, c.store.Put(w))
		writers = append(writers, w)
	}
	require.Greater(t, c.store.Size(), uint64(target))
	for _, w := range writers {
		require.NoError(t, w.Close())
	}
	require.LessOrEqual(t, c.store.Size(), uint64(target))
	// Drain dio cleanup before checking the completed unlinks.
	require.NoError(t, c.Close())
	objects, err := os.ReadDir(filepath.Join(dir, "objects"))
	require.NoError(t, err)
	require.NotEmpty(t, objects)
	require.LessOrEqual(t, len(objects)*len(value), target)
}

func TestPutConcurrentDiskReclamation(t *testing.T) {
	const target = 256 << 10
	dir := t.TempDir()
	c := newPutCache(t, dir, WithMemory(2<<20), WithTargetSize(target), WithInlineThreshold(0))
	group, ctx := errgroup.WithContext(context.Background())
	for worker := range 4 {
		group.Go(func() error {
			value := bytes.Repeat([]byte("v"), 8<<10)
			for i := range 20 {
				if err := ctx.Err(); err != nil {
					return err
				}
				key := []byte(fmt.Sprintf("%d-%d", worker, i))
				w, err := c.NewWriter(key, int64(len(value)))
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
	require.LessOrEqual(t, len(objects)*(8<<10), target)
}
