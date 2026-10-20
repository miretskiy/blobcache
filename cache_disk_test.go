package blobcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDiskReclaimPolicyTargets(t *testing.T) {
	const gib = uint64(1 << 30)
	const target = 1024 * gib
	p := diskReclaimPolicy{headroom: target / 20}
	trigger := target - p.headroom
	require.Zero(t, p.reclaimBytes(trigger-1, target))
	require.Equal(t, target/20, p.reclaimBytes(trigger, target), "start with a 5%% reserve")

	// These bytes arrived over the entire deletion, irrespective of how long
	// it took. Leave that space plus 25%, and reclaim a further batch at it.
	p.observe(2*gib, target)
	require.Equal(t, 5*gib/2, p.headroom)
	trigger = target - p.headroom
	require.Equal(t, p.headroom+100, p.reclaimBytes(trigger+100, target))

	p.observe(target/10, target)
	require.Greater(t, p.headroom, target/20, "the reserve is not limited to one batch")
	require.Equal(t, target/20, p.reclaimBytes(target-p.headroom, target), "each request stays bounded")
	p.observe(0, target)
	require.EqualValues(t, 1<<20, p.headroom, "quiescent passes discard the old traffic estimate")
	p.observe(2*target, target)
	require.Equal(t, target, p.headroom)
	require.Zero(t, p.reclaimBytes(0, target), "an empty cache cannot spin at a zero trigger")
	require.Equal(t, target/20, p.reclaimBytes(2*target, target))
}

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
		require.False(t, h.Flags.IsDeleted(), "eviction flags are only in memory")
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

func TestDiskReclaimContinuesAfterWritesStop(t *testing.T) {
	const target = 64 << 10
	dir := t.TempDir()
	c := newPutCache(t, dir, WithTargetSize(target), WithInlineThreshold(0))
	value := bytes.Repeat([]byte("v"), 2<<10)
	// Publish directly, then signal after writes stop. Its excess requires
	// multiple bounded reclamation passes without another Put.
	for i := range 40 {
		key := []byte(fmt.Sprint(i))
		w, err := c.NewWriter(key, int64(len(value)))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, w.Close()) })
		_, err = w.Write(value)
		require.NoError(t, err)
		require.NoError(t, c.store.Put(w, nil))
		require.NoError(t, w.Close())
	}
	c.store.Wait()
	c.startDiskReclaim()
	require.Eventually(t, func() bool {
		return c.store.Size() < c.diskReclaimTrigger.Load()
	}, 5*time.Second, time.Millisecond)
	// Join the worker before checking the completed unlinks.
	require.NoError(t, c.Close())
	objects, err := os.ReadDir(filepath.Join(dir, "objects"))
	require.NoError(t, err)
	require.NotEmpty(t, objects)
	require.Less(t, uint64(len(objects)*len(value)), c.diskReclaimTrigger.Load())
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
	require.Less(t, len(objects)*(8<<10), target)
}
