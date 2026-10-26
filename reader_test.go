package blobcache

import (
	"bytes"
	"testing"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestMemoryReadWithoutStore(t *testing.T) {
	pool, err := alloc.NewExtentAllocator(align.BlockSize, 8)
	require.NoError(t, err)
	defer pool.Close()
	// No Store, scheduler, or files: a resident hit must stay in blobcache.
	c := &Cache{values: newMemoryCache()}
	defer c.values.clear()
	key, value := bytes.Repeat([]byte("k"), 5000), bytes.Repeat([]byte("v"), 20000)
	h := blobstore.RecordHeader{
		Flags: blobstore.FlagHeaderIntegrity, KeyLen: uint16(len(key)),
		PhysicalLen: uint64(len(value)), LogicalLen: uint64(len(value)),
	}
	// Leave the payload checksum unset: resident reads trust memory even when
	// the record flags say a checksum is present.
	h.Flags.SetDataIntegrity(blobstore.DataIntegrityCRC32C)
	var raw blobstore.RawHeader
	h.Encode(&raw)
	framed := append(append(raw[:], key...), value...)
	memory, err := pool.TryAcquire(int64(len(framed)))
	require.NoError(t, err)
	for rest, i := framed, 0; len(rest) != 0; i++ {
		rest = rest[copy(memory.Chunks()[i], rest):]
	}
	c.values.replace(blobstore.HashKey(key), memory, h)
	checkRead(t, c, key, value) // Full key and value span multiple chunks.

	// Simulate hash collisions, including a mismatch beyond the first chunk.
	other := bytes.Clone(key)
	other[len(other)-1] ^= 1
	for _, wrongKey := range [][]byte{key[:1], other} {
		require.True(t, memory.TryInc())
		c.values.replace(blobstore.HashKey(wrongKey), memory, h)
		r, err := c.Read(wrongKey)
		require.Nil(t, r)
		require.ErrorIs(t, err, ErrNotFound)
	}
	c.values.clear()
	require.Equal(t, pool.Capacity(), pool.Available(), "success and failure release reader pins")
}
