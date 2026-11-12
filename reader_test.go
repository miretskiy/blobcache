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
	pool := newTestPool(t, 8*align.BlockSize)
	// No Store, scheduler, or files: a resident hit must stay in blobcache.
	c := &Cache{values: newMemoryCache()}
	defer c.values.clear()
	key, value := bytes.Repeat([]byte("k"), MaxKeyLen), bytes.Repeat([]byte("v"), 20000)
	h := blobstore.RecordHeader{
		Flags: blobstore.FlagHeaderIntegrity, KeyLen: uint16(len(key)),
		PhysicalLen: uint64(len(value)), LogicalLen: uint64(len(value)),
	}
	// Leave the payload checksum unset: resident reads trust memory even when
	// the record flags say a checksum is present.
	h.Flags.SetDataIntegrity(blobstore.DataIntegrityCRC32C)
	var raw blobstore.RawHeader
	h.Encode(&raw)
	framed := make([]byte, h.FrameLen())
	copy(framed, raw[:])
	copy(framed[h.KeyOffset():], key)
	copy(framed[h.ValueOffset():], value)
	memory, err := pool.TryAcquire(int64(len(framed)))
	require.NoError(t, err)
	copy(singleMemoryChunk(memory), framed)
	c.values.replace(blobstore.HashKey(key), memory, h, false)
	checkRead(t, c, key, value)

	// Simulate hash collisions, including a mismatch at the end of the key.
	other := bytes.Clone(key)
	other[len(other)-1] ^= 1
	for _, wrongKey := range [][]byte{key[:1], other} {
		memory.Retain()
		c.values.replace(blobstore.HashKey(wrongKey), memory, h, false)
		r, err := c.Read(wrongKey)
		require.Nil(t, r)
		require.ErrorIs(t, err, ErrNotFound)
	}
	c.values.clear()
	require.Equal(t, pool.Capacity(), pool.Available(), "success and failure release reader pins")
}

func TestMemoryReaderKeyFitsFirstPage(t *testing.T) {
	c := newPutCache(t, t.TempDir())
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
	buffer := residentBuffer(t, c, key)
	require.Len(t, buffer.First(), align.BlockSize, "the header and maximum-length key share the first allocation")
	checkRead(t, c, key, value)
	c.values.reclaimCacheMemory(c.memory.Capacity())
	checkRead(t, c, key, value)
}

func TestMemoryReaderRejectsIncompleteFrame(t *testing.T) {
	pool := newTestPool(t, 128<<10)
	buffer, err := pool.TryAcquire(align.BlockSize)
	require.NoError(t, err)
	header := blobstore.RecordHeader{KeyLen: MaxKeyLen, PhysicalLen: 1}
	r, err := newMemoryReader(bytes.Repeat([]byte("k"), MaxKeyLen), header, buffer)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrCorrupt)
	require.Equal(t, pool.Capacity(), pool.Available())
}
