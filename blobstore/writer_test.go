package blobstore

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func checkWriterRecord(t *testing.T, st *Store, w *Writer, key, value []byte) RecordHeader {
	t.Helper()
	h := w.header
	require.Equal(t, uint64(len(value)), h.PhysicalLen)
	require.Equal(t, h.PhysicalLen, h.LogicalLen)
	require.Equal(t, uint16(len(key)), h.KeyLen)
	if h.Flags.DataIntegrity() == DataIntegrityCRC32C {
		require.Equal(t, uint64(crc32.Checksum(append(bytes.Clone(key), value...), crc32.MakeTable(crc32.Castagnoli))), h.Hash)
	} else {
		require.Zero(t, h.Hash)
	}
	path := st.data.Name()
	if h.Flags.IsExternal() {
		path = filepath.Join(st.base, "objects", strconv.FormatUint(h.ObjectID, 10))
	}
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	n := align.PageAlign(RecordHeaderSize + int64(len(key)+len(value)))
	raw := make([]byte, n)
	_, err = f.ReadAt(raw, h.fileOffset())
	require.NoError(t, err)
	var decoded RecordHeader
	require.NoError(t, (*RawHeader)(raw).Decode(&decoded, true))
	require.Equal(t, h, decoded)
	require.Equal(t, key, raw[RecordHeaderSize:RecordHeaderSize+len(key)])
	require.Equal(t, value, raw[RecordHeaderSize+len(key):RecordHeaderSize+len(key)+len(value)])
	require.Equal(t, make([]byte, int(n)-RecordHeaderSize-len(key)-len(value)), raw[RecordHeaderSize+len(key)+len(value):])
	h.Hash = HashKey(key) // Return the metadata representation used by the index.
	return h
}

func TestWriterInlineReservationsAndCommit(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	st.mu.dataAppendOffset = 2 * align.BlockSize
	key := []byte("/bucket/foo/bar.txt")
	value := bytes.Repeat([]byte("data"), 2600)
	w, err := st.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	defer w.Discard()
	require.Zero(t, w.Size(), "copying the key must not add value bytes")
	require.Equal(t, uint64(2*align.BlockSize), w.header.Offset)
	require.False(t, w.header.Flags.IsExternal())
	require.NotZero(t, w.header.ObjectID)
	require.Empty(t, indexHeaders(st.mu.index))
	end := uint64(2*align.BlockSize + align.PageAlign(RecordHeaderSize+int64(len(key)+len(value))))
	require.Equal(t, end, st.mu.dataAppendOffset)

	other, err := st.NewWriter([]byte("next"), 100)
	require.NoError(t, err)
	defer other.Discard()
	require.Equal(t, end, other.header.Offset)
	require.Equal(t, w.header.ObjectID+1, other.header.ObjectID)

	input := bytes.Clone(value[:4100])
	n, err := w.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	clear(input) // The caller can reuse its slice as soon as Write returns.
	require.NoError(t, w.Flush())
	n64, err := w.ReadFrom(bytes.NewReader(value[4100:]))
	require.NoError(t, err)
	require.Equal(t, int64(len(value)-4100), n64)
	require.Equal(t, int64(len(value)), w.Size())
	require.Empty(t, indexHeaders(st.mu.index))
	stat, err := st.mu.index.file.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 0, stat.Size())

	require.NoError(t, st.Put(w))
	h := checkWriterRecord(t, st, w, key, value)
	require.Equal(t, map[uint64]RecordHeader{h.ObjectID: h}, indexHeaders(st.mu.index))
	require.Equal(t, uint64(len(value)), st.mu.index.size())
}

func TestWriterExternalStagingReuse(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
	}{
		{"known, multiple chunks", 100003},
		{"unknown, single extent", SizeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t, t.TempDir())
			capacity, chunks := int64(16<<10), 1
			if tc.size != SizeUnknown {
				staging, err := alloc.NewSlabAllocator(3 * align.BlockSize)
				require.NoError(t, err)
				t.Cleanup(staging.Close)
				st.staging = staging
				capacity, chunks = 3*align.BlockSize, 3
			}
			key := bytes.Repeat([]byte("k"), 3*align.BlockSize+13)
			value := make([]byte, 100003)
			for i := range value {
				value[i] = byte(i*37 + i/4096)
			}
			w, err := st.NewWriter(key, tc.size)
			require.NoError(t, err)
			defer w.Discard()
			require.Zero(t, w.Size(), "copying the key must not add value bytes")
			require.True(t, w.header.Flags.IsExternal())
			require.Zero(t, st.mu.dataAppendOffset)
			require.Empty(t, indexHeaders(st.mu.index))
			require.Equal(t, capacity, w.buffer.Cap())
			require.Len(t, w.memory.Chunks(), chunks)
			for start := 0; start < 21000; start += 3000 {
				n, err := w.Write(value[start : start+3000])
				require.NoError(t, err)
				require.Equal(t, 3000, n)
			}
			require.NoError(t, w.Flush())
			n, err := w.ReadFrom(bytes.NewReader(value[21000:]))
			require.NoError(t, err)
			require.Equal(t, int64(len(value)-21000), n)
			name := st.objectPath(w.header.ObjectID)
			require.NoError(t, st.Put(w))
			checkWriterRecord(t, st, w, key, value)
			w.Discard()
			_, err = os.Stat(name)
			require.NoError(t, err)
		})
	}
}

func TestWriterFragmentedBuffer(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	pool, err := alloc.NewSlabAllocator(8 * align.BlockSize)
	require.NoError(t, err)
	defer pool.Close()
	occupied := make([]*alloc.Buffer, 8)
	for i := range occupied {
		occupied[i], err = pool.TryAcquire(align.BlockSize)
		require.NoError(t, err)
	}
	for i, buffer := range occupied {
		if i%2 == 0 {
			buffer.Unpin()
		} else {
			defer buffer.Unpin()
		}
	}
	st.memory = pool
	key := []byte("fragmented")
	value := bytes.Repeat([]byte("v"), 3*align.BlockSize)
	w, err := st.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	defer w.Discard()
	require.Len(t, w.memory.Chunks(), 4)
	_, err = w.ReadFrom(bytes.NewReader(value))
	require.NoError(t, err)
	require.NoError(t, st.Put(w))
	checkWriterRecord(t, st, w, key, value)
	retained := w.RetainBuffer()
	require.Same(t, w.memory, retained)
	defer retained.Unpin()
	w.Discard()
	r := newBufferReader(retained, RecordHeaderSize+int64(len(key)), w.Size())
	prefix := make([]byte, 31)
	_, err = io.ReadFull(r, prefix)
	require.NoError(t, err)
	var rest bytes.Buffer
	n, err := r.WriteTo(&rest)
	require.NoError(t, err)
	require.Equal(t, int64(len(value)-len(prefix)), n)
	require.Equal(t, value, append(prefix, rest.Bytes()...))
	_, err = r.Read(prefix)
	require.ErrorIs(t, err, io.EOF)
}

func TestWriterAllocationExhaustionAndDiscard(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	all, err := st.memory.TryAcquire(128 << 10)
	require.NoError(t, err)
	w, err := st.NewWriter([]byte("key"), 100)
	require.ErrorIs(t, err, alloc.ErrExhausted)
	require.Nil(t, w)
	require.Empty(t, indexReservations(st.mu.index))
	require.Zero(t, st.mu.dataAppendOffset)
	all.Unpin()

	all, err = st.staging.TryAcquire(32 << 10)
	require.NoError(t, err)
	w, err = st.NewWriter([]byte("key"), SizeUnknown)
	require.ErrorIs(t, err, alloc.ErrExhausted)
	require.Nil(t, w)
	require.Empty(t, indexReservations(st.mu.index))
	entries, err := os.ReadDir(filepath.Join(st.base, "objects"))
	require.NoError(t, err)
	require.Empty(t, entries)
	all.Unpin()

	w, err = st.NewWriter([]byte("key"), SizeUnknown)
	require.NoError(t, err)
	_, err = w.Write(make([]byte, 2*align.BlockSize))
	require.NoError(t, err)
	name := st.objectPath(w.header.ObjectID)
	w.Discard()
	w.Discard()
	st.Wait()
	_, err = os.Stat(name)
	require.ErrorIs(t, err, os.ErrNotExist)
	all, err = st.staging.TryAcquire(32 << 10)
	require.NoError(t, err)
	all.Unpin()
}

type writerErrorReader struct{ err error }

func (r writerErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestWriterReadFromSourceError(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	key := []byte("key")
	value := bytes.Repeat([]byte("v"), align.BlockSize-RecordHeaderSize-len(key))
	w, err := st.NewWriter(key, int64(len(value)+1))
	require.NoError(t, err)
	defer w.Discard()
	failure := errors.New("download failed")
	n, err := w.ReadFrom(io.MultiReader(bytes.NewReader(value), writerErrorReader{failure}))
	require.Equal(t, int64(len(value)), n)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, st.Put(w), failure)
	require.Empty(t, indexHeaders(st.mu.index), "failed writers publish no record")
	require.Zero(t, st.mu.index.size())
}

func TestWriterEmptyAndExactPage(t *testing.T) {
	for _, size := range []int{0, align.BlockSize - RecordHeaderSize - len("key")} {
		st := openTestStore(t, t.TempDir())
		st.cfg.flags = 0
		value := bytes.Repeat([]byte("v"), size)
		w, err := st.NewWriter([]byte("key"), int64(size))
		require.NoError(t, err)
		defer w.Discard()
		input := bytes.NewReader(append(bytes.Clone(value), []byte("next")...))
		n, err := w.ReadFrom(input)
		require.NoError(t, err)
		require.Equal(t, int64(size), n)
		require.Equal(t, len("next"), input.Len())
		require.NoError(t, st.Put(w))
		checkWriterRecord(t, st, w, []byte("key"), value)
	}
}

func TestWriterInlineUsesOnlyReservedPages(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	memory, err := alloc.NewExtentAllocator(16<<10, 2)
	require.NoError(t, err)
	defer memory.Close()
	st.memory = memory
	key := []byte("key")
	value := bytes.Repeat([]byte("v"), 100)
	first, err := st.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	defer first.Discard()
	second, err := st.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	defer second.Discard()
	require.Equal(t, int64(16<<10), first.buffer.Cap())
	require.Equal(t, uint64(align.BlockSize), second.header.Offset)
	_, err = second.Write(value)
	require.NoError(t, err)
	require.NoError(t, st.Put(second))
	checkWriterRecord(t, st, second, key, value)
	index, err := openDiskIndex(st.mu.index.file.Name(), true)
	require.NoError(t, err)
	expected := second.header
	expected.Hash = second.key
	require.Equal(t, []RecordHeader{expected}, indexReservations(index))
	require.NoError(t, index.close())
	_, err = first.Write(value)
	require.NoError(t, err)
	require.NoError(t, st.Put(first))
	checkWriterRecord(t, st, first, key, value)
	got := make([]byte, len(value))
	_, err = st.data.ReadAt(got, int64(second.header.Offset)+RecordHeaderSize+int64(len(key)))
	require.NoError(t, err)
	require.Equal(t, value, got)
	stat, err := st.data.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(2*align.BlockSize), stat.Size())
}

func newBufferReader(memory *alloc.Buffer, offset, size int64) *Buffer {
	b := NewBuffer(memory.Chunks())
	b.rPos, b.wPos = b.position(offset), b.position(offset+size)
	return b
}

func TestWriterRetainedBufferLifetime(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	key := []byte("key")
	w, err := st.NewWriter(key, 10)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, st.Put(w))
	buffer, header := w.RetainBuffer(), w.Header()
	require.NotNil(t, buffer)
	defer buffer.Unpin()
	require.Equal(t, uint64(5), header.PhysicalLen)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
	value, err := io.ReadAll(newBufferReader(buffer, RecordHeaderSize+int64(len(key)), 5))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)

	w, err = st.NewWriter(key, SizeUnknown)
	require.NoError(t, err)
	defer w.Close()
	require.NoError(t, st.Put(w))
	retained := w.RetainBuffer()
	require.Nil(t, retained, "object staging is never a complete cached value")
	require.NoError(t, w.Close())
	require.NoError(t, st.Close())
}
