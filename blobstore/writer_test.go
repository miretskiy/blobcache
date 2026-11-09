package blobstore

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func checkWriterRecord(t *testing.T, st *Store, w *Writer, key, value []byte) RecordHeader {
	t.Helper()
	h := w.wr.(*bufferSink).header
	require.EqualValues(t, len(value), h.PhysicalLen)
	require.Equal(t, h.PhysicalLen, h.LogicalLen)
	require.EqualValues(t, len(key), h.KeyLen)
	diskHeader := h
	diskHeader.Hash = 0
	if h.Flags.DataIntegrity() == DataIntegrityCRC32C {
		diskHeader.Hash = uint64(crc32.Checksum(append(bytes.Clone(key), value...), crcTable))
	}
	path := st.data.Name()
	if h.Flags.IsExternal() {
		path = st.objectPath(h.ObjectID)
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
	prefix := diskHeader
	if h.Flags&FlagFooter != 0 {
		prefix.PhysicalLen, prefix.LogicalLen, prefix.Hash = 0, 0, 0
		var footer RawHeader
		_, err = f.ReadAt(footer[:], n)
		require.NoError(t, err)
		var final RecordHeader
		require.NoError(t, footer.Decode(&final, true))
		require.Equal(t, diskHeader, final)
	}
	require.Equal(t, prefix, decoded)
	start := RecordHeaderSize + len(key)
	require.Equal(t, key, raw[RecordHeaderSize:start])
	require.Equal(t, value, raw[start:start+len(value)])
	require.Equal(t, make([]byte, len(raw)-start-len(value)), raw[start+len(value):])
	return h
}

func TestWriterInlineReservationsAndCommit(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	st.mu.dataAppendOffset = 2 * align.BlockSize
	key, value := []byte("key"), bytes.Repeat([]byte("v"), 10400)
	w, err := st.NewWriter(key)
	require.NoError(t, err)
	defer w.Close()
	input := bytes.Clone(value[:4100])
	n, err := w.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	clear(input)
	n64, err := w.ReadFrom(bytes.NewReader(value[4100:]))
	require.NoError(t, err)
	require.EqualValues(t, len(value)-4100, n64)
	require.NoError(t, putStoreWriter(st, w))
	h := checkWriterRecord(t, st, w, key, value)
	require.EqualValues(t, 2*align.BlockSize, h.Offset)
	require.Equal(t, map[uint64]RecordHeader{h.ObjectID: h}, indexHeaders(st.mu.index))
}

func TestWriterExternalBuffering(t *testing.T) {
	for _, size := range []int{100003, 512 << 10} {
		st := openTestStore(t, t.TempDir(), WithVerifyChecksums(true))
		st.memory = newStoreMemory(t, 128<<10)
		key, value := bytes.Repeat([]byte("k"), 3*align.BlockSize+13), make([]byte, size)
		for i := range value {
			value[i] = byte(i*37 + i/4096)
		}
		w, err := st.NewWriter(key)
		require.NoError(t, err)
		defer w.Close()
		capacity := w.buf.Cap()
		_, err = w.Write(value[:21000])
		require.NoError(t, err)
		n, err := w.ReadFrom(bytes.NewReader(value[21000:]))
		require.NoError(t, err)
		require.EqualValues(t, len(value)-21000, n)
		require.Equal(t, capacity, w.buf.Cap(), "streaming reuses the same allocation")
		require.NoError(t, putStoreWriter(st, w))
		h := checkWriterRecord(t, st, w, key, value)
		require.True(t, h.Flags.IsExternal())
		readReclaimRecord(t, st, string(key), h, value)
		require.Equal(t, st.memory.Capacity(), st.memory.Available())
	}
}

func TestWriterAllocationExhaustionAndDiscard(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	held := alloc.NewAllocatingBuffer(st.memory)
	require.NoError(t, held.Grow(int(st.memory.Capacity())))
	w, err := st.NewWriter([]byte("key"))
	require.ErrorIs(t, err, alloc.ErrExhausted)
	require.Nil(t, w)
	held.Release()
	w, err = st.NewWriter([]byte("key"))
	require.NoError(t, err)
	_, err = w.Write(make([]byte, 9000))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
	require.Empty(t, indexReservations(st.mu.index))
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

type writerErrorReader struct{ err error }

func (r writerErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestWriterReadFromSourceError(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	w, err := st.NewWriter([]byte("key"))
	require.NoError(t, err)
	failure := errors.New("source failed")
	n, err := w.ReadFrom(io.MultiReader(bytes.NewReader([]byte("partial")), writerErrorReader{failure}))
	require.EqualValues(t, 7, n)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, putStoreWriter(st, w), failure)
	require.Empty(t, indexHeaders(st.mu.index))
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

func TestWriterEmptyAndExactPage(t *testing.T) {
	for _, size := range []int{0, align.BlockSize - RecordHeaderSize - 3} {
		st := openTestStore(t, t.TempDir(), WithDataIntegrity(DataIntegrityNone))
		key, value := []byte("key"), bytes.Repeat([]byte("v"), size)
		w, err := st.NewWriter(key)
		require.NoError(t, err)
		input := bytes.NewReader(append(bytes.Clone(value), []byte("next")...))
		n, err := w.ReadFrom(io.LimitReader(input, int64(size)))
		require.NoError(t, err)
		require.EqualValues(t, size, n)
		require.Equal(t, 4, input.Len())
		require.NoError(t, putStoreWriter(st, w))
		checkWriterRecord(t, st, w, key, value)
	}
}

func TestWriterInlineUsesOnlyReservedPages(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	for range 2 {
		putReclaimRecord(t, st, "key", bytes.Repeat([]byte("v"), 100))
	}
	stat, err := st.data.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 2*align.BlockSize, stat.Size())
}

func TestWriterRetainedBufferLifetime(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	w, err := st.NewWriter([]byte("key"))
	require.NoError(t, err)
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	var h RecordHeader
	var memory Memory
	done := make(chan error, 1)
	err = st.Put(w, func(header RecordHeader, retained Memory) { h, memory = header, retained },
		func(_ RecordHeader, err error) { done <- err })
	require.NoError(t, err)
	require.NotEmpty(t, memory)
	require.NoError(t, <-done)
	require.NoError(t, w.Close())
	var data []byte
	for chunk := range memory.Chunks() {
		data = append(data, chunk...)
	}
	require.Equal(t, []byte("value"), data[RecordHeaderSize+int(h.KeyLen):RecordHeaderSize+int(h.KeyLen)+5])
	memory.Release()
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

func TestWriterAfterCloseOrPut(t *testing.T) {
	for _, put := range []bool{false, true} {
		st := openTestStore(t, t.TempDir())
		w, err := st.NewWriter([]byte("key"))
		require.NoError(t, err)
		if put {
			require.NoError(t, putStoreWriter(st, w))
		} else {
			require.NoError(t, w.Close())
		}
		_, err = w.Write([]byte("x"))
		require.ErrorIs(t, err, io.ErrClosedPipe)
		_, err = w.ReadFrom(bytes.NewReader([]byte("x")))
		require.ErrorIs(t, err, io.ErrClosedPipe)
		require.ErrorIs(t, putStoreWriter(st, w), io.ErrClosedPipe)
	}
}
