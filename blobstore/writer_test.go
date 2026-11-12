package blobstore

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func checkWriterRecord(t *testing.T, st *Store, w *Writer, key, value []byte) RecordHeader {
	t.Helper()
	h := w.header
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
	n := align.PageAlign(h.FrameLen())
	raw := make([]byte, n)
	_, err = f.ReadAt(raw, h.fileOffset())
	require.NoError(t, err)
	var decoded RecordHeader
	end := int(h.FrameLen())
	require.NoError(t, (*RawHeader)(raw).Decode(&decoded, true))
	require.Equal(t, diskHeader, decoded)
	require.Equal(t, value, raw[h.ValueOffset():h.FrameLen()])
	require.Equal(t, key, raw[h.KeyOffset():h.KeyOffset()+int64(len(key))])
	require.Equal(t, make([]byte, len(raw)-end), raw[end:])
	require.Equal(t, make([]byte, int(h.ValueOffset())-RecordHeaderSize-len(key)), raw[RecordHeaderSize+len(key):h.ValueOffset()])
	return h
}

func TestWriterInlineReservationsAndCommit(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	st.mu.dataAppendOffset = 2 * align.BlockSize
	key, value := []byte("key"), bytes.Repeat([]byte("v"), 10400)
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	input := bytes.Clone(value[:4100])
	n, err := w.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	clear(input)
	n64, err := io.Copy(w, bytes.NewReader(value[4100:]))
	require.NoError(t, err)
	require.EqualValues(t, len(value)-4100, n64)
	require.NoError(t, putStoreWriter(st, key, w))
	h := checkWriterRecord(t, st, w, key, value)
	require.EqualValues(t, 2*align.BlockSize, h.Offset)
	require.Equal(t, map[uint64]RecordHeader{h.ObjectID: h}, indexHeaders(st.mu.index))
}

func TestWriterExternalBuffering(t *testing.T) {
	for _, size := range []int{100003, 512 << 10} {
		st := openTestStore(t, t.TempDir(), WithVerifyChecksums(true))
		st.memory = newStoreMemory(t, 2<<20)
		key, value := bytes.Repeat([]byte("k"), MaxKeyLen), make([]byte, size)
		for i := range value {
			value[i] = byte(i*37 + i/4096)
		}
		w, err := st.NewWriter()
		require.NoError(t, err)
		defer w.Discard()
		capacity := w.buf.Cap()
		_, err = w.Write(value[:21000])
		require.NoError(t, err)
		n, err := io.Copy(w, bytes.NewReader(value[21000:]))
		require.NoError(t, err)
		require.EqualValues(t, len(value)-21000, n)
		require.GreaterOrEqual(t, w.buf.Cap(), capacity, "unknown-size values grow in allocator memory")
		require.NoError(t, putStoreWriter(st, key, w))
		h := checkWriterRecord(t, st, w, key, value)
		require.True(t, h.Flags.IsExternal())
		readReclaimRecord(t, st, string(key), h, value)
		w.Discard()
		require.Equal(t, st.memory.Capacity(), st.memory.Available())
	}
}

func TestWriterAllocationExhaustionAndDiscard(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	held := alloc.NewAllocatingBuffer(st.memory)
	require.NoError(t, held.Grow(int(st.memory.Capacity())))
	w, err := st.NewWriter()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	require.Nil(t, w)
	held.Release()
	w, err = st.NewWriter()
	require.NoError(t, err)
	_, err = w.Write(make([]byte, 9000))
	require.NoError(t, err)
	w.Discard()
	w.Discard()
	require.Empty(t, indexReservations(st.mu.index))
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

type writerErrorReader struct{ err error }

func (r writerErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestWriterCopySourceErrorDiscard(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	w, err := st.NewWriter()
	require.NoError(t, err)
	failure := errors.New("source failed")
	n, err := io.Copy(w, io.MultiReader(bytes.NewReader([]byte("partial")), writerErrorReader{failure}))
	require.EqualValues(t, 7, n)
	require.ErrorIs(t, err, failure)
	w.Discard()
	require.Empty(t, indexHeaders(st.mu.index))
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

func TestWriterEmptyAndExactPage(t *testing.T) {
	for _, size := range []int{0, align.BlockSize - RecordHeaderSize - 3} {
		st := openTestStore(t, t.TempDir(), WithDataIntegrity(DataIntegrityNone))
		key, value := []byte("key"), bytes.Repeat([]byte("v"), size)
		w, err := st.NewWriter()
		require.NoError(t, err)
		input := bytes.NewReader(append(bytes.Clone(value), []byte("next")...))
		n, err := io.Copy(w, io.LimitReader(input, int64(size)))
		require.NoError(t, err)
		require.EqualValues(t, size, n)
		require.Equal(t, 4, input.Len())
		require.NoError(t, putStoreWriter(st, key, w))
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
	require.EqualValues(t, 4*align.BlockSize, stat.Size())
}

func TestStorePutBorrowsWriterBuffer(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	w, err := st.NewWriter()
	require.NoError(t, err)
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	h := sealWriter(t, []byte("key"), w)
	buffer := w.Buffer()
	ticket, err := st.Put(w, nil)
	require.NoError(t, err)
	require.NoError(t, ticket.Wait())
	var data []byte
	view := buffer.View(0, buffer.Cap())
	for chunk, err := view.Next(); err == nil; chunk, err = view.Next() {
		data = append(data, chunk...)
	}
	require.Equal(t, []byte("value"), data[h.ValueOffset():h.ValueOffset()+5])
	w.Discard()
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

func TestWriterAfterDiscardOrPut(t *testing.T) {
	for _, put := range []bool{false, true} {
		st := openTestStore(t, t.TempDir())
		w, err := st.NewWriter()
		require.NoError(t, err)
		defer w.Discard()
		if put {
			require.NoError(t, putStoreWriter(st, []byte("key"), w))
		} else {
			w.Discard()
		}
		_, err = w.Write([]byte("x"))
		require.ErrorIs(t, err, map[bool]error{false: io.ErrClosedPipe, true: ErrSealed}[put])
		_, err = io.Copy(w, bytes.NewReader([]byte("x")))
		require.ErrorIs(t, err, map[bool]error{false: io.ErrClosedPipe, true: ErrSealed}[put])
		require.ErrorIs(t, putStoreWriter(st, []byte("key"), w), map[bool]error{false: io.ErrClosedPipe, true: ErrSealed}[put])
	}
}

func TestWriterSealDefersAllIOAndPreservesValue(t *testing.T) {
	for _, keyLen := range []int{3, MaxKeyLen} {
		t.Run(strconv.Itoa(keyLen), func(t *testing.T) {
			st := openTestStore(t, t.TempDir())
			w, err := st.NewWriter()
			require.NoError(t, err)
			defer w.Discard()
			value := bytes.Repeat([]byte("v"), 300001)
			_, err = io.Copy(w, bytes.NewReader(value))
			require.NoError(t, err)
			firstValue := w.buf.RawAllocations()[0].Bytes()[recordPrefixSize:]
			if len(firstValue) == 0 {
				firstValue = w.buf.RawAllocations()[1].Bytes()
			}
			first := &firstValue[0]
			_, err = w.WriteTo(io.Discard)
			require.ErrorIs(t, err, ErrUnsealed)
			require.ErrorIs(t, w.Seal(nil), ErrEmptyKey)
			require.ErrorIs(t, w.Seal(make([]byte, MaxKeyLen+1)), ErrKeyTooLarge)
			key := bytes.Repeat([]byte("k"), keyLen)
			sealWriter(t, key, w)
			require.Equal(t, 1, st.mu.index.records.Len())
			sealedValue := w.buf.RawAllocations()[0].Bytes()[recordPrefixSize:]
			if len(sealedValue) == 0 {
				sealedValue = w.buf.RawAllocations()[1].Bytes()
			}
			require.Same(t, first, &sealedValue[0], "sealing does not move value bytes")
			require.ErrorIs(t, w.Seal(key), ErrSealed)
			require.ErrorIs(t, w.Seal([]byte("different")), ErrSealed)
			_, err = w.Write([]byte("x"))
			require.ErrorIs(t, err, ErrSealed)
			clear(key)
			var frame bytes.Buffer
			n, err := w.WriteTo(&frame)
			require.NoError(t, err)
			require.EqualValues(t, w.Header().FrameLen(), n)
			require.Equal(t, value, frame.Bytes()[w.Header().ValueOffset():w.Header().FrameLen()])
			require.Equal(t, bytes.Repeat([]byte("k"), keyLen), frame.Bytes()[RecordHeaderSize:RecordHeaderSize+keyLen])
			n2, err := w.WriteTo(io.Discard)
			require.NoError(t, err)
			require.Zero(t, n2, "WriteTo drains the sealed frame")
			info, err := st.data.Stat()
			require.NoError(t, err)
			require.Zero(t, info.Size())
			entries, err := os.ReadDir(st.base + "/objects")
			require.NoError(t, err)
			require.Empty(t, entries)
			require.Empty(t, indexReservations(st.mu.index))
		})
	}
}

func TestWriterSealNeedsNoMoreMemory(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	w, err := st.NewWriter()
	require.NoError(t, err)
	_, err = w.Write(make([]byte, w.buf.Cap()-recordPrefixSize))
	require.NoError(t, err)
	held := alloc.NewAllocatingBuffer(st.memory)
	require.NoError(t, held.Grow(int(st.memory.Available())))
	sealWriter(t, bytes.Repeat([]byte("k"), MaxKeyLen), w)
	require.Equal(t, 1, st.mu.index.records.Len())
	require.Zero(t, st.memory.Available())
	require.Empty(t, indexReservations(st.mu.index))
	held.Release()
	w.Discard()
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

func TestPutSubmissionFailureKeepsCallerMemory(t *testing.T) {
	for _, external := range []bool{false, true} {
		st := openTestStore(t, t.TempDir())
		if external {
			st.cfg.inlineThreshold = 0
		}
		w, err := st.NewWriter()
		require.NoError(t, err)
		_, err = w.Write([]byte("value"))
		require.NoError(t, err)
		sealWriter(t, []byte("key"), w)
		require.Equal(t, 1, st.mu.index.records.Len())
		require.Empty(t, indexHeaders(st.mu.index))
		rejectTestIO(st).reject = ErrRejected
		completed := 0
		_, err = st.Put(w, func(err error) {
			completed++
			require.ErrorIs(t, err, ErrRejected)
		})
		require.ErrorIs(t, err, ErrRejected)
		require.Equal(t, 1, completed)
		require.Zero(t, st.mu.index.records.Len())
		require.NotNil(t, w.buf, "Store.Put does not release caller memory")
		w.Discard()
		require.Equal(t, st.memory.Capacity(), st.memory.Available())
	}
}

func TestStorePutPublishesAfterCallerUpdates(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write([]byte("value"))
	require.NoError(t, err)
	sealWriter(t, []byte("key"), w)
	called := false
	ticket, err := st.Put(w, func(err error) {
		require.NoError(t, err)
		require.Zero(t, st.Size(), "cache updates precede storage publication")
		called = true
	})
	require.NoError(t, err)
	require.NoError(t, ticket.Wait())
	require.True(t, called)
	require.NotNil(t, w.buf, "the caller still owns the buffer")
	require.EqualValues(t, len("value"), st.Size())
	checkWriterRecord(t, st, w, []byte("key"), []byte("value"))
}

func sealWriter(t *testing.T, key []byte, w *Writer) RecordHeader {
	t.Helper()
	require.NoError(t, w.Seal(key))
	return w.Header()
}

// DIO splits a frame with more than IOV_MAX fragments within one submission.
func TestPutHighlyFragmentedFrame(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0), WithVerifyChecksums(true))
	st.memory = newStoreMemory(t, 12<<20)
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	var held []alloc.Memory
	for {
		memory, err := st.memory.TryAcquire(align.BlockSize)
		if errors.Is(err, alloc.ErrExhausted) {
			break
		}
		require.NoError(t, err)
		held = append(held, memory)
	}
	for i := 0; i < len(held); i += 2 {
		held[i].Unpin()
	}
	defer func() {
		for i := 1; i < len(held); i += 2 {
			held[i].Unpin()
		}
	}()
	value := bytes.Repeat([]byte("v"), 1100*align.BlockSize-3)
	_, err = w.Write(value)
	require.NoError(t, err)
	require.Greater(t, len(w.buf.RawAllocations()), 1024)
	require.NoError(t, putStoreWriter(st, []byte("fragmented"), w))
	checkWriterRecord(t, st, w, []byte("fragmented"), value)
	h := w.Header()
	w.Discard()
	readReclaimRecord(t, st, "fragmented", h, value)
}
