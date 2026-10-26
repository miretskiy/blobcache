package blobstore

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func publishForRead(t *testing.T, st *Store, key, value []byte) RecordHeader {
	t.Helper()
	w, err := st.NewWriter(key, int64(len(value)))
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write(value)
	require.NoError(t, err)
	require.NoError(t, st.Put(w))
	return w.Header()
}

func TestReaderFragmentedBuffers(t *testing.T) {
	for _, integrity := range []DataIntegrity{DataIntegrityNone, DataIntegrityCRC32C} {
		t.Run(string(rune('0'+integrity)), func(t *testing.T) {
			pool, err := alloc.NewExtentAllocator(align.BlockSize, 32)
			require.NoError(t, err)
			defer pool.Close()
			st, err := Open(t.TempDir(), pool, pool, WithStagingSize(12<<10), WithDataIntegrity(integrity), WithVerifyChecksums(true))
			require.NoError(t, err)
			defer st.Close()
			key, value := bytes.Repeat([]byte("k"), 5000), bytes.Repeat([]byte("v"), 20000)
			w, err := st.NewWriter(key, int64(len(value)))
			require.NoError(t, err)
			defer w.Close()
			_, err = w.Write(value)
			require.NoError(t, err)
			require.NoError(t, st.Put(w))
			h := w.Header()
			require.NoError(t, w.Close())
			r, err := st.NewReader(key, h)
			require.NoError(t, err)
			defer r.Close()
			require.Greater(t, len(r.memory.Chunks()), 1)
			got, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, value, got)
			for _, chunk := range r.memory.Chunks() {
				require.Len(t, chunk, align.BlockSize, "I/O trimming must not change allocator chunks")
			}
			require.NoError(t, r.Close())
			require.Equal(t, pool.Capacity(), pool.Available())
		})
	}
}

func TestReaderStaleLocationAndMissingObject(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithTargetSize(64<<10), WithInlineThreshold(64<<10))
	old := publishForRead(t, st, []byte("old"), bytes.Repeat([]byte("v"), (64<<10)-RecordHeaderSize-3))
	fresh := publishForRead(t, st, []byte("new"), []byte("value"))
	require.Equal(t, old.Offset, fresh.Offset)
	r, err := st.NewReader([]byte("old"), old)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
	// Full-key comparison still matters if a hash lookup picked the wrong key.
	r, err = st.NewReader([]byte("bad"), fresh)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)

	object := putReclaimSizedRecord(t, st, "object", bytes.Repeat([]byte("v"), 40000), SizeUnknown)
	r, err = st.NewReader([]byte("object"), object)
	require.NoError(t, err)
	defer r.Close()
	// Each refill opens its own descriptor, so removing the object mid-stream
	// fails the next refill rather than reading through a retained descriptor.
	require.NoError(t, os.Remove(filepath.Join(st.base, "objects", strconv.FormatUint(object.ObjectID, 10))))
	_, err = io.Copy(io.Discard, r)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, r.Close())
	r, err = st.NewReader([]byte("object"), object)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound)
	pool := st.staging.(*alloc.ExtentAllocator)
	require.Equal(t, pool.Capacity(), pool.Available(), "misses release staging memory")
}

func TestReaderDetectsCorruptAndTruncatedData(t *testing.T) {
	for _, mutation := range []string{"header", "payload", "truncate"} {
		t.Run(mutation, func(t *testing.T) {
			st := openTestStore(t, t.TempDir(), WithVerifyChecksums(true))
			key := []byte("key")
			h := publishForRead(t, st, key, bytes.Repeat([]byte("v"), 40000))
			f, err := os.OpenFile(filepath.Join(st.base, "objects", strconv.FormatUint(h.ObjectID, 10)), os.O_WRONLY, 0)
			require.NoError(t, err)
			defer f.Close()
			end := int64(RecordHeaderSize+len(key)) + int64(h.PhysicalLen)
			switch mutation {
			case "header":
				_, err = f.WriteAt([]byte{0xff}, 8)
			case "payload":
				_, err = f.WriteAt([]byte{0xff}, end-1)
			case "truncate":
				err = f.Truncate(end - 1)
			}
			require.NoError(t, err)
			r, err := st.NewReader(key, h)
			if mutation == "header" {
				require.Nil(t, r)
				require.ErrorIs(t, err, ErrCorrupt)
				return
			}
			require.NoError(t, err)
			defer r.Close()
			// Partial reads and Close do not drain or verify unseen data.
			_, err = r.Read(make([]byte, 1))
			require.NoError(t, err)
			require.NoError(t, r.Close())
			r, err = st.NewReader(key, h)
			require.NoError(t, err)
			defer r.Close()
			_, err = io.Copy(io.Discard, r)
			require.ErrorIs(t, err, ErrCorrupt)
			_, err = r.Read(make([]byte, 1))
			require.ErrorIs(t, err, ErrCorrupt)
			require.NoError(t, r.Close())
		})
	}
}

func TestReaderAllowsMissingPadding(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0), WithVerifyChecksums(true))
	key, value := []byte("key"), []byte("value")
	h := publishForRead(t, st, key, value)
	require.NoError(t, os.Truncate(st.objectPath(h.ObjectID), RecordHeaderSize+int64(len(key)+len(value))))
	r, err := st.NewReader(key, h)
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, got)
}

func TestHeaderChecksumPolicyAfterRestart(t *testing.T) {
	for _, flags := range []RecordFlags{0, FlagHeaderIntegrity} {
		dir := t.TempDir()
		st := openTestStore(t, dir, funcOpt(func(c *config) { c.flags = flags }))
		key, value := []byte("key"), []byte("value")
		h := publishForRead(t, st, key, value)
		require.NoError(t, st.Close())
		// Change only the CRC bytes, leaving decoded metadata and payload intact.
		for name, offset := range map[string]int64{
			"data.db":  RecordHeaderSize - 4,
			"index.db": RecordHeaderSize - 4,
		} {
			f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR, 0)
			require.NoError(t, err)
			var crc [1]byte
			_, err = f.ReadAt(crc[:], offset)
			require.NoError(t, err)
			crc[0] ^= 1
			_, err = f.WriteAt(crc[:], offset)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}
		for _, verify := range []bool{false, true} {
			reopened, err := Open(dir, st.memory, st.staging, TestingWithDirectIO(false), WithVerifyChecksums(verify))
			if verify && flags.HasHeaderIntegrity() {
				require.ErrorIs(t, err, ErrBadCRC)
				continue
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			r, err := reopened.NewReader(key, h)
			require.NoError(t, err)
			got, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, value, got)
			require.NoError(t, r.Close())
			require.NoError(t, reopened.Close())
		}
	}
}
