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
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

type countingReadScheduler struct {
	iosched.Scheduler
	submissions int
	rejectAt    int
}

func (s *countingReadScheduler) Submit(op iosched.Op) (iosched.Ticket, error) {
	s.submissions++
	if s.submissions == s.rejectAt {
		return iosched.Ticket{}, ErrRejected
	}
	return s.Scheduler.Submit(op)
}

func TestReaderOneShot(t *testing.T) {
	for _, external := range []bool{false, true} {
		threshold := int64(64 << 10)
		if external {
			threshold = 0
		}
		for _, size := range []int{0, 100, 48 << 10} {
			t.Run(strconv.FormatBool(external)+"/"+strconv.Itoa(size), func(t *testing.T) {
				st := openTestStore(t, t.TempDir(), WithInlineThreshold(threshold), WithVerifyChecksums(true))
				key, value := []byte("key"), bytes.Repeat([]byte("v"), size)
				h := publishForRead(t, st, key, value)
				scheduler := &countingReadScheduler{Scheduler: st.schedulers[0]}
				st.schedulers[0] = scheduler
				r, err := st.NewReader(key, h)
				require.NoError(t, err)
				defer r.Close()
				require.Equal(t, 1, scheduler.submissions)
				require.GreaterOrEqual(t, int64(r.frame.Cap()), h.FrameLen())
				got := make([]byte, 0, len(value))
				for p, err := r.Next(); err == nil; p, err = r.Next() {
					got = append(got, p...)
				}
				require.Equal(t, value, got)
				require.Equal(t, 1, scheduler.submissions, "the entire record uses one submission")
				_, err = r.Next()
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

func publishForRead(t *testing.T, st *Store, key, value []byte) RecordHeader {
	t.Helper()
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write(value)
	require.NoError(t, err)
	err = putStoreWriter(st, key, w)
	require.NoError(t, err)

	return w.header
}

func TestReaderFragmentedBuffers(t *testing.T) {
	for _, integrity := range []DataIntegrity{DataIntegrityNone, DataIntegrityCRC32C} {
		t.Run(strconv.Itoa(int(integrity)), func(t *testing.T) {
			st := openTestStore(t, t.TempDir(), WithInlineThreshold(0), WithDataIntegrity(integrity), WithVerifyChecksums(true))
			key, value := bytes.Repeat([]byte("k"), MaxKeyLen), bytes.Repeat([]byte("v"), 300000)
			h := publishForRead(t, st, key, value)
			var held []alloc.Memory
			for {
				m, err := st.memory.TryAcquire(128 << 10)
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

			r, err := st.NewReader(key, h)
			require.NoError(t, err)
			defer r.Close()
			chunk, err := r.Next()
			require.NoError(t, err)
			require.Less(t, len(chunk), len(value), "fragmented storage exposes separate chunks")
			require.Same(t, &r.frame.First()[h.ValueOffset()], &chunk[0], "Next borrows the actual read buffer")
			var prefix [7]byte
			_, err = io.ReadFull(r, prefix[:])
			require.NoError(t, err)
			var rest bytes.Buffer
			_, err = r.WriteTo(&rest)
			require.NoError(t, err)
			got := append(bytes.Clone(chunk), prefix[:]...)
			got = append(got, rest.Bytes()...)
			require.Equal(t, value, got)
			require.Equal(t, bytes.Repeat([]byte("v"), len(chunk)), chunk, "later reads leave borrowed chunks untouched")
			require.NoError(t, r.Close())
			for i := 1; i < len(held); i += 2 {
				held[i].Unpin()
			}
			held = nil
			require.Equal(t, st.memory.Capacity(), st.memory.Available())
		})
	}
}

func TestReaderWholeFrameMemoryPressure(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	key := []byte("key")
	h := publishForRead(t, st, key, bytes.Repeat([]byte("v"), 192<<10))
	held := alloc.NewAllocatingBuffer(st.memory)
	defer held.Release()
	require.NoError(t, held.Grow(int(st.memory.Capacity())-(64<<10)))
	available := st.memory.Available()
	r, err := st.NewReader(key, h)
	require.ErrorIs(t, err, alloc.ErrExhausted)
	require.Nil(t, r)
	require.Equal(t, available, st.memory.Available(), "failed growth releases partial allocations")
}

func TestReaderAdmissionFailure(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	key := []byte("key")
	h := publishForRead(t, st, key, bytes.Repeat([]byte("v"), 48000))
	st.schedulers[0] = &countingReadScheduler{Scheduler: st.schedulers[0], rejectAt: 1}
	r, err := st.NewReader(key, h)
	require.ErrorIs(t, err, ErrRejected)
	require.Nil(t, r)
	require.Equal(t, st.memory.Capacity(), st.memory.Available())
}

func TestReaderWholeFrameFitsSmallAllocator(t *testing.T) {
	dir := t.TempDir()
	st := openTestStore(t, dir)
	key, value := []byte("key"), bytes.Repeat([]byte("v"), 96<<10)
	h := publishForRead(t, st, key, value)
	require.NoError(t, st.Close())
	pool := newStoreMemory(t, 128<<10)
	reopened, err := Open(dir, pool, TestingWithDirectIO(false), WithVerifyChecksums(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	r, err := reopened.NewReader(key, h)
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.NoError(t, r.Close())
	require.Equal(t, pool.Capacity(), pool.Available())
}

func TestReaderStaleLocationAndMissingObject(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithTargetSize(64<<10), WithInlineThreshold(64<<10))
	old := publishForRead(t, st, []byte("old"), bytes.Repeat([]byte("v"), (64<<10)-align.BlockSize))
	fresh := publishForRead(t, st, []byte("new"), []byte("value"))
	require.Equal(t, old.Offset, fresh.Offset)
	r, err := st.NewReader([]byte("old"), old)
	require.NoError(t, err)
	_, err = r.Next()
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, r.Close())
	// Full-key comparison still matters if a hash lookup picked the wrong key.
	r, err = st.NewReader([]byte("bad"), fresh)
	require.NoError(t, err)
	_, err = r.Next()
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, r.Close())

	object := putReclaimObject(t, st, "object", bytes.Repeat([]byte("v"), 40000))
	r, err = st.NewReader([]byte("object"), object)
	require.NoError(t, err)
	defer r.Close()
	// The whole frame is available before Next exposes bytes. Unlinking the
	// object afterward cannot affect subsequent reads.
	require.NoError(t, r.load())
	require.NoError(t, os.Remove(filepath.Join(st.base, "objects", strconv.FormatUint(object.ObjectID, 10))))
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("v"), 40000), got)
	require.NoError(t, r.Close())
	r, err = st.NewReader([]byte("object"), object)
	require.NoError(t, err)
	_, err = r.Next()
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, r.Close())
	pool := st.memory
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
			end := h.FrameLen()
			switch mutation {
			case "header":
				_, err = f.WriteAt([]byte{0xff}, 8)
			case "payload":
				_, err = f.WriteAt([]byte{0xff}, h.ValueOffset()+int64(h.PhysicalLen)-1)
			case "truncate":
				err = f.Truncate(end - 1)
			}
			require.NoError(t, err)
			r, err := st.NewReader(key, h)
			require.NoError(t, err)
			defer r.Close()
			chunk, err := r.Next()
			require.Empty(t, chunk, "corrupt records expose no value bytes")
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
	require.NoError(t, os.Truncate(st.objectPath(h.ObjectID), h.FrameLen()))
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
		corrupt := func(name string) {
			offset := int64(RecordHeaderSize - 4)

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
		corrupt("data.db")
		for _, verify := range []bool{false, true} {
			corrupt("index.db") // Close rewrites the snapshot with a valid CRC.
			reopened, err := Open(dir, st.memory, TestingWithDirectIO(false), WithVerifyChecksums(verify))
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
