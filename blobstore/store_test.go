package blobstore

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
	"github.com/stretchr/testify/require"
)

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) ^ seed ^ byte(i/4096)
	}
	return b
}

// checkFrame compares a record's bytes on the device with what Seal encoded.
func checkFrame(t *testing.T, device *nvme.Device, st *Store, h RecordHeader, key string, value []byte) {
	t.Helper()
	raw := rawRead(t, device, st.geo.offset(h.Position), int(h.frameSize()))
	var decoded RecordHeader
	require.NoError(t, (*RawHeader)(raw).Decode(&decoded, true))
	require.Equal(t, h, decoded, "the disk header is the index header")
	require.Equal(t, key, string(raw[RecordHeaderSize:RecordHeaderSize+len(key)]))
	require.Equal(t, value, raw[recordPrefixSize:recordPrefixSize+len(value)])
	require.Equal(t, make([]byte, len(raw)-int(h.FrameLen())), raw[h.FrameLen():], "padding is zero")
}

func TestStorePutReadReopen(t *testing.T) {
	device := testDevice(t, 64<<20)
	st := openTestStore(t, device)
	sizes := []int{0, 1, recordPrefixSize - RecordHeaderSize - 3, 100 << 10, 3<<20 + 7}
	var headers []RecordHeader
	for i, size := range sizes {
		key, value := fmt.Sprintf("key-%d", i), pattern(size, byte(i))
		h := putValue(t, st, key, value)
		got, err := readValue(st, h)
		require.NoError(t, err)
		require.Equal(t, value, got)
		checkFrame(t, device, st, h, key, value)
		if i > 0 {
			require.Equal(t, headers[i-1].Position+headers[i-1].frameSize(), h.Position, "frames are contiguous")
		}
		headers = append(headers, h)
	}
	require.NoError(t, st.Close())
	require.Len(t, directoryPositions(st), 1, "Close sealed the segment")

	st = openTestStore(t, device)
	require.Equal(t, headers, slices.Collect(st.Records()))
	for i, h := range headers {
		got, err := readValue(st, h)
		require.NoError(t, err)
		require.Equal(t, pattern(sizes[i], byte(i)), got)
	}
	next := putValue(t, st, "after", []byte("reopen"))
	require.Greater(t, next.Position, headers[len(headers)-1].Position, "positions keep growing")
}

func TestRangeReads(t *testing.T) {
	value := pattern(3<<20+7, 9)
	size := int64(len(value))
	ranges := [][2]int64{{0, 0}, {0, 1}, {5, 4096}, {4096, 100}, {1 << 20, 300 << 10}, {size - 10, 100}, {size, 5}, {0, math.MaxInt64}}
	for _, window := range []int64{defaultReadWindow, 64 << 10} {
		st := openTestStore(t, testDevice(t, 16<<20), WithReadWindow(window))
		h := putValue(t, st, "k", value)
		for i, rg := range ranges {
			r, err := st.NewRangeReader(h, rg[0], rg[1])
			require.NoError(t, err)
			want := value[rg[0]:][:min(rg[1], size-rg[0])]
			require.Equal(t, int64(len(want)), r.Size())
			lo := h.ValueOffset() + rg[0]
			require.Equal(t, align.PageAlign(lo+int64(len(want))), r.end, "reads stop at the range's last page")
			if len(want) != 0 {
				require.Equal(t, lo&^(align.BlockSize-1), r.next.lo, "reads start at the range's first page")
				require.LessOrEqual(t, r.next.hi-r.next.lo, window)
			}
			var got bytes.Buffer
			if i%2 == 0 {
				_, err = io.Copy(&got, r) // WriteTo
			} else {
				var b []byte
				b, err = io.ReadAll(r)
				got.Write(b)
			}
			require.NoError(t, err, "range %v window=%d", rg, window)
			require.True(t, bytes.Equal(want, got.Bytes()), "range %v window=%d", rg, window)
			require.NoError(t, r.Close())
		}
		for _, rg := range [][2]int64{{-1, 1}, {0, -1}, {size + 1, 0}} {
			_, err := st.NewRangeReader(h, rg[0], rg[1])
			require.ErrorIs(t, err, ErrInvalidRange)
		}
	}
}

func TestMultiPageFooterRecovers(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device)
	var headers []RecordHeader
	for i := range 100 {
		headers = append(headers, putValue(t, st, fmt.Sprintf("k%d", i), []byte("v")))
	}
	require.NoError(t, st.Close())
	require.Len(t, directoryPositions(st), 1, "one segment lists every record")
	require.Greater(t, footerSize(len(headers)), uint64(align.BlockSize))
	st = openTestStore(t, device)
	require.Equal(t, headers, slices.Collect(st.Records()))
}

func TestLogWrapEvictsOldest(t *testing.T) {
	device := testDevice(t, 16<<20)
	var missed []KeyHash
	st := openTestStore(t, device, WithMissFunc(func(h RecordHeader) { missed = append(missed, h.KeyHash()) }))
	var headers []RecordHeader
	for i := range 40 {
		headers = append(headers, putValue(t, st, fmt.Sprintf("k%d", i), pattern(1<<20, byte(i))))
	}
	require.Equal(t, st.geo.capacity, st.Size())
	live := 0
	var overwritten []KeyHash
	for i, h := range headers {
		got, err := readValue(st, h)
		if !st.Live(h) {
			require.ErrorIs(t, err, ErrNotFound, "record %d was overwritten", i)
			overwritten = append(overwritten, HashKey([]byte(fmt.Sprintf("k%d", i))))
			continue
		}
		live++
		require.NoError(t, err)
		require.Equal(t, pattern(1<<20, byte(i)), got)
	}
	require.Less(t, live, 16)
	require.Greater(t, live, 10)
	require.True(t, st.Live(headers[len(headers)-1]))
	require.Equal(t, overwritten, missed, "each miss reports the key hash once")

	require.NoError(t, st.Close())
	st = openTestStore(t, device)
	recovered := slices.Collect(st.Records())
	require.NotEmpty(t, recovered)
	for _, h := range recovered {
		require.True(t, st.Live(h))
		i := slices.IndexFunc(headers, func(x RecordHeader) bool { return x.Position == h.Position })
		require.NotEqual(t, -1, i)
		got, err := readValue(st, h)
		require.NoError(t, err)
		require.Equal(t, pattern(1<<20, byte(i)), got)
	}
}

func TestSizeSealsSurviveCrash(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device)
	// Segments seal just under 2 MiB: four 516 KiB frames cross it, three do not.
	require.Greater(t, st.segmentBytes, uint64(3*516<<10))
	require.Less(t, st.segmentBytes, uint64(4*516<<10))
	var headers []RecordHeader
	for i := range 5 {
		headers = append(headers, putValue(t, st, fmt.Sprintf("k%d", i), pattern(512<<10, byte(i))))
	}
	// The fourth record crossed 2 MiB and sealed the segment; the fifth is in
	// the active segment.
	waitIdle(st)
	require.Len(t, directoryPositions(st), 1)
	crash(st)

	st = openTestStore(t, device)
	require.Equal(t, headers[:4], slices.Collect(st.Records()), "the unsealed record is forgotten")
	require.GreaterOrEqual(t, st.head.Load(), headers[4].Position, "head resumes after the newest footer")
}

func TestTimedSealSurvivesCrash(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device, WithSealInterval(10*time.Millisecond))
	unit := int(st.geo.unit)
	short := putValue(t, st, "short", pattern(unit/5, 1))
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, directoryPositions(st), "a segment shorter than a unit waits")
	long := putValue(t, st, "long", pattern(unit, 2))
	require.Eventually(t, func() bool { return len(directoryPositions(st)) == 1 }, 5*time.Second, 5*time.Millisecond)
	waitIdle(st)
	crash(st)

	st = openTestStore(t, device)
	require.Equal(t, []RecordHeader{short, long}, slices.Collect(st.Records()))
}

func TestCorruptFooterSkipped(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device)
	var headers []RecordHeader
	for i := range 12 {
		headers = append(headers, putValue(t, st, fmt.Sprintf("k%d", i), pattern(512<<10, byte(i))))
	}
	require.NoError(t, st.Close())
	footers := directoryPositions(st)
	slices.Sort(footers)
	require.Len(t, footers, 3)

	// Destroy the middle footer: its segment's records become misses.
	zero := align.AllocAligned(align.BlockSize)
	defer align.FreeAligned(zero)
	rawIO(t, device, func(d *nvme.Device) iosched.Op { return iosched.NVMeWriteOp(d, zero, st.geo.offset(footers[1])) })
	st = openTestStore(t, device)
	var want []RecordHeader
	for _, h := range headers {
		if h.Position < footers[0] || h.Position > footers[1] {
			want = append(want, h)
		}
	}
	require.Equal(t, want, slices.Collect(st.Records()))
}

func TestDiscardedReservationNotRecovered(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device)
	w, err := st.NewWriter()
	require.NoError(t, err)
	_, err = w.Write([]byte("abandoned"))
	require.NoError(t, err)
	require.NoError(t, w.Seal([]byte("abandoned")))
	w.Discard()
	w.Discard()
	kept := putValue(t, st, "kept", []byte("value"))
	require.NoError(t, st.Close())
	st = openTestStore(t, device)
	require.Equal(t, []RecordHeader{kept}, slices.Collect(st.Records()))
}

func TestConcurrentPutsAcrossSegments(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device)
	var mu sync.Mutex
	written := make(map[string]RecordHeader)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 8 {
				key := fmt.Sprintf("g%d-%d", g, i)
				h := putValue(t, st, key, pattern(96<<10, byte(g*8+i)))
				mu.Lock()
				written[key] = h
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.NoError(t, st.Close())
	require.Greater(t, len(directoryPositions(st)), 4)

	st = openTestStore(t, device)
	recovered := slices.Collect(st.Records())
	require.Len(t, recovered, len(written), "every record was sealed before Close returned")
	for key, h := range written {
		require.Contains(t, recovered, h)
		var g, i int
		_, err := fmt.Sscanf(key, "g%d-%d", &g, &i)
		require.NoError(t, err)
		got, err := readValue(st, h)
		require.NoError(t, err)
		require.Equal(t, pattern(96<<10, byte(g*8+i)), got)
	}
}

func TestOpenRequiresFormat(t *testing.T) {
	device := testDevice(t, 32<<20)
	zero := align.AllocAligned(align.BlockSize)
	defer align.FreeAligned(zero)
	rawIO(t, device, func(d *nvme.Device) iosched.Op { return iosched.NVMeWriteOp(d, zero, 0) })
	_, err := Open(device, newStoreMemory(t, 8<<20))
	require.ErrorIs(t, err, ErrUnformatted)

	require.NoError(t, Format(device))
	smaller, err := nvme.Open(device.Info().CharDevice, nvme.WithRange(device.Offset(), 16<<20))
	require.NoError(t, err)
	defer smaller.Close()
	_, err = Open(smaller, newStoreMemory(t, 8<<20))
	require.ErrorIs(t, err, ErrUnformatted, "a different range size is a different layout")
}

func TestWriterLifecycle(t *testing.T) {
	device := testDevice(t, 16<<20)
	st := openTestStore(t, device)
	held := alloc.NewAllocatingBuffer(st.memory)
	require.NoError(t, held.Grow(int(st.memory.Capacity())))
	_, err := st.NewWriter()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	held.Release()

	w, err := st.NewWriter()
	require.NoError(t, err)
	require.NoError(t, putStoreWriter(st, []byte("key"), w))
	_, err = w.Write([]byte("x"))
	require.ErrorIs(t, err, ErrSealed)
	require.ErrorIs(t, putStoreWriter(st, []byte("key"), w), ErrSealed)
	_, err = st.Put(w, func(error) { t.Fatal("a second Put must not notify") })
	require.ErrorIs(t, err, ErrSealed)
	w.Discard()
	w.Discard()
	_, err = w.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Equal(t, st.memory.Capacity(), st.memory.Available())

	h := RecordHeader{PhysicalLen: st.geo.capacity / 2}
	_, _, err = st.reserve(&h)
	require.ErrorIs(t, err, ErrRecordTooLarge)
}

// waitIdle waits for every sealed footer and directory write to land.
func waitIdle(st *Store) {
	st.mu.Lock()
	for st.mu.outstanding != 0 {
		st.mu.idle.Wait()
	}
	st.mu.Unlock()
}

func TestRegisteredMemory(t *testing.T) {
	device := testDevice(t, 16<<20)
	// Each ring locks the registered memory; stay within an 8 MiB RLIMIT_MEMLOCK.
	st, err := Open(device, newStoreMemory(t, 2<<20), WithSealInterval(0), WithRegisteredMemory())
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	for i, size := range []int{1, 100 << 10, 1 << 20} {
		key, value := fmt.Sprintf("k%d", i), pattern(size, byte(i))
		h := putValue(t, st, key, value)
		got, err := readValue(st, h)
		require.NoError(t, err)
		require.Equal(t, value, got)
	}
}
