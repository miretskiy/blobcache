package blobstore

import (
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
	"github.com/stretchr/testify/require"
)

var testOffset atomic.Uint64

// testDevice formats a fresh size-byte slice of the dedicated test range named
// by BLOBCACHE_NVME_DEVICE, BLOBCACHE_NVME_OFFSET, and BLOBCACHE_NVME_SIZE.
// Tests skip unless BLOBCACHE_NVME_WRITE=1.
func testDevice(t testing.TB, size uint64) *nvme.Device {
	t.Helper()
	if os.Getenv("BLOBCACHE_NVME_WRITE") != "1" {
		t.Skip("set BLOBCACHE_NVME_WRITE=1 and BLOBCACHE_NVME_{DEVICE,OFFSET,SIZE} to a dedicated range")
	}
	env := func(name string) uint64 {
		v, err := strconv.ParseUint(os.Getenv(name), 10, 64)
		require.NoError(t, err, name)
		return v
	}
	base, total := env("BLOBCACHE_NVME_OFFSET"), env("BLOBCACHE_NVME_SIZE")
	start := testOffset.Add(size) - size
	require.LessOrEqual(t, start+size, total, "test range exhausted")
	device, err := nvme.Open(os.Getenv("BLOBCACHE_NVME_DEVICE"), nvme.WithRange(base+start, size))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, device.Close()) })
	if serial := os.Getenv("BLOBCACHE_NVME_SERIAL"); serial != "" {
		require.Equal(t, serial, device.Info().Serial)
	}
	require.NoError(t, Format(device))
	return device
}

func newStoreMemory(t testing.TB, size int64) *alloc.Allocator {
	t.Helper()
	a := alloc.NewAllocator(size)
	t.Cleanup(a.Close)
	return a
}

// openTestStore opens a store without timed seals, so tests decide when
// segments seal.
func openTestStore(t testing.TB, device *nvme.Device, opts ...Option) *Store {
	t.Helper()
	opts = append([]Option{WithSealInterval(0)}, opts...)
	st, err := Open(device, newStoreMemory(t, 8<<20), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func putStoreWriter(st *Store, key []byte, w *Writer) error {
	if err := w.Seal(key); err != nil {
		return err
	}
	ticket, err := st.Put(w, nil)
	if err != nil {
		return err
	}
	return ticket.Wait()
}

// putValue writes and waits for one record, returning its index header.
func putValue(t testing.TB, st *Store, key string, value []byte) RecordHeader {
	t.Helper()
	w, err := st.NewWriter()
	require.NoError(t, err)
	defer w.Discard()
	_, err = w.Write(value)
	require.NoError(t, err)
	require.NoError(t, putStoreWriter(st, []byte(key), w))
	return w.Header()
}

func readValue(st *Store, h RecordHeader) ([]byte, error) {
	r, err := st.NewReader(h)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// crash stops the store as a process exit would: no final seal and no wait
// for footers. In-flight I/O still completes inside the schedulers.
func crash(st *Store) {
	if st.stopSeal != nil {
		close(st.stopSeal)
		<-st.sealDone
		st.stopSeal = nil
	}
	for _, scheduler := range st.schedulers {
		_ = scheduler.Close()
	}
	st.schedulers = nil
	_ = st.device.Close()
	st.device = nil
}

// rawIO reads or writes device bytes outside the store.
func rawIO(t testing.TB, device *nvme.Device, op func(*nvme.Device) iosched.Op) {
	t.Helper()
	scheduler, err := iosched.NewURingScheduler(iosched.WithNVMeCommands())
	require.NoError(t, err)
	defer func() { require.NoError(t, scheduler.Close()) }()
	require.NoError(t, waitAll(scheduler, []iosched.Op{op(device)}))
}

func rawRead(t testing.TB, device *nvme.Device, offset int64, n int) []byte {
	buf := align.AllocAligned(align.PageAlign(int64(n)))
	t.Cleanup(func() { align.FreeAligned(buf) })
	rawIO(t, device, func(d *nvme.Device) iosched.Op { return iosched.NVMeReadOp(d, buf, offset) })
	return buf[:n]
}

// directoryPositions lists the footer positions the store's directory names.
func directoryPositions(st *Store) []uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	var positions []uint64
	for _, position := range st.mu.directory {
		if position != 0 {
			positions = append(positions, position)
		}
	}
	return positions
}
