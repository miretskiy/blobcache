package blobstore

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestVirtualFilesSharedByReadersAndWriters(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("virtual files require io_uring")
	}
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	require.NoError(t, st.schedulers[0].Close())
	scheduler, err := iosched.NewURingScheduler(iosched.WithVFiles(1))
	require.NoError(t, err)
	st.schedulers[0] = scheduler
	key, value := []byte("object"), bytes.Repeat([]byte("v"), 40000)
	h := publishForRead(t, st, key, value)
	r, err := st.NewReader([]byte("wrong!"), h)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound, "constructor failures must release their descriptor")
	// The failed reader's descriptor closes asynchronously.
	require.Eventually(t, func() bool {
		r, err = st.NewReader(key, h)
		return err == nil
	}, time.Second, time.Millisecond)
	// A started reader holds the only slot until Close, so a writer's open
	// fails and the reader still completes.
	w, err := st.NewWriter([]byte("busy"))
	require.NoError(t, err)
	_, err = w.Write(make([]byte, 3*st.cfg.stagingSize))
	require.ErrorIs(t, err, ErrNoDescriptors)
	require.ErrorIs(t, err, unix.ENFILE)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.NoError(t, r.Close())
	require.NoError(t, w.Close())

	value = bytes.Repeat([]byte("n"), 40005)
	require.Eventually(t, func() bool {
		w, err = st.NewWriter([]byte("reuse"))
		require.NoError(t, err)
		defer w.Close()
		_, err = w.Write(value)
		if err != nil {
			return false
		}
		return putStoreWriter(st, w) == nil
	}, time.Second, time.Millisecond)
	require.Nil(t, w.buf, "completion releases the record's buffer")
	require.NoError(t, w.Close())
	readReclaimRecord(t, st, "reuse", w.wr.(*bufferSink).header, value)
}
