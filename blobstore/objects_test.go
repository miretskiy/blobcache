package blobstore

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestVirtualFilesSharedByReadersAndWriters(t *testing.T) {
	st := openTestStore(t, t.TempDir(), WithInlineThreshold(0))
	require.NoError(t, st.schedulers[0].Close())
	st.schedulers[0] = iosched.NewDefaultScheduler(iosched.WithVFiles(3), iosched.WithFiles(st.data, st.readData))
	key, value := []byte("object"), bytes.Repeat([]byte("v"), 40000)
	h := publishForRead(t, st, key, value)
	r, err := st.NewReader([]byte("wrong!"), h)
	require.NoError(t, err)
	_, err = r.Next()
	require.ErrorIs(t, err, ErrNotFound, "failed reads must release their descriptor")
	require.NoError(t, r.Close())
	// Completion closes the external descriptor even while the reader keeps
	// its buffer. With one external slot, a subsequent writer must succeed.
	r, err = st.NewReader(key, h)
	require.NoError(t, err)
	require.NoError(t, r.load())
	w, err := st.NewWriter()
	require.NoError(t, err)
	_, err = w.Write(bytes.Repeat([]byte("n"), 40000))
	require.NoError(t, err)
	require.NoError(t, putStoreWriter(st, []byte("next"), w))
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.NoError(t, r.Close())
	w.Discard()

	value = bytes.Repeat([]byte("n"), 40005)
	require.Eventually(t, func() bool {
		w, err = st.NewWriter()
		require.NoError(t, err)
		defer w.Discard()
		_, err = w.Write(value)
		if err != nil {
			return false
		}
		return putStoreWriter(st, []byte("reuse"), w) == nil
	}, time.Second, time.Millisecond)
	require.Nil(t, w.buf, "caller discards the record buffer")
	w.Discard()
	readReclaimRecord(t, st, "reuse", w.header, value)
}
