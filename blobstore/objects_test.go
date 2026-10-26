package blobstore

import (
	"bytes"
	"io"
	"os"
	"testing"

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
	st.Wait()
	r, err := st.NewReader([]byte("wrong!"), h)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNotFound, "constructor failures must release their descriptor")
	// A reader holds no slot between refills; a writer holding the only slot
	// makes the reader's next refill fail with ErrNoDescriptors.
	r, err = st.NewReader(key, h)
	require.NoError(t, err)
	defer r.Close()
	w, err := st.NewWriter([]byte("busy"), SizeUnknown)
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	_, err = io.ReadAll(r)
	require.ErrorIs(t, err, ErrNoDescriptors)
	require.ErrorIs(t, err, unix.ENFILE)
	require.NoError(t, r.Close())
	r, err = st.NewReader(key, h)
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrNoDescriptors)
	require.NoError(t, w.Close(), "an unpublished writer closes and removes its object")
	st.Wait()
	r, err = st.NewReader(key, h)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.NoError(t, r.Close())
	w, err = st.NewWriter([]byte("reuse"), SizeUnknown)
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	_, err = w.Write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, st.Put(w))
	require.Equal(t, -1, w.vfd, "Put closes the external descriptor")
	require.NoError(t, w.Close())
	st.Wait()
	r, err = st.NewReader([]byte("reuse"), w.Header())
	require.NoError(t, err)
	got, err = io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), got)
	require.NoError(t, r.Close())

	// A failed index write follows a successful close in the final chain.
	// Its writer must not close that slot after another reader has reused it.
	readonly, err := os.Open(st.mu.index.file.Name())
	require.NoError(t, err)
	require.NoError(t, st.mu.index.file.Close())
	st.mu.index.file = readonly
	w, err = st.NewWriter([]byte("failed"), SizeUnknown)
	require.NoError(t, err)
	_, err = w.Write([]byte("unpublished"))
	require.NoError(t, err)
	require.ErrorIs(t, st.Put(w), unix.EBADF)
	require.Equal(t, -1, w.vfd)
	r, err = st.NewReader(key, h)
	require.NoError(t, err)
	defer r.Close()
	require.ErrorIs(t, w.Close(), unix.EBADF)
	st.Wait()
	require.NoFileExists(t, st.objectPath(w.header.ObjectID))
	got, err = io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, value, got)
}
