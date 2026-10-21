package blobstore

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestVirtualFilesWaitForClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := openTestStore(t, t.TempDir())
		scheduler := holdTestIO(t, st)
		w, err := st.NewWriter([]byte("object"), SizeUnknown)
		require.NoError(t, err)
		slot := w.vfd
		for range 1023 {
			st.acquireVFile()
		}
		scheduler.holdNext = true // Submitting close must not release the slot yet.
		go func() { require.NoError(t, w.Close()) }()
		acquired := -1
		go func() { acquired = st.acquireVFile() }()
		synctest.Wait()
		require.Equal(t, -1, acquired, "allocation waits while every slot is occupied")
		scheduler.complete(t, 0)
		synctest.Wait()
		require.Equal(t, slot, acquired, "close completion returns the slot and wakes allocation")
		st.Wait()
		require.NoFileExists(t, st.objectPath(w.header.ObjectID))
	})
}
