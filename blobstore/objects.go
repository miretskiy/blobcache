package blobstore

import (
	"errors"
	"log/slog"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/iosched"
	"golang.org/x/sys/unix"
)

func (st *Store) objectPath(id uint64) string {
	return filepath.Join(st.base, "objects", strconv.FormatUint(id, 10))
}

// A slot belongs to one writer until its close completes. Only exhaustion of
// all 1024 slots waits; close completion wakes a waiting allocator.
func (st *Store) acquireVFile() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	for {
		for i, occupied := range st.mu.vfiles {
			if free := bits.TrailingZeros64(^occupied); free != 64 {
				st.mu.vfiles[i] |= 1 << free
				return i*64 + free
			}
		}
		st.vfilesAvailable.Wait()
	}
}

func (st *Store) openObject(id uint64) (int, error) {
	vfd := st.acquireVFile()
	_, err := st.scheduler.Submit(iosched.VOpenatOp(unix.AT_FDCWD, st.objectPath(id),
		os.O_CREATE|os.O_TRUNC|os.O_RDWR|st.fileFlags().OpenFlags(), 0o644, uint32(vfd)))
	if err != nil {
		st.releaseVFile(vfd)
		return -1, err
	}
	// Later writes are ordered after this open. An open failure leaves the slot
	// empty, so those writes fail and cannot publish the record.
	return vfd, nil
}

func (st *Store) releaseVFile(vfd int) {
	st.mu.Lock()
	st.mu.vfiles[vfd/64] &^= 1 << (vfd % 64)
	st.vfilesAvailable.Signal()
	st.mu.Unlock()
}

// Close is submitted after the writer's last I/O. A failed open can leave an
// empty slot (EBADF); other close failures keep the slot occupied.
func (st *Store) closeObject(vfd int, id uint64, remove bool) {
	st.cleanup.Add(1)
	complete := func(_ int, err error) {
		if err == nil || errors.Is(err, unix.EBADF) {
			st.releaseVFile(vfd)
		} else {
			metrics.CloseErrors.Add(1)
			slog.Error("close object", "object", id, "error", err)
		}
		if remove {
			_, err := iosched.SubmitNotify(st.scheduler, iosched.UnlinkatOp(unix.AT_FDCWD, st.objectPath(id)),
				func(_ int, err error) {
					logUnlinkError(id, err)
					st.cleanup.Done()
				})
			if err == nil {
				return
			}
			logUnlinkError(id, err)
		}
		st.cleanup.Done()
	}
	if _, err := iosched.SubmitNotify(st.scheduler, iosched.VCloseOp(uint32(vfd)), complete); err != nil {
		// Nothing was submitted: retain the slot until the scheduler is closed.
		metrics.CloseErrors.Add(1)
		slog.Error("submit object close", "object", id, "error", err)
		st.cleanup.Done()
	}
}
