package blobstore

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/iosched"
	"golang.org/x/sys/unix"
)

func (st *Store) objectPath(id uint64) string {
	return filepath.Join(st.base, "objects", strconv.FormatUint(id, 10))
}

func (st *Store) openObject(id uint64, flags int) iosched.Op {
	return iosched.VOpenatOp(unix.AT_FDCWD, st.objectPath(id),
		flags|st.fileFlags().OpenFlags(), 0o644, iosched.AllocateVFD)
}

// openError reports descriptor exhaustion from an AllocateVFD open as
// ErrNoDescriptors, keeping ENFILE in the chain.
func openError(err error) error {
	if errors.Is(err, unix.ENFILE) {
		return fmt.Errorf("%w: %w", ErrNoDescriptors, err)
	}
	return err
}

// Close is submitted after the writer's last I/O completes. Dio returns the
// slot to its allocator when the close completes.
func (st *Store) closeObject(scheduler iosched.Scheduler, vfd int, id uint64, remove bool) {
	st.cleanup.Add(1)
	complete := func(_ int, err error) {
		if err != nil && !errors.Is(err, unix.EBADF) {
			metrics.CloseErrors.Add(1)
			slog.Error("close object", "object", id, "error", err)
		}
		if remove {
			st.unlinkObject(id)
		}
		st.cleanup.Done()
	}
	if _, err := iosched.SubmitNotify(scheduler, iosched.VCloseOp(uint32(vfd)), complete); err != nil {
		// Nothing was submitted: the descriptor remains open until shutdown.
		metrics.CloseErrors.Add(1)
		slog.Error("submit object close", "object", id, "error", err)
		st.cleanup.Done()
	}
}

func (st *Store) unlinkObject(id uint64) {
	st.cleanup.Add(1)
	if _, err := iosched.SubmitNotify(st.writeScheduler(), iosched.UnlinkatOp(unix.AT_FDCWD, st.objectPath(id)),
		func(_ int, err error) {
			logUnlinkError(id, err)
			st.cleanup.Done()
		}); err != nil {
		logUnlinkError(id, err)
		st.cleanup.Done()
	}
}
