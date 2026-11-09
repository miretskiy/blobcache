package blobstore

import (
	"errors"
	"fmt"
	"log/slog"
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

func (st *Store) closeObject(scheduler iosched.Scheduler, vfd int, id uint64) {
	logError := func(_ int, err error) {
		if err != nil && !errors.Is(err, unix.EBADF) {
			metrics.CloseErrors.Add(1)
			slog.Error("close object", "object", id, "error", err)
		}
	}
	if _, err := iosched.SubmitNotify(scheduler, iosched.VCloseOp(uint32(vfd)), logError); err != nil {
		logError(0, err) // The descriptor remains open until shutdown.
	}
}

// removeOrphans deletes object files the index does not reference: a crash
// loses records since the last snapshot, and shutdown can cut removals short.
func (st *Store) removeOrphans() error {
	dir := filepath.Join(st.base, "objects")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	live := make(map[uint64]bool)
	for h := range st.mu.index.headers() {
		if h.Flags.IsExternal() {
			live[h.ObjectID] = true
		}
	}
	for _, entry := range entries {
		if id, err := strconv.ParseUint(entry.Name(), 10, 64); err == nil && !live[id] {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				metrics.UnlinkErrors.Add(1)
				slog.Error("unlink object", "object", id, "error", err)
			}
		}
	}
	return nil
}

func (st *Store) unlinkObject(scheduler iosched.Scheduler, id uint64) {
	logError := func(_ int, err error) {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			metrics.UnlinkErrors.Add(1)
			slog.Error("unlink object", "object", id, "error", err)
		}
	}
	if _, err := iosched.SubmitNotify(scheduler, iosched.UnlinkatOp(unix.AT_FDCWD, st.objectPath(id)), logError); err != nil {
		logError(0, err)
	}
}
