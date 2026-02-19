//go:build linux

package iosched_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/miretskiy/blobcache/internal/iosched"
)

func TestURingScheduler_Available(t *testing.T) {
	// On Linux CI, io_uring should be available (kernel 5.1+).
	// If not, the rest of the tests will be skipped.
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
}

func newURingSched(t *testing.T) *iosched.URingScheduler {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{
		RingDepth: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

func writeTestFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "test.dat")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func TestURingScheduler_BasicRead(t *testing.T) {
	sched := newURingSched(t)

	path, data := writeTestFile(t, 4096)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 4096)
	n, err := sched.ReadAt(int(f.Fd()), buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatalf("expected 4096 bytes, got %d", n)
	}
	for i := range data {
		if buf[i] != data[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestURingScheduler_PartialRead(t *testing.T) {
	sched := newURingSched(t)

	path, data := writeTestFile(t, 8192)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 1024)
	n, err := sched.ReadAt(int(f.Fd()), buf, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", n)
	}
	for i := range buf {
		if buf[i] != data[4096+i] {
			t.Fatalf("mismatch at offset 4096+%d", i)
		}
	}
}

func TestURingScheduler_EmptyBuf(t *testing.T) {
	sched := newURingSched(t)

	n, err := sched.ReadAt(0, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}
}

func TestURingScheduler_Concurrent(t *testing.T) {
	const fileSize = 1 << 20 // 1MB
	const goroutines = 64
	const readsPerGoroutine = 50
	const readSize = 4096

	sched := newURingSched(t)

	path, data := writeTestFile(t, fileSize)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readSize)
			for i := 0; i < readsPerGoroutine; i++ {
				offset := int64((i * readSize) % (fileSize - readSize))
				n, err := sched.ReadAt(int(f.Fd()), buf, offset)
				if err != nil {
					errs <- fmt.Errorf("read at offset %d: %w", offset, err)
					return
				}
				if n != readSize {
					errs <- fmt.Errorf("short read at offset %d: %d", offset, n)
					return
				}
				for j := range buf {
					if buf[j] != data[offset+int64(j)] {
						errs <- fmt.Errorf("mismatch at offset %d+%d", offset, j)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestURingScheduler_LargeRead(t *testing.T) {
	sched := newURingSched(t)

	// 1MB read — typical blob size.
	const size = 1 << 20
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, size)
	n, err := sched.ReadAt(int(f.Fd()), buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != size {
		t.Fatalf("expected %d bytes, got %d", size, n)
	}
	for i := range data {
		if buf[i] != data[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestURingScheduler_CloseThenRead(t *testing.T) {
	sched := newURingSched(t)

	path, _ := writeTestFile(t, 4096)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Close the scheduler, then attempt a read.
	if err := sched.Close(); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 4096)
	_, err = sched.ReadAt(int(f.Fd()), buf, 0)
	if err == nil {
		t.Fatal("expected error after close")
	}
}

func TestURingScheduler_Interface(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var _ iosched.IOScheduler = s
	s.Close()
}

func TestURingScheduler_SQPOLL(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}

	// SQPOLL may require root or CAP_SYS_NICE on some kernels.
	s, err := iosched.NewURingScheduler(iosched.URingConfig{
		RingDepth: 64,
		SQPOLL:    true,
	})
	if err != nil {
		t.Skipf("SQPOLL not available: %v", err)
	}
	defer s.Close()

	path, data := writeTestFile(t, 4096)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 4096)
	n, err := s.ReadAt(int(f.Fd()), buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatalf("expected 4096, got %d", n)
	}
	for i := range data {
		if buf[i] != data[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}
