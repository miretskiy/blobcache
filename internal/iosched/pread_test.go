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

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
func TestPreadScheduler_BasicRead(t *testing.T) {
	data := make([]byte, 4096)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "test.dat")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sched := must(iosched.NewPreadScheduler())
	defer sched.Close()

	buf := make([]byte, len(data))
	n, err := sched.ReadAt(int(f.Fd()), buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes, got %d", len(data), n)
	}
	for i := range data {
		if buf[i] != data[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, buf[i], data[i])
		}
	}
}

func TestPreadScheduler_PartialRead(t *testing.T) {
	data := make([]byte, 8192)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "test.dat")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sched := must(iosched.NewPreadScheduler())
	defer sched.Close()

	// Read from offset 4096, 1024 bytes.
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
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestPreadScheduler_Concurrent(t *testing.T) {
	const fileSize = 1 << 20 // 1MB
	const goroutines = 32
	const readsPerGoroutine = 100
	const readSize = 4096

	data := make([]byte, fileSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "test.dat")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sched := must(iosched.NewPreadScheduler())
	defer sched.Close()

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
					errs <- err
					return
				}
				if n != readSize {
					errs <- fmt.Errorf("short read: %d", n)
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

func TestPreadScheduler_EmptyBuf(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.dat")
	if err := os.WriteFile(path, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sched := must(iosched.NewPreadScheduler())
	n, err := sched.ReadAt(int(f.Fd()), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}
}
