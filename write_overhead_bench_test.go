package blobcache

import (
	"os"
	"testing"
)

// Benchmark_MemoryCopy measures pure memory copy speed (baseline)
func Benchmark_MemoryCopy(b *testing.B) {
	src := make([]byte, 1024*1024) // 1MB
	dst := make([]byte, 1024*1024)

	b.SetBytes(1024 * 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		copy(dst, src)
	}
}

// Benchmark_WriteToDevNull measures syscall + userspace→kernel copy overhead
func Benchmark_WriteToDevNull(b *testing.B) {
	f, err := os.OpenFile("/dev/null", os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 1024*1024) // 1MB
	// Fill buffer to prevent zero-page optimization
	for i := range buf {
		buf[i] = byte(i % 256)
	}

	var totalBytes int64
	b.ResetTimer()
	b.SetBytes(1024 * 1024)
	for i := 0; i < b.N; i++ {
		n, err := f.Write(buf)
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(n)
	}
	b.StopTimer()

	// Verify all bytes were "written"
	if totalBytes != int64(b.N)*1024*1024 {
		b.Fatalf("Expected %d bytes, wrote %d", int64(b.N)*1024*1024, totalBytes)
	}
}

// Benchmark_WriteToTempFile measures full buffered write stack
func Benchmark_WriteToTempFile(b *testing.B) {
	// Use /instance_storage on Graviton, /tmp otherwise
	tmpDir := "/tmp"
	if _, err := os.Stat("/instance_storage"); err == nil {
		tmpDir = "/instance_storage"
	}

	tmpFile, err := os.CreateTemp(tmpDir, "write-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	buf := make([]byte, 1024*1024) // 1MB

	b.SetBytes(1024 * 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := tmpFile.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_WriteToTempFileParallel measures parallel writes to separate files
func Benchmark_WriteToTempFileParallel(b *testing.B) {
	// Use /instance_storage on Graviton, /tmp otherwise
	tmpDir := "/tmp"
	if _, err := os.Stat("/instance_storage"); err == nil {
		tmpDir = "/instance_storage"
	}

	b.SetBytes(1024 * 1024)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		tmpFile, err := os.CreateTemp(tmpDir, "write-bench-*")
		if err != nil {
			b.Fatal(err)
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		buf := make([]byte, 1024*1024) // 1MB

		for pb.Next() {
			if _, err := tmpFile.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}
