package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncw/directio"
)

// Benchmark_IO_ManySmall writes many 1MB files (1MB write, 1MB file)
func Benchmark_IO_ManySmall(b *testing.B) {
	benchIO(b, "ManySmall", 1024*1024, 1024*1024, false)
}

// Benchmark_IO_ManySmall_DirectIO writes many 1MB files using direct I/O
func Benchmark_IO_ManySmall_DirectIO(b *testing.B) {
	benchIO(b, "ManySmall_DirectIO", 1024*1024, 1024*1024, true)
}

// Benchmark_IO_FewLarge_1MB writes 1GB files using 1MB Write() calls
func Benchmark_IO_FewLarge_1MB(b *testing.B) {
	benchIO(b, "FewLarge_1MB", 1024*1024, 1024*1024*1024, false)
}

// Benchmark_IO_FewLarge_1MB_DirectIO writes 1GB files using 1MB Write() calls with direct I/O
func Benchmark_IO_FewLarge_1MB_DirectIO(b *testing.B) {
	benchIO(b, "FewLarge_1MB_DirectIO", 1024*1024, 1024*1024*1024, true)
}

// Benchmark_IO_FewLarge_512MB writes 1GB files using 512MB Write() calls
func Benchmark_IO_FewLarge_512MB(b *testing.B) {
	benchIO(b, "FewLarge_512MB", 512*1024*1024, 1024*1024*1024, false)
}

// Benchmark_IO_FewLarge_1GB writes 1GB files using single 1GB Write() call
func Benchmark_IO_FewLarge_1GB(b *testing.B) {
	benchIO(b, "FewLarge_1GB", 1024*1024*1024, 1024*1024*1024, false)
}

// Benchmark_IO_FewLarge_1GB_DirectIO writes 1GB files using single 1GB Write() call with direct I/O
func Benchmark_IO_FewLarge_1GB_DirectIO(b *testing.B) {
	benchIO(b, "FewLarge_1GB_DirectIO", 1024*1024, 1024*1024*1024, true)
}

// benchIO writes data using specified pattern
// Uses b.RunParallel for concurrent workers
// writeSize: size of each Write() call
// fileSize: total size of each file
// useDirectIO: if true, use directio for unbuffered I/O
func benchIO(b *testing.B, name string, writeSize, fileSize int64, useDirectIO bool) {
	baseDir := os.Getenv("BENCH_IO_DIR")
	if baseDir == "" {
		baseDir = "/tmp"
	}
	tmpDir := fmt.Sprintf("%s/bench-io-%s", baseDir, name)
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)

	// Allocate buffer - directio.AlignedBlock handles alignment automatically
	var data []byte
	if useDirectIO {
		data = directio.AlignedBlock(int(writeSize))
	} else {
		data = make([]byte, writeSize)
	}
	// Fill with pattern
	for i := range data {
		data[i] = byte(i % 256)
	}

	writesPerFile := int(fileSize / writeSize)
	var totalBytes atomic.Int64
	start := time.Now()

	b.ResetTimer()
	var fileCounter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fileNum := fileCounter.Add(1)
			filename := filepath.Join(tmpDir, fmt.Sprintf("file-%d.dat", fileNum))

			var f *os.File
			var err error
			if useDirectIO {
				f, err = directio.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
			} else {
				f, err = os.Create(filename)
			}
			if err != nil {
				continue
			}

			for writeNum := 0; writeNum < writesPerFile; writeNum++ {
				n, _ := f.Write(data)
				totalBytes.Add(int64(n))
			}
			_ = f.Close()
		}
	})
	b.StopTimer()

	elapsed := time.Since(start)
	bytes := totalBytes.Load()
	throughput := float64(bytes) / elapsed.Seconds() / (1024 * 1024 * 1024)
	b.ReportMetric(throughput, "GB/s")
}
