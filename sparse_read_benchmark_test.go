package blobcache

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/sys"
)

// BenchmarkSparseFileReadLatency measures read throughput and latency from
// segment files with varying levels of sparseness (hole-punched regions).
//
// Uses O_DIRECT to bypass page cache entirely, ensuring every read hits NVMe.
// This isolates whether filesystem extent tree fragmentation from hole punching
// degrades read performance, independent of the hole-punching syscalls themselves.
//
// Run on Linux/XFS for meaningful results:
//
//	go test -bench=BenchmarkSparseFileReadLatency -benchtime=10000x -v
func BenchmarkSparseFileReadLatency(b *testing.B) {
	const (
		fileSize = 64 << 20 // 64MB segment
		blobSize = 1 << 20  // 1MB reads (typical blob size)
	)

	// Use /instance_storage for XFS NVMe on the remote workspace.
	tmpDir := "/instance_storage/sparse_bench"
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		tmpDir = b.TempDir() // fallback for local dev
	} else {
		b.Cleanup(func() { os.RemoveAll(tmpDir) })
	}

	// Create the dense source file filled with random data.
	srcPath := filepath.Join(tmpDir, "dense.seg")
	createDenseFile(b, srcPath, fileSize)

	levels := []struct {
		name       string
		holeFrac   float64
		holeSize   int64
		holeStride int64
	}{
		{"dense_0pct", 0.0, 0, 0},
		{"sparse_25pct", 0.25, blobSize, 4 * blobSize},
		{"sparse_50pct", 0.50, blobSize, 2 * blobSize},
		{"sparse_75pct", 0.75, 3 * blobSize, 4 * blobSize},
	}

	for _, level := range levels {
		b.Run(level.name, func(b *testing.B) {
			path := filepath.Join(tmpDir, level.name+".seg")
			copyFile(b, srcPath, path)

			if level.holeFrac > 0 {
				punchHoles(b, path, fileSize, level.holeSize, level.holeStride)
			}

			// Verify actual sparseness via stat block count.
			var st syscall.Stat_t
			if err := syscall.Stat(path, &st); err != nil {
				b.Fatal(err)
			}
			totalBlocks := int64(fileSize / 512)
			b.Logf("%s: physical blocks: %d/%d (%.1f%% sparse)",
				level.name, st.Blocks, totalBlocks,
				100.0*(1.0-float64(st.Blocks)/float64(totalBlocks)))

			// Pre-compute read offsets into data (non-hole) regions.
			// All offsets are 1MB-aligned (page-aligned), safe for O_DIRECT.
			offsets := computeReadOffsets(fileSize, blobSize, level.holeSize, level.holeStride, level.holeFrac)
			if len(offsets) == 0 {
				b.Skip("no readable offsets")
			}

			// Open with O_DIRECT to bypass page cache entirely.
			f, err := sys.OpenDirect(path, sys.FlDirectIO)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			// Aligned buffer for O_DIRECT reads.
			buf := align.AllocAligned(blobSize)
			defer align.FreeAligned(buf)

			b.SetBytes(blobSize)
			b.ResetTimer()

			var totalLatency time.Duration
			for i := range b.N {
				off := offsets[i%len(offsets)]
				start := time.Now()
				n, err := f.ReadAt(buf, off)
				elapsed := time.Since(start)
				if err != nil {
					b.Fatalf("read at offset %d: %v", off, err)
				}
				if n != blobSize {
					b.Fatalf("short read: %d/%d at offset %d", n, blobSize, off)
				}
				totalLatency += elapsed
			}

			b.StopTimer()
			avgLatency := totalLatency / time.Duration(b.N)
			throughputMBs := float64(b.N) * float64(blobSize) / (1 << 20) / totalLatency.Seconds()
			b.ReportMetric(float64(avgLatency.Microseconds()), "avg_us/read")
			b.ReportMetric(throughputMBs, "MB/s")
		})
	}
}

func createDenseFile(b *testing.B, path string, size int64) {
	b.Helper()
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}()

	chunk := make([]byte, 1<<20)
	for written := int64(0); written < size; {
		if _, err := rand.Read(chunk); err != nil {
			b.Fatal(err)
		}
		n := min(int64(len(chunk)), size-written)
		if _, err := f.Write(chunk[:n]); err != nil {
			b.Fatal(err)
		}
		written += n
	}
}

func copyFile(b *testing.B, src, dst string) {
	b.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		b.Fatal(err)
	}
}

func punchHoles(b *testing.B, path string, fileSize, holeSize, holeStride int64) {
	b.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}()

	var punchCount int
	for off := int64(0); off+holeSize <= fileSize; off += holeStride {
		if _, err := sys.PunchHole(f, off, holeSize); err != nil {
			b.Fatalf("punch hole at offset %d: %v", off, err)
		}
		punchCount++
	}
	b.Logf("punched %d holes of %dKB every %dKB", punchCount, holeSize>>10, holeStride>>10)
}

// computeReadOffsets returns offsets into data (non-hole) regions.
// All returned offsets are multiples of blobSize (1MB), safe for O_DIRECT.
// BenchmarkDirectIOReadSize measures raw Direct I/O read latency at different
// read sizes (4KB, 8KB, 16KB, 32KB, 64KB) on a dense data file.
//
// This isolates the NVMe I/O cost from any application-level overhead,
// answering: "how much does a 64KB read cost vs a 4KB read?"
//
// Run on Linux/NVMe for meaningful results:
//
//	go test -bench=BenchmarkDirectIOReadSize -benchtime=10000x -v
func BenchmarkDirectIOReadSize(b *testing.B) {
	const fileSize = 64 << 20 // 64MB dense file

	tmpDir := "/instance_storage/dio_bench"
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		tmpDir = b.TempDir()
	} else {
		b.Cleanup(func() { os.RemoveAll(tmpDir) })
	}

	srcPath := filepath.Join(tmpDir, "dense.seg")
	createDenseFile(b, srcPath, fileSize)

	sizes := []struct {
		name string
		size int
	}{
		{"4KB", 4 << 10},
		{"8KB", 8 << 10},
		{"16KB", 16 << 10},
		{"32KB", 32 << 10},
		{"64KB", 64 << 10},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			f, err := sys.OpenDirect(srcPath, sys.FlDirectIO)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			buf := align.AllocAligned(sz.size)
			defer align.FreeAligned(buf)

			// Pre-compute page-aligned offsets that fit the read size.
			var offsets []int64
			for off := int64(0); off+int64(sz.size) <= fileSize; off += int64(sz.size) {
				offsets = append(offsets, off)
			}

			b.SetBytes(int64(sz.size))
			b.ResetTimer()

			var totalLatency time.Duration
			for i := range b.N {
				off := offsets[i%len(offsets)]
				start := time.Now()
				n, err := f.ReadAt(buf, off)
				elapsed := time.Since(start)
				if err != nil {
					b.Fatalf("read %d bytes at offset %d: %v", sz.size, off, err)
				}
				if n != sz.size {
					b.Fatalf("short read: %d/%d", n, sz.size)
				}
				totalLatency += elapsed
			}

			b.StopTimer()
			avgLatency := totalLatency / time.Duration(b.N)
			throughputMBs := float64(b.N) * float64(sz.size) / (1 << 20) / totalLatency.Seconds()
			b.ReportMetric(float64(avgLatency.Microseconds()), "avg_us/read")
			b.ReportMetric(throughputMBs, "MB/s")
		})
	}
}

func computeReadOffsets(fileSize, blobSize, holeSize, holeStride int64, holeFrac float64) []int64 {
	if holeFrac == 0 {
		var offsets []int64
		for off := int64(0); off+blobSize <= fileSize; off += blobSize {
			offsets = append(offsets, off)
		}
		return offsets
	}

	var offsets []int64
	for strideStart := int64(0); strideStart+holeStride <= fileSize; strideStart += holeStride {
		dataStart := strideStart + holeSize
		for off := dataStart; off+blobSize <= strideStart+holeStride; off += blobSize {
			offsets = append(offsets, off)
		}
	}
	return offsets
}
