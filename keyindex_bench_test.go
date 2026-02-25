package blobcache

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// BenchmarkIterator measures real iterator throughput over persisted data.
//
// Each benchmark iteration (-benchtime=Nx) writes ~1MB to populate the cache.
// After populate → drain → close, each scan variant reopens the cache fresh
// (Librarian empty, page cache cold) and measures iteration with View() on
// every key.
//
// Semantics:
//
//	-benchtime=Nx means "write N megabytes of data" (~1MB per iteration)
//	-benchtime=10000x    →  ~10GB
//	-benchtime=100000x   →  ~100GB
//	-benchtime=1000000x  →  ~1TB
//
// Dimensions (all combinations):
//
//	Scope:       full (all keys) | subset (random non-overlapping ranges)
//	I/O mode:    buffered | directio
//	Parallelism: P=1, 2, 4, 8, 16, 32
//
// Benchmark names:
//
//	BenchmarkIterator/full/buffered/P=1
//	BenchmarkIterator/full/directio/P=8
//	BenchmarkIterator/subset/buffered/P=16
//	BenchmarkIterator/subset/directio/P=32
//
// Run:
//
//	go test -bench=BenchmarkIterator -benchtime=10000x -v -timeout=30m
//
// Filter variants with ITER_FILTER (substring match on variant name):
//
//	ITER_FILTER=directio go test -bench=BenchmarkIterator -benchtime=100000x -v -timeout=60m
//	ITER_FILTER=full/directio go test -bench=BenchmarkIterator -benchtime=100000x -v -timeout=60m
//	ITER_FILTER=subset go test -bench=BenchmarkIterator -benchtime=100000x -v -timeout=60m
//
// On Linux remote:
//
//	BENCH_DIR=/instance_storage/iter_bench go test -bench=BenchmarkIterator -benchtime=100000x -v -timeout=120m
//
// Monitor during run:
//
//	iostat -d -m -x nvme1n1 5
func BenchmarkIterator(b *testing.B) {
	dir := benchDir(b)

	const (
		blobSizeLo    = 100_000
		blobSizeHiRng = 1_900_000 // [100KB, 2MB] uniform
	)

	numKeys := b.N
	maxSizeBytes := int64(numKeys) * (1 << 20) * 2 // 2× populate size

	// --- Phase 1: Populate ---
	fmt.Printf(">>> Populate: Writing %d entries (~%d GB)...\n", numKeys, numKeys>>10)

	entropy := make([]byte, 32<<20) // 32MB entropy pool
	crand.Read(entropy)
	rng := rand.New(rand.NewPCG(42, 0))

	var totalPopulateBytes int64
	populateStart := time.Now()
	{
		cache, err := New(dir,
			WithMaxSize(maxSizeBytes),
			WithWriteBufferSize(64<<20),
			WithMaxInflightSlabs(8),
			WithMaxCachedSlabs(4),
			WithFlushConcurrency(2),
			WithBallast(0),
			WithDegradedMode(DegradedPanic),
		)
		if err != nil {
			b.Fatal(err)
		}
		cache.Start()

		keyBuf := make([]byte, 64)
		for i := range numKeys {
			k := iterKey(keyBuf, i)
			blobSize := blobSizeLo + rng.IntN(blobSizeHiRng)
			offset := rng.IntN(len(entropy) - blobSize)
			if err := cache.Put(k, entropy[offset:offset+blobSize]); err != nil {
				b.Fatal(err)
			}
			totalPopulateBytes += int64(blobSize)
		}
		cache.Drain()
		if err := cache.Close(); err != nil {
			b.Fatal(err)
		}
	}
	populateElapsed := time.Since(populateStart)
	fmt.Printf(">>> Populate complete: %d keys, %.1f GB in %v (%.1f GB/s)\n",
		numKeys, float64(totalPopulateBytes)/(1<<30),
		populateElapsed.Round(time.Millisecond),
		float64(totalPopulateBytes)/(1<<30)/populateElapsed.Seconds())

	// --- Phase 2: Scan matrix ---
	type scanConfig struct {
		scope    string // "full" or "subset"
		ioMode   string // "buffered" or "directio"
		parallel int
	}

	var configs []scanConfig
	for _, scope := range []string{"full", "subset"} {
		for _, ioMode := range []string{"buffered", "directio"} {
			for _, p := range []int{1, 2, 4, 8, 16, 32} {
				configs = append(configs, scanConfig{scope, ioMode, p})
			}
		}
	}

	iterFilter := os.Getenv("ITER_FILTER")
	for _, tc := range configs {
		name := fmt.Sprintf("%s/%s/P=%d", tc.scope, tc.ioMode, tc.parallel)
		if iterFilter != "" && !strings.Contains(name, iterFilter) {
			continue
		}
		directIO := tc.ioMode == "directio"
		fmt.Printf("\n=== ITER: %s ===\n", name)
		runIterScan(b, dir, maxSizeBytes, numKeys, directIO, tc.scope == "subset", tc.parallel)
	}
}

// runIterScan opens the cache fresh from disk, creates P iterators, and
// measures the throughput of scanning with View() on every key.
// Each invocation reopens the cache to ensure an empty Librarian.
func runIterScan(
	b *testing.B,
	dir string,
	maxSize int64,
	numKeys int,
	directIO bool,
	subset bool,
	parallel int,
) {
	b.Helper()

	sched, err := newBenchScheduler()
	if err != nil {
		b.Fatal(err)
	}
	defer sched.Close()

	cache, err := New(dir,
		WithMaxSize(maxSize),
		WithWriteBufferSize(64<<20),
		WithMaxInflightSlabs(4),
		WithMaxCachedSlabs(2),
		WithBallast(0),
		WithIOScheduler(sched),
		WithDirectIORead(directIO),
		WithDegradedMode(DegradedPanic),
	)
	if err != nil {
		b.Fatal(err)
	}
	cache.Start()
	defer func() {
		if err := cache.Close(); err != nil {
			b.Logf("close: %v", err)
		}
	}()

	// Compute per-iterator bounds.
	ranges := make([]iterRange, parallel)
	if subset {
		// Assign each iterator a random non-overlapping slice of the keyspace.
		// Shuffle key indices, then partition into P equal chunks.
		// Each chunk becomes a contiguous range in the sorted keyspace.
		chunkSize := max(numKeys/parallel, 1)
		// Generate P random start offsets (non-overlapping chunks).
		rng := rand.New(rand.NewPCG(99, 0))
		starts := make([]int, parallel)
		for i := range parallel {
			starts[i] = rng.IntN(numKeys)
		}
		sort.Ints(starts)
		// Deduplicate and ensure non-overlapping by spacing chunks.
		for i := 1; i < len(starts); i++ {
			if starts[i] < starts[i-1]+chunkSize {
				starts[i] = starts[i-1] + chunkSize
			}
		}

		buf := make([]byte, 64)
		for i := range parallel {
			lo := starts[i]
			hi := min(lo+chunkSize, numKeys)
			if lo >= numKeys {
				// Wrap around — this iterator scans from near the start.
				lo = 0
				hi = chunkSize
			}
			lower := make([]byte, len(iterKey(buf, lo)))
			copy(lower, iterKey(buf, lo))
			upper := make([]byte, len(iterKey(buf, hi)))
			copy(upper, iterKey(buf, hi))
			ranges[i] = iterRange{lower: lower, upper: upper}
		}
	}
	// For full scan, ranges stay nil,nil.

	if parallel <= 1 {
		runSingleIterScan(b, cache, ranges[0].lower, ranges[0].upper)
	} else {
		runConcurrentIterScan(b, cache, ranges, parallel)
	}
}

func runSingleIterScan(b *testing.B, cache *Cache, lower, upper []byte) {
	b.Helper()

	hist := hdrhistogram.New(100, 10_000_000_000, 3) // 100ns – 10s

	var totalKeys int64
	var totalBytes int64

	// Progress heartbeat.
	var keysScanned, bytesScanned atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scanStart := time.Now()
	go iterHeartbeat(ctx, scanStart, &keysScanned, &bytesScanned)

	const scans = 3
	for scan := range scans {
		iter, err := cache.NewIterator(lower, upper)
		if err != nil {
			b.Fatal(err)
		}

		var scanKeys int64
		for ok := iter.First(); ok; ok = iter.Next() {
			start := time.Now()
			iter.View(func(data []byte) {
				n := int64(len(data))
				totalBytes += n
				bytesScanned.Add(n)
			})
			elapsed := time.Since(start).Nanoseconds()
			if elapsed >= 100 { // HDR min is 100ns
				hist.RecordValue(elapsed)
			}
			totalKeys++
			scanKeys++
			keysScanned.Add(1)
		}
		if err := iter.Error(); err != nil {
			b.Fatalf("scan %d: %v", scan, err)
		}
		if err := iter.Close(); err != nil {
			b.Fatalf("close iter %d: %v", scan, err)
		}

		scanElapsed := time.Since(scanStart)
		fmt.Printf("  scan %d/%d: %d keys, %.1f GB, %v\n",
			scan+1, scans, scanKeys,
			float64(totalBytes)/(1<<30),
			scanElapsed.Round(time.Millisecond))
	}
	cancel()

	elapsed := time.Since(scanStart)
	keysPerSec := float64(totalKeys) / elapsed.Seconds()
	mbPerSec := float64(totalBytes) / elapsed.Seconds() / (1 << 20)
	nsPerKey := float64(elapsed.Nanoseconds()) / float64(totalKeys)
	keysPerScan := totalKeys / scans

	fmt.Printf("  --- RESULT ---\n")
	fmt.Printf("  %d scans | %d keys/scan | %.0f keys/sec (≈ IOPS) | %.0f MB/s | %.0f ns/key\n",
		scans, keysPerScan, keysPerSec, mbPerSec, nsPerKey)
	fmt.Printf("  Total: %d keys, %.1f GB in %v\n",
		totalKeys, float64(totalBytes)/(1<<30), elapsed.Round(time.Millisecond))
	reportLatency(b, "ITER-VIEW", hist)
}

func runConcurrentIterScan(b *testing.B, cache *Cache, ranges []iterRange, concurrent int) {
	b.Helper()

	var totalKeys, totalBytes atomic.Int64
	var scanErrors atomic.Int64

	scanStart := time.Now()

	// Progress heartbeat.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go iterHeartbeat(ctx, scanStart, &totalKeys, &totalBytes)

	const scansPerIter = 1 // 1 scan per goroutine to keep runtime bounded.
	var wg sync.WaitGroup
	for g := range concurrent {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := ranges[id]
			for range scansPerIter {
				iter, err := cache.NewIterator(r.lower, r.upper)
				if err != nil {
					scanErrors.Add(1)
					return
				}
				for ok := iter.First(); ok; ok = iter.Next() {
					iter.View(func(data []byte) {
						totalBytes.Add(int64(len(data)))
					})
					totalKeys.Add(1)
				}
				if err := iter.Error(); err != nil {
					scanErrors.Add(1)
				}
				if err := iter.Close(); err != nil {
					scanErrors.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	cancel()

	if errs := scanErrors.Load(); errs > 0 {
		b.Fatalf("%d scan errors", errs)
	}

	elapsed := time.Since(scanStart)
	aggKeysPerSec := float64(totalKeys.Load()) / elapsed.Seconds()
	aggMBPerSec := float64(totalBytes.Load()) / elapsed.Seconds() / (1 << 20)
	perIterKeysPerSec := aggKeysPerSec / float64(concurrent)
	totalScans := concurrent * scansPerIter

	fmt.Printf("  --- RESULT ---\n")
	fmt.Printf("  %d iterators × %d scan/iter (%d total) | %.0f agg keys/sec | %.0f per-iter keys/sec | %.0f agg MB/s\n",
		concurrent, scansPerIter, totalScans, aggKeysPerSec, perIterKeysPerSec, aggMBPerSec)
	fmt.Printf("  Total: %d keys, %.1f GB in %v\n",
		totalKeys.Load(), float64(totalBytes.Load())/(1<<30), elapsed.Round(time.Millisecond))
}

// iterHeartbeat prints scan progress every 10 seconds.
func iterHeartbeat(ctx context.Context, start time.Time, keys, bytes *atomic.Int64) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			k := keys.Load()
			mb := float64(bytes.Load()) / (1 << 20)
			elapsed := time.Since(start).Seconds()
			fmt.Printf("  [heartbeat] %dk keys | %.0f MB | %.0f keys/sec | %.0f MB/s\n",
				k/1000, mb, float64(k)/elapsed, mb/elapsed)
		}
	}
}

// iterKey formats a zero-padded key for deterministic lexicographic ordering.
// Keys sort in numeric order: key-0000000000 < key-0000000001 < ... < key-0000099999.
func iterKey(buf []byte, id int) []byte {
	return fmt.Appendf(buf[:0], "key-%010d", id)
}

// benchDir returns a directory for benchmark data.
// Uses BENCH_DIR env var if set (for /instance_storage on remote),
// otherwise falls back to b.TempDir().
// Any pre-existing data in the directory is removed on entry to ensure
// a clean state (prevents stale data from crashed or previous runs).
func benchDir(b *testing.B) string {
	b.Helper()
	if dir := os.Getenv("BENCH_DIR"); dir != "" {
		path := filepath.Join(dir, b.Name())
		os.RemoveAll(path) // Clean stale data from previous/crashed runs.
		if err := os.MkdirAll(path, 0o755); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(path) })
		return path
	}
	return b.TempDir()
}

// iterRange defines bounds for a single iterator.
type iterRange struct {
	lower, upper []byte // nil,nil = full scan
}
