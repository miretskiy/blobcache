package blobcache

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/xxh3"
)

// BenchmarkKeyIndex_Physics establishes the fundamental performance characteristics
// of the Pebble-backed KeyIndex at scale.
//
// This is NOT a micro-benchmark — it populates millions of entries distributed
// across thousands of segments (matching real-world segment counts), then measures
// the raw iteration physics: keys/sec, bytes/sec, ns/key, and how these scale
// with dataset size and concurrent iterators.
//
// Run with:
//
//	go test -bench=BenchmarkKeyIndex_Physics -benchtime=1x -v -timeout=10m
//
// On Linux remote, use /instance_storage for NVMe:
//
//	BENCH_DIR=/instance_storage/ki_bench go test -bench=BenchmarkKeyIndex_Physics -benchtime=1x -v -timeout=10m
func BenchmarkKeyIndex_Physics(b *testing.B) {
	for _, tc := range []struct {
		name           string
		numEntries     int
		entriesPerSeg  int // Realistic: WriteBufferSize / avg_blob_size
		userKeyMinLen  int
		userKeyMaxLen  int
		churnFraction  float64 // Fraction of entries to delete before iterating
		concurrentIter int     // Number of parallel iterators
	}{
		// Baseline: scale dataset size. 1000 entries/seg ≈ 128MB seg / 128KB blobs.
		{"1M_entries", 1_000_000, 1000, 80, 256, 0, 1},
		{"5M_entries", 5_000_000, 1000, 80, 256, 0, 1},
		{"10M_entries", 10_000_000, 1000, 80, 256, 0, 1},

		// Churn: measure tombstone impact on iteration.
		{"5M_30pct_churn", 5_000_000, 1000, 80, 256, 0.30, 1},
		{"5M_50pct_churn", 5_000_000, 1000, 80, 256, 0.50, 1},

		// Concurrent iterators: scaling behavior.
		{"5M_2iter", 5_000_000, 1000, 80, 256, 0, 2},
		{"5M_4iter", 5_000_000, 1000, 80, 256, 0, 4},
		{"5M_8iter", 5_000_000, 1000, 80, 256, 0, 8},
		{"5M_16iter", 5_000_000, 1000, 80, 256, 0, 16},

		// Small segments (4KB blobs in 128MB seg = ~32K entries/seg = fewer segments).
		{"5M_large_segs", 5_000_000, 32000, 80, 256, 0, 1},

		// Long keys: stress Pebble's key comparison and block encoding.
		{"5M_long_keys", 5_000_000, 1000, 256, 512, 0, 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runKeyIndexPhysics(b, tc.numEntries, tc.entriesPerSeg,
				tc.userKeyMinLen, tc.userKeyMaxLen, tc.churnFraction, tc.concurrentIter)
		})
	}
}

func runKeyIndexPhysics(
	b *testing.B,
	numEntries, entriesPerSeg, keyMinLen, keyMaxLen int,
	churnFraction float64,
	concurrentIter int,
) {
	b.Helper()

	dir := benchDir(b)
	ki, err := OpenKeyIndex(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer ki.Close()

	// --- Phase 1: Populate ---
	b.Logf("populating %d entries across %d segments...",
		numEntries, (numEntries+entriesPerSeg-1)/entriesPerSeg)

	rng := rand.New(rand.NewSource(42))
	prefixes := []string{
		"tenant-001/images/uploads/2026/02/24",
		"tenant-002/videos/processed/hls/manifest",
		"tenant-003/documents/pdf/archive/legal",
		"tenant-001/thumbnails/256x256/webp/cache",
		"tenant-004/backups/daily/incremental/v3",
		"tenant-005/analytics/events/raw/stream",
		"tenant-006/models/checkpoints/epoch",
		"tenant-007/logs/structured/json/hourly",
	}

	var totalUserKeyBytes int64
	var segID uint32
	batch := make([]KeyIndexEntry, 0, entriesPerSeg)

	populateStart := time.Now()
	for i := range numEntries {
		// Generate realistic key: prefix + random suffix to desired length.
		pfx := prefixes[rng.Intn(len(prefixes))]
		targetLen := keyMinLen + rng.Intn(keyMaxLen-keyMinLen+1)
		key := make([]byte, targetLen)
		// Fill with prefix + formatted index + random padding.
		n := copy(key, pfx)
		n += copy(key[n:], fmt.Sprintf("/%010d/%016x", i, rng.Uint64()))
		// Fill remainder with printable ASCII for realistic key comparison cost.
		for j := n; j < targetLen; j++ {
			key[j] = byte('a' + rng.Intn(26))
		}

		batch = append(batch, KeyIndexEntry{
			UserKey: key,
			Hash:    xxh3.Hash128(key),
		})
		totalUserKeyBytes += int64(len(key))

		if len(batch) >= entriesPerSeg || i == numEntries-1 {
			segID++
			if err := ki.AddEntries(segID, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	populateElapsed := time.Since(populateStart)

	numSegments := int(segID)
	// 3 KVs per entry (hash→key, key→hash, seg_member) + 1 sentinel per segment.
	totalPebbleKVs := numEntries*3 + numSegments

	b.Logf("populate: %d entries, %d segments, %d pebble KVs in %v (%.0f entries/sec)",
		numEntries, numSegments, totalPebbleKVs,
		populateElapsed.Round(time.Millisecond),
		float64(numEntries)/populateElapsed.Seconds())

	// --- Phase 2: Compact (establish steady-state) ---
	compactStart := time.Now()
	if err := ki.db.Compact([]byte{0x00}, []byte{0x04}, true); err != nil {
		b.Fatal(err)
	}
	b.Logf("compaction: %v", time.Since(compactStart).Round(time.Millisecond))

	// Report DB size on disk.
	dbSize := dirSize(dir)
	b.Logf("pebble DB size: %.1f MB (%.0f bytes/entry, %.1f bytes/user-key-byte)",
		float64(dbSize)/(1<<20),
		float64(dbSize)/float64(numEntries),
		float64(dbSize)/float64(totalUserKeyBytes))

	// --- Phase 3: Churn (optional) ---
	expectedLive := numEntries
	if churnFraction > 0 {
		deleteCount := int(float64(numEntries) * churnFraction)
		b.Logf("deleting %d entries (%.0f%% churn)...", deleteCount, churnFraction*100)

		churnRng := rand.New(rand.NewSource(99))
		churnStart := time.Now()

		// Generate random hashes to delete. We regenerate keys the same way
		// as population (same seed=42), but pick random indices.
		perm := churnRng.Perm(numEntries)

		// Regenerate the entries we need (we didn't keep them all in memory).
		deleteRng := rand.New(rand.NewSource(42))
		allHashes := make([]Key, numEntries)
		for i := range numEntries {
			pfx := prefixes[deleteRng.Intn(len(prefixes))]
			targetLen := keyMinLen + deleteRng.Intn(keyMaxLen-keyMinLen+1)
			key := make([]byte, targetLen)
			n := copy(key, pfx)
			n += copy(key[n:], fmt.Sprintf("/%010d/%016x", i, deleteRng.Uint64()))
			for j := n; j < targetLen; j++ {
				key[j] = byte('a' + deleteRng.Intn(26))
			}
			allHashes[i] = xxh3.Hash128(key)
		}

		for i := range deleteCount {
			if err := ki.DeleteByHash(allHashes[perm[i]]); err != nil {
				b.Fatal(err)
			}
		}
		expectedLive = numEntries - deleteCount

		b.Logf("churn complete: %v, %d live entries remain", time.Since(churnStart).Round(time.Millisecond), expectedLive)
		// Do NOT compact after churn — measure with tombstones in LSM.
	}

	// --- Phase 4: Measure iteration ---
	// Warm up: one full scan to prime Pebble's block cache.
	warmupCount := fullScan(b, ki)
	if warmupCount != expectedLive {
		b.Fatalf("warmup: expected %d keys, got %d", expectedLive, warmupCount)
	}

	// Per-iteration data volume: prefix(1) + userKey + value(16) per entry.
	avgKeyLen := float64(totalUserKeyBytes) / float64(numEntries)
	bytesPerScan := int64(float64(expectedLive) * (1 + avgKeyLen + hashSize))

	if concurrentIter == 1 {
		// Single iterator: precise timing.
		const iterations = 10
		var totalElapsed time.Duration

		b.ResetTimer()
		for range iterations {
			start := time.Now()
			count := fullScan(b, ki)
			elapsed := time.Since(start)
			totalElapsed += elapsed

			if count != expectedLive {
				b.Fatalf("expected %d keys, got %d", expectedLive, count)
			}
		}
		b.StopTimer()

		avg := totalElapsed / iterations
		keysPerSec := float64(expectedLive) / avg.Seconds()
		mbPerSec := float64(bytesPerScan) / avg.Seconds() / (1 << 20)
		nsPerKey := float64(avg.Nanoseconds()) / float64(expectedLive)

		b.ReportMetric(nsPerKey, "ns/key")
		b.ReportMetric(keysPerSec, "keys/sec")
		b.ReportMetric(mbPerSec, "MB/s")
		b.ReportMetric(float64(expectedLive), "keys/scan")
		b.SetBytes(bytesPerScan)

		b.Logf("iteration (single): %v per scan, %.0f keys/sec, %.0f MB/s, %.0f ns/key",
			avg.Round(time.Microsecond), keysPerSec, mbPerSec, nsPerKey)
	} else {
		// Concurrent iterators: measure scaling.
		const scansPerIterator = 5

		b.ResetTimer()
		start := time.Now()

		var totalKeys atomic.Int64
		var wg sync.WaitGroup
		for range concurrentIter {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range scansPerIterator {
					count := fullScan(b, ki)
					totalKeys.Add(int64(count))
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		b.StopTimer()

		totalScans := concurrentIter * scansPerIterator
		avgPerScan := elapsed / time.Duration(totalScans)
		// Aggregate throughput: total keys processed / wall time.
		aggKeysPerSec := float64(totalKeys.Load()) / elapsed.Seconds()
		// Per-iterator throughput.
		perIterKeysPerSec := aggKeysPerSec / float64(concurrentIter)
		perIterNsPerKey := float64(avgPerScan.Nanoseconds()) / float64(expectedLive)

		b.ReportMetric(perIterNsPerKey, "ns/key")
		b.ReportMetric(aggKeysPerSec, "agg-keys/sec")
		b.ReportMetric(perIterKeysPerSec, "per-iter-keys/sec")
		b.ReportMetric(float64(concurrentIter), "iterators")

		b.Logf("iteration (%d concurrent): %v avg/scan, %.0f agg keys/sec, %.0f per-iter keys/sec, %.0f ns/key",
			concurrentIter, avgPerScan.Round(time.Microsecond),
			aggKeysPerSec, perIterKeysPerSec, perIterNsPerKey)
	}
}

// fullScan performs a complete ordered iteration over the nsKeyToHash namespace.
// Returns the number of live keys found.
func fullScan(b *testing.B, ki *KeyIndex) int {
	b.Helper()

	snap := ki.NewSnapshot()
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{nsKeyToHash},
		UpperBound: []byte{nsKeyToHash + 1},
	})
	if err != nil {
		b.Fatal(err)
	}

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		_ = iter.Key()
		_ = iter.Value()
		count++
	}
	if err := iter.Error(); err != nil {
		b.Fatal(err)
	}
	_ = iter.Close()
	_ = snap.Close()

	return count
}

// benchDir returns a directory for benchmark data.
// Uses BENCH_DIR env var if set (for /instance_storage on remote),
// otherwise falls back to b.TempDir().
func benchDir(b *testing.B) string {
	b.Helper()
	if dir := os.Getenv("BENCH_DIR"); dir != "" {
		path := filepath.Join(dir, b.Name())
		if err := os.MkdirAll(path, 0o755); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(path) })
		return path
	}
	return b.TempDir()
}

// dirSize returns the total size of all files in a directory tree.
func dirSize(path string) int64 {
	var size int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
