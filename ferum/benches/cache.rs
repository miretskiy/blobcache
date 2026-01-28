//! Benchmarks for ferum cache.
//!
//! Matches the Go BenchmarkBlobCache methodology:
//! - Mixed read/write workloads with Zipfian distribution
//! - Variable blob sizes (100KB - 2MB)
//! - Latency histograms via HDR
//! - System monitoring (RSS, disk I/O)
//! - PARALLEL execution with num_cpus workers
//!
//! # Usage
//!
//! ```bash
//! # Small test: ~10GB logical writes
//! cargo bench --bench cache -- blobcache --sample-size 10000
//!
//! # Medium test: ~100GB logical writes
//! cargo bench --bench cache -- blobcache --sample-size 100000
//!
//! # Large test: ~256GB (exercises eviction + hole punching)
//! cargo bench --bench cache -- blobcache --sample-size 256000
//!
//! # Run parallel benchmark directly (bypassing criterion):
//! cargo bench --bench cache -- parallel
//! ```

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferum::{cache, Config};
use rand::prelude::*;
use tempfile::tempdir;

/// Generate random data of given size.
fn random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; size];
    rng.fill(&mut data[..]);
    data
}

/// Zero-allocation key formatter. Writes "prefix<id>" into buf and returns the slice used.
/// Buffer must be at least prefix.len() + 20 bytes (max u64 decimal digits).
#[inline]
fn format_key<'a>(buf: &'a mut [u8], prefix: &[u8], id: u64) -> &'a [u8] {
    let prefix_len = prefix.len();
    buf[..prefix_len].copy_from_slice(prefix);

    // Fast path for small numbers (common case)
    if id < 10 {
        buf[prefix_len] = b'0' + id as u8;
        return &buf[..prefix_len + 1];
    }

    // Format number backwards into buffer
    let mut n = id;
    let mut pos = buf.len();
    while n > 0 {
        pos -= 1;
        buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }

    // Move digits to right after prefix
    let digit_len = buf.len() - pos;
    buf.copy_within(pos.., prefix_len);
    &buf[..prefix_len + digit_len]
}

/// Benchmark put operations.
fn bench_put(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut config = Config::new(dir.path());
    config.write_buffer_size = 64 * 1024 * 1024; // 64MB buffer
    config.max_inflight_slabs = 8;
    let cache = ferum::Cache::open(config).unwrap();

    let value = random_data(4096); // 4KB value

    let mut group = c.benchmark_group("put");
    group.throughput(Throughput::Bytes(4096));

    group.bench_function("4kb", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{}", i);
            cache.put(key.as_bytes(), black_box(&value)).unwrap();
            i += 1;
            // Periodic flush to prevent buffer exhaustion
            if i % 10000 == 0 {
                cache.flush();
            }
        });
    });

    group.finish();
    cache.close().unwrap();
}

/// Benchmark get operations (cache hits).
fn bench_get_hit(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let cache = cache(dir.path()).open().unwrap();

    // Pre-populate cache
    let num_keys = 1000;
    let value = random_data(4096);
    for i in 0..num_keys {
        let key = format!("key{:06}", i);
        cache.put(key.as_bytes(), &value).unwrap();
    }

    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Bytes(4096));

    group.bench_function("hit_4kb", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let i: usize = rng.gen_range(0..num_keys);
            let key = format!("key{:06}", i);
            black_box(cache.get(key.as_bytes()));
        });
    });

    group.finish();
    cache.close().unwrap();
}

/// Benchmark get operations (cache misses).
fn bench_get_miss(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let cache = cache(dir.path()).open().unwrap();

    // Empty cache - all gets will miss
    let mut group = c.benchmark_group("get");

    group.bench_function("miss", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("nonexistent{}", i);
            black_box(cache.get(key.as_bytes()));
            i += 1;
        });
    });

    group.finish();
    cache.close().unwrap();
}

/// Benchmark mixed read/write workload with Zipfian distribution.
///
/// This is the Rust equivalent of Go's BenchmarkBlobCache:
/// - 10% writes with ~1MB blob sizes
/// - 40% hot reads (Zipfian distribution)
/// - 25% cold reads (sequential scan)
/// - 25% misses (bloom filter test)
///
/// Configuration matches Go EXACTLY:
/// - max_size: 400GB
/// - write_buffer_size: 128MB
/// - max_inflight_slabs: 32
/// - flush_concurrency: 6
/// - direct_io_write: true
/// - fdatasync: true (via WAL)
/// - degraded_mode: Panic
fn bench_blobcache(c: &mut Criterion) {
    use ferum::config::DegradedMode;
    use rand_distr::{Distribution, Zipf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    // Use instance_storage if available (like Go), else temp
    let tmp_dir;
    let bench_path = if std::path::Path::new("/instance_storage").exists() {
        std::path::PathBuf::from("/instance_storage/bench-ferum")
    } else {
        tmp_dir = tempdir().unwrap();
        tmp_dir.path().to_path_buf()
    };
    let _ = std::fs::remove_dir_all(&bench_path);
    std::fs::create_dir_all(&bench_path).unwrap();

    // Configuration matching Go BenchmarkBlobCache EXACTLY
    let mut config = Config::new(&bench_path);
    config.max_size = 400 << 30;           // 400GB (matches Go)
    config.write_buffer_size = 128 << 20;  // 128MB
    config.max_inflight_slabs = 32;        // 32 (matches Go)
    config.max_cached_slabs = 8;           // 8 (matches Go default)
    config.flush_concurrency = 6;          // 6 (matches Go)
    config.direct_io_write = true;         // Direct I/O for writes
    config.fdatasync = true;               // fdatasync enabled
    config.wal_enabled = true;             // WAL enabled (CAS mode)
    config.degraded_mode = DegradedMode::Panic; // Crash on errors

    let cache = Arc::new(ferum::Cache::open(config).unwrap());

    // Pre-generate entropy buffer (32MB)
    let entropy = random_data(32 << 20);

    // Blob size range: 100KB - 2MB (matches Go)
    const BLOB_SIZE_LO: usize = 100_000;
    const BLOB_SIZE_HI_RNG: usize = 1_900_000;

    // Workload weights (matches Go)
    const WRITE_BOUND: u32 = 10;
    const HOT_READ_BOUND: u32 = 50; // 10 + 40
    const COLD_READ_BOUND: u32 = 75; // 50 + 25

    // Warmup phase (10000 keys like Go)
    const WARMUP_KEYS: u64 = 10_000;
    const READ_MIN_KEYS: u64 = 5_000;

    println!(">>> Warmup: Writing {} keys to reach steady-state...", WARMUP_KEYS);
    let warmup_start = std::time::Instant::now();
    let mut warmup_bytes: u64 = 0;
    for i in 0..WARMUP_KEYS {
        let key = format!("key-{}", i);
        let blob_size = 1024 * 1024; // 1MB
        cache.put(key.as_bytes(), &entropy[..blob_size]).unwrap();
        warmup_bytes += blob_size as u64;
    }
    cache.drain();
    let warmup_elapsed = warmup_start.elapsed();
    let warmup_throughput =
        (warmup_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / warmup_elapsed.as_secs_f64();
    println!(
        ">>> Warmup complete: {:.2} GB/s ({} keys in {:.2}s)",
        warmup_throughput,
        WARMUP_KEYS,
        warmup_elapsed.as_secs_f64()
    );

    let write_head = Arc::new(AtomicU64::new(WARMUP_KEYS));
    let total_write_bytes = Arc::new(AtomicU64::new(0));

    let mut group = c.benchmark_group("blobcache");
    group.sample_size(100); // Each sample = one write iteration
    group.throughput(Throughput::Bytes(1_000_000)); // ~1MB per iteration

    let cache_clone = Arc::clone(&cache);
    let entropy_clone = entropy.clone();
    let write_head_clone = Arc::clone(&write_head);
    let total_bytes_clone = Arc::clone(&total_write_bytes);

    group.bench_function("mixed_workload", move |b| {
        let mut rng = rand::thread_rng();

        // Zipf distribution: s=1.1, v=1.0, range=2^25 (matches Go)
        // s > 1.0 creates hot-spot behavior where top 10-15% of keys
        // account for 60-70% of accesses
        let zipf = Zipf::new(1 << 25, 1.1).unwrap();

        b.iter(|| {
            let mut data_written = false;

            while !data_written {
                let mut op: u32 = rng.gen_range(0..100);
                let max_id = write_head_clone.load(Ordering::Relaxed);

                // Early stage bias (matches Go's ReadMinKeys logic)
                if max_id < READ_MIN_KEYS {
                    if op < 50 {
                        op = 0; // Force write
                    } else {
                        op = 99; // Force miss
                    }
                }

                if op < WRITE_BOUND {
                    // Write
                    let id = write_head_clone.fetch_add(1, Ordering::Relaxed);
                    let key = format!("key-{}", id);
                    let blob_size = BLOB_SIZE_LO + rng.gen_range(0..BLOB_SIZE_HI_RNG);
                    let offset: usize = rng.gen_range(0..entropy_clone.len().saturating_sub(blob_size));
                    cache_clone
                        .put(key.as_bytes(), &entropy_clone[offset..offset + blob_size])
                        .unwrap();
                    total_bytes_clone.fetch_add(blob_size as u64, Ordering::Relaxed);
                    data_written = true;
                } else if op < HOT_READ_BOUND {
                    // Hot read (Zipfian)
                    let id = (zipf.sample(&mut rng) as u64) % max_id;
                    let key = format!("key-{}", id);
                    black_box(cache_clone.get(key.as_bytes()));
                } else if op < COLD_READ_BOUND {
                    // Cold read (sequential scan of 4 keys)
                    let base_id = rng.gen_range(0..max_id.saturating_sub(4).max(1));
                    for i in 0..4 {
                        let key = format!("key-{}", base_id + i);
                        black_box(cache_clone.get(key.as_bytes()));
                    }
                } else {
                    // Miss (negative lookup)
                    let miss_id: u64 = rng.r#gen();
                    let key = format!("miss-{}", miss_id);
                    black_box(cache_clone.get(key.as_bytes()));
                }
            }
        });
    });

    group.finish();

    // Final stats
    let total_bytes = total_write_bytes.load(Ordering::Relaxed);
    println!(
        "\n>>> Total written: {:.2} GB",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    cache.drain();
    cache.close().unwrap();

    // Cleanup
    let _ = std::fs::remove_dir_all(&bench_path);
}

/// Parallel benchmark matching Go's b.RunParallel() behavior.
///
/// This benchmark runs with num_cpus workers concurrently, each performing
/// the mixed workload. Latencies are tracked via HDR histogram.
///
/// Run with: cargo bench --bench cache -- parallel
fn bench_parallel_blobcache(c: &mut Criterion) {
    use ferum::config::DegradedMode;
    use hdrhistogram::Histogram;
    use rand_distr::{Distribution, Zipf};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use sysinfo::System;

    // Initialize logger (RUST_LOG=info to see eviction logs)
    let _ = env_logger::try_init();

    // Parse iteration count from environment or use default
    let iterations: u64 = std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    let num_workers = num_cpus::get();
    println!(">>> Parallel benchmark: {} workers, {} iterations", num_workers, iterations);

    // Use instance_storage if available (like Go), else temp
    let tmp_dir;
    let bench_path = if std::path::Path::new("/instance_storage").exists() {
        std::path::PathBuf::from("/instance_storage/bench-ferum-parallel")
    } else {
        tmp_dir = tempdir().unwrap();
        tmp_dir.path().to_path_buf()
    };
    let _ = std::fs::remove_dir_all(&bench_path);
    std::fs::create_dir_all(&bench_path).unwrap();

    // Configuration matching Go BenchmarkBlobCache EXACTLY
    let mut config = Config::new(&bench_path);
    config.max_size = 400 << 30;           // 400GB
    config.write_buffer_size = 128 << 20;  // 128MB
    config.max_inflight_slabs = 32;        // 32
    config.max_cached_slabs = 64;          // 64 (increased for Librarian hits)
    config.flush_concurrency = 6;          // 6
    config.direct_io_write = true;
    config.fdatasync = true;
    config.wal_enabled = true;
    config.degraded_mode = DegradedMode::Panic;

    let cache = Arc::new(ferum::Cache::open(config).unwrap());

    // Pre-generate entropy buffer (32MB)
    let entropy: Arc<Vec<u8>> = Arc::new(random_data(32 << 20));

    // Constants matching Go (updated weights: 40/40/10/10)
    const BLOB_SIZE_LO: usize = 100_000;
    const BLOB_SIZE_HI_RNG: usize = 1_900_000;
    const WRITE_BOUND: u32 = 40;      // 40% writes
    const HOT_READ_BOUND: u32 = 80;   // 40% hot reads (40 + 40)
    const COLD_READ_BOUND: u32 = 90;  // 10% cold reads (80 + 10)
    // Remaining 10% = misses
    const WARMUP_KEYS: u64 = 10_000;
    const READ_MIN_KEYS: u64 = 5_000;

    // Warmup phase
    println!(">>> Warmup: Writing {} keys to reach steady-state...", WARMUP_KEYS);
    let warmup_start = Instant::now();
    let mut warmup_bytes: u64 = 0;
    for i in 0..WARMUP_KEYS {
        let key = format!("key-{}", i);
        let blob_size = 1024 * 1024;
        cache.put(key.as_bytes(), &entropy[..blob_size]).unwrap();
        warmup_bytes += blob_size as u64;
    }
    cache.drain();
    let warmup_elapsed = warmup_start.elapsed();
    let warmup_throughput =
        (warmup_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / warmup_elapsed.as_secs_f64();
    println!(
        ">>> Warmup complete: {:.2} GB/s ({} keys in {:.2}s)",
        warmup_throughput,
        WARMUP_KEYS,
        warmup_elapsed.as_secs_f64()
    );

    // Shared state
    let write_head = Arc::new(AtomicU64::new(WARMUP_KEYS));
    let total_write_bytes = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Thread-local histograms will be merged into these
    let get_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let put_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));

    // System monitor
    let sys = Arc::new(Mutex::new(System::new_all()));

    // Start monitor thread
    let monitor_stop = Arc::clone(&stop_flag);
    let monitor_sys = Arc::clone(&sys);
    let monitor_writes = Arc::clone(&total_writes);
    let monitor_bytes = Arc::clone(&total_write_bytes);
    let monitor_handle = thread::spawn(move || {
        let start = Instant::now();
        let mut last_bytes = 0u64;
        let mut last_time = start;

        // Helper to read disk stats from /proc/diskstats (Linux only)
        fn read_disk_stats() -> (u64, u64) {
            #[cfg(target_os = "linux")]
            {
                if let Ok(content) = std::fs::read_to_string("/proc/diskstats") {
                    let mut read_bytes = 0u64;
                    let mut write_bytes = 0u64;
                    for line in content.lines() {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 14 {
                            let name = parts[2];
                            // Only count physical devices (nvme*, sd*)
                            if name.starts_with("nvme") || name.starts_with("sd") {
                                // Field 6: sectors read, Field 10: sectors written
                                // Sector size is 512 bytes
                                if let (Ok(r), Ok(w)) = (parts[5].parse::<u64>(), parts[9].parse::<u64>()) {
                                    read_bytes += r * 512;
                                    write_bytes += w * 512;
                                }
                            }
                        }
                    }
                    return (read_bytes, write_bytes);
                }
            }
            (0, 0)
        }

        // Initial disk stats
        let (mut last_disk_read, mut last_disk_write) = read_disk_stats();

        while !monitor_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(10)); // Heartbeat every 10s like Go

            let now = Instant::now();
            let elapsed = now.duration_since(start).as_secs_f64();
            let bytes = monitor_bytes.load(Ordering::Relaxed);
            let writes = monitor_writes.load(Ordering::Relaxed);

            // Calculate throughput since last heartbeat
            let delta_bytes = bytes - last_bytes;
            let delta_time = now.duration_since(last_time).as_secs_f64();
            let log_throughput = (delta_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / delta_time;

            // Disk I/O stats
            let (curr_read, curr_write) = read_disk_stats();
            let phys_read_tp = ((curr_read - last_disk_read) as f64 / (1024.0 * 1024.0 * 1024.0)) / delta_time;
            let phys_write_tp = ((curr_write - last_disk_write) as f64 / (1024.0 * 1024.0 * 1024.0)) / delta_time;

            // Get RSS
            let mut sys = monitor_sys.lock().unwrap();
            sys.refresh_all();
            let pid = sysinfo::get_current_pid().unwrap();
            let rss = sys
                .process(pid)
                .map(|p| p.memory() as f64 / (1024.0 * 1024.0 * 1024.0))
                .unwrap_or(0.0);

            println!(
                "\n[HEARTBEAT {:.0}s]\n  MEM:   RSS: {:.2}GB\n  DISK:  Phys-Read: {:.2} GB/s | Phys-Write: {:.2} GB/s\n  WRITE: Writes: {} | Log-TP: {:.2} GB/s | Total: {:.2} GB",
                elapsed, rss, phys_read_tp, phys_write_tp, writes, log_throughput, bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );

            last_bytes = bytes;
            last_time = now;
            last_disk_read = curr_read;
            last_disk_write = curr_write;
        }
    });

    // Iteration counter for parallel work distribution
    let iterations_done = Arc::new(AtomicU64::new(0));

    let bench_start = Instant::now();

    // Criterion doesn't support parallel directly, so we run the parallel
    // benchmark outside of criterion's iteration loop
    let mut group = c.benchmark_group("parallel_blobcache");
    group.sample_size(10); // Minimal samples since we do our own iteration
    group.measurement_time(Duration::from_secs(1)); // Short measurement

    let cache_for_bench = Arc::clone(&cache);
    let entropy_for_bench = Arc::clone(&entropy);
    let write_head_for_bench = Arc::clone(&write_head);
    let total_bytes_for_bench = Arc::clone(&total_write_bytes);
    let total_writes_for_bench = Arc::clone(&total_writes);
    let iterations_for_bench = Arc::clone(&iterations_done);
    let get_hist_for_bench = Arc::clone(&get_hist);
    let put_hist_for_bench = Arc::clone(&put_hist);

    group.bench_function("mixed_workload", move |b| {
        b.iter(|| {
            // Spawn worker threads
            let mut handles = Vec::with_capacity(num_workers);

            for _ in 0..num_workers {
                let cache = Arc::clone(&cache_for_bench);
                let entropy = Arc::clone(&entropy_for_bench);
                let write_head = Arc::clone(&write_head_for_bench);
                let total_bytes = Arc::clone(&total_bytes_for_bench);
                let total_writes = Arc::clone(&total_writes_for_bench);
                let iter_done = Arc::clone(&iterations_for_bench);
                let get_hist = Arc::clone(&get_hist_for_bench);
                let put_hist = Arc::clone(&put_hist_for_bench);

                handles.push(thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    let zipf = Zipf::new(1 << 25, 1.1).unwrap();

                    // Thread-local histograms
                    let mut local_get_hist = Histogram::<u64>::new(3).unwrap();
                    let mut local_put_hist = Histogram::<u64>::new(3).unwrap();

                    // Zero-allocation key buffer (prefix + max u64 digits)
                    let mut key_buf = [0u8; 32];

                    loop {
                        // Check if we've done enough iterations
                        let done = iter_done.fetch_add(1, Ordering::Relaxed);
                        if done >= iterations {
                            break;
                        }

                        let mut data_written = false;
                        while !data_written {
                            let mut op: u32 = rng.gen_range(0..100);
                            let max_id = write_head.load(Ordering::Relaxed);

                            if max_id < READ_MIN_KEYS {
                                if op < 50 {
                                    op = 0;
                                } else {
                                    op = 99;
                                }
                            }

                            if op < WRITE_BOUND {
                                // Write with latency tracking
                                let start = Instant::now();
                                let id = write_head.fetch_add(1, Ordering::Relaxed);
                                let key = format_key(&mut key_buf, b"key-", id);
                                let blob_size = BLOB_SIZE_LO + rng.gen_range(0..BLOB_SIZE_HI_RNG);
                                let offset: usize =
                                    rng.gen_range(0..entropy.len().saturating_sub(blob_size));
                                cache
                                    .put(key, &entropy[offset..offset + blob_size])
                                    .unwrap();
                                let elapsed = start.elapsed().as_micros() as u64;
                                let _ = local_put_hist.record(elapsed);

                                total_bytes.fetch_add(blob_size as u64, Ordering::Relaxed);
                                total_writes.fetch_add(1, Ordering::Relaxed);
                                data_written = true;
                            } else if op < HOT_READ_BOUND {
                                // Hot read with latency tracking (target newest keys for Librarian hits)
                                let start = Instant::now();
                                let zipf_val = (zipf.sample(&mut rng) as u64) % max_id;
                                let id = max_id - 1 - zipf_val;
                                let key = format_key(&mut key_buf, b"key-", id);
                                black_box(cache.get(key));
                                let elapsed = start.elapsed().as_micros() as u64;
                                let _ = local_get_hist.record(elapsed);
                            } else if op < COLD_READ_BOUND {
                                // Cold read
                                let start = Instant::now();
                                let base_id = rng.gen_range(0..max_id.saturating_sub(4).max(1));
                                for i in 0..4 {
                                    let key = format_key(&mut key_buf, b"key-", base_id + i);
                                    black_box(cache.get(key));
                                }
                                let elapsed = start.elapsed().as_micros() as u64;
                                let _ = local_get_hist.record(elapsed);
                            } else {
                                // Miss
                                let start = Instant::now();
                                let miss_id: u64 = rng.r#gen();
                                let key = format_key(&mut key_buf, b"miss-", miss_id);
                                black_box(cache.get(key));
                                let elapsed = start.elapsed().as_micros() as u64;
                                let _ = local_get_hist.record(elapsed);
                            }
                        }
                    }

                    // Merge thread-local histograms into global
                    get_hist.lock().unwrap().add(&local_get_hist).unwrap();
                    put_hist.lock().unwrap().add(&local_put_hist).unwrap();
                }));
            }

            // Wait for all workers
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.finish();

    let bench_elapsed = bench_start.elapsed();

    // Stop monitor
    stop_flag.store(true, Ordering::Relaxed);
    let _ = monitor_handle.join();

    // Final stats
    let total_bytes = total_write_bytes.load(Ordering::Relaxed);
    let total_writes_count = total_writes.load(Ordering::Relaxed);
    let throughput =
        (total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / bench_elapsed.as_secs_f64();

    println!("\n>>> Benchmark complete!");
    println!(">>> Duration: {:.2}s", bench_elapsed.as_secs_f64());
    println!(">>> Total writes: {}", total_writes_count);
    println!(
        ">>> Total written: {:.2} GB",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(">>> Throughput: {:.2} GB/s", throughput);

    // Print latency histograms
    {
        let get_h = get_hist.lock().unwrap();
        let put_h = put_hist.lock().unwrap();

        println!("\n>>> GET Latency (µs):");
        println!(
            ">>>   p50: {}, p99: {}, p999: {}",
            get_h.value_at_quantile(0.50),
            get_h.value_at_quantile(0.99),
            get_h.value_at_quantile(0.999)
        );

        println!(">>> PUT Latency (µs):");
        println!(
            ">>>   p50: {}, p99: {}, p999: {}",
            put_h.value_at_quantile(0.50),
            put_h.value_at_quantile(0.99),
            put_h.value_at_quantile(0.999)
        );
    }

    // Final RSS
    {
        let mut sys = sys.lock().unwrap();
        sys.refresh_all();
        let pid = sysinfo::get_current_pid().unwrap();
        let rss = sys
            .process(pid)
            .map(|p| p.memory() as f64 / (1024.0 * 1024.0 * 1024.0))
            .unwrap_or(0.0);
        println!(">>> Final RSS: {:.2} GB", rss);
    }

    cache.drain();
    cache.close().unwrap();

    // Cleanup
    let _ = std::fs::remove_dir_all(&bench_path);
}

/// Benchmark bloom filter fast rejection.
fn bench_bloom_rejection(c: &mut Criterion) {
    use ferum::BloomFilter;
    use ferum::Key;

    let filter = BloomFilter::new(100_000, 0.01);

    // Add some keys
    for i in 0..10_000 {
        let key = format!("bloomkey{}", i);
        filter.add(Key::from_bytes(key.as_bytes()));
    }

    let mut group = c.benchmark_group("bloom");

    group.bench_function("test_hit", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let i: u32 = rng.gen_range(0..10_000);
            let key = format!("bloomkey{}", i);
            black_box(filter.test(Key::from_bytes(key.as_bytes())));
        });
    });

    group.bench_function("test_miss", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let i: u32 = rng.r#gen();
            let key = format!("nonexistent{}", i);
            black_box(filter.test(Key::from_bytes(key.as_bytes())));
        });
    });

    group.finish();
}

/// Benchmark key hashing.
fn bench_key_hash(c: &mut Criterion) {
    use ferum::Key;

    let data_16 = random_data(16);
    let data_64 = random_data(64);
    let data_256 = random_data(256);
    let data_1k = random_data(1024);

    let mut group = c.benchmark_group("key_hash");

    group.bench_function("16b", |b| {
        b.iter(|| black_box(Key::from_bytes(&data_16)));
    });

    group.bench_function("64b", |b| {
        b.iter(|| black_box(Key::from_bytes(&data_64)));
    });

    group.bench_function("256b", |b| {
        b.iter(|| black_box(Key::from_bytes(&data_256)));
    });

    group.bench_function("1kb", |b| {
        b.iter(|| black_box(Key::from_bytes(&data_1k)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get_hit,
    bench_get_miss,
    bench_blobcache,
    bench_parallel_blobcache,
    bench_bloom_rejection,
    bench_key_hash,
);

criterion_main!(benches);
