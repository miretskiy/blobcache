//! BlobCache Benchmark - Rust equivalent of Go's BenchmarkBlobCache
//!
//! This is the primary benchmark for validating system behavior under realistic production load.
//!
//! # Usage
//!
//! ```bash
//! # Small test: ~10GB logical writes
//! cargo run --release --example blobcache_bench -- --iterations 10000
//!
//! # Medium test: ~100GB logical writes (exercises eviction)
//! cargo run --release --example blobcache_bench -- --iterations 100000
//!
//! # Large test: ~256GB (extended eviction + hole punching)
//! cargo run --release --example blobcache_bench -- --iterations 256000
//!
//! # Full stress test: ~1TB (validates hole punching, stability, leak detection)
//! cargo run --release --example blobcache_bench -- --iterations 1000000
//!
//! # Custom configuration
//! cargo run --release --example blobcache_bench -- --iterations 10000 --max-size 100 --threads 4
//! ```
//!
//! # Workload Distribution
//!
//! - 10% Write (new data, 100KB - 2MB per blob)
//! - 40% Hot Read (Zipfian: top 10-15% of keys = 60-70% of accesses)
//! - 25% Cold Read (sequential scan pattern)
//! - 25% Miss (negative lookups, tests bloom filter)

use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use ferum::{Config, Cache};
use hdrhistogram::Histogram;
use rand::prelude::*;
use rand_distr::{Distribution, Zipf};
use sysinfo::{System, ProcessRefreshKind};

// --- WORKLOAD CONFIGURATION ---
const WRITE_WEIGHT: u32 = 10;
const HOT_READ_WEIGHT: u32 = 40;
const COLD_READ_WEIGHT: u32 = 25;

const WRITE_BOUND: u32 = WRITE_WEIGHT;
const HOT_READ_BOUND: u32 = WRITE_BOUND + HOT_READ_WEIGHT;
const COLD_READ_BOUND: u32 = HOT_READ_BOUND + COLD_READ_WEIGHT;

const WARMUP_KEYS: u64 = 10000;
const READ_MIN_KEYS: u64 = 5000;

// Blob size range: 100KB - 2MB
const BLOB_SIZE_LO: usize = 100_000;
const BLOB_SIZE_HI_RNG: usize = 1_900_000;

fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse arguments
    let mut iterations: u64 = 10000;
    let mut max_size_gb: u64 = 400;
    let mut num_threads: usize = num_cpus();
    let mut cache_path: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--iterations" | "-n" => {
                i += 1;
                iterations = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(10000);
            }
            "--max-size" => {
                i += 1;
                max_size_gb = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(400);
            }
            "--threads" | "-t" => {
                i += 1;
                num_threads = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(num_cpus());
            }
            "--path" => {
                i += 1;
                cache_path = args.get(i).map(PathBuf::from);
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {}
        }
        i += 1;
    }

    // Determine cache path
    let tmp_dir = cache_path.unwrap_or_else(|| {
        let base = if std::path::Path::new("/instance_storage").exists() {
            PathBuf::from("/instance_storage")
        } else {
            env::temp_dir()
        };
        base.join("bench-blobcache-rust")
    });

    // Clean up existing directory
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir).expect("Failed to create cache directory");

    println!("=== BlobCache Benchmark (Rust) ===");
    println!("Iterations: {}", iterations);
    println!("Max Size: {} GB", max_size_gb);
    println!("Threads: {}", num_threads);
    println!("Cache Path: {:?}", tmp_dir);
    println!();

    // Configuration matching Go benchmark
    let mut config = Config::new(&tmp_dir);
    config.max_size = max_size_gb << 30;
    config.write_buffer_size = 128 << 20; // 128MB
    config.max_inflight_slabs = 32;
    config.max_cached_slabs = 8;
    config.flush_concurrency = 6;
    config.wal_enabled = true;
    config.checksum_enabled = true;

    let cache = Cache::open(config).expect("Failed to open cache");

    // Pre-generate entropy buffer (32MB)
    let entropy = Arc::new(random_data(32 << 20));

    // Shared state
    let write_head = Arc::new(AtomicU64::new(0));
    let total_write_bytes = Arc::new(AtomicU64::new(0));
    let num_reads = Arc::new(AtomicU64::new(0));
    let num_found = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    // --- WARMUP PHASE ---
    println!(">>> Warmup: Writing {} keys to reach steady-state...", WARMUP_KEYS);
    let warmup_start = Instant::now();
    let mut warmup_bytes: u64 = 0;

    for i in 0..WARMUP_KEYS {
        let blob_size = 1024 * 1024; // 1MB
        let key = format!("key-{}", i);
        cache.put(key.as_bytes(), &entropy[..blob_size]).expect("Warmup put failed");
        warmup_bytes += blob_size as u64;
    }
    cache.drain();

    let warmup_elapsed = warmup_start.elapsed();
    let warmup_throughput = (warmup_bytes as f64 / (1 << 30) as f64) / warmup_elapsed.as_secs_f64();
    println!(">>> Warmup complete: {:.2} GB/s ({} keys in {:.2}s)",
             warmup_throughput, WARMUP_KEYS, warmup_elapsed.as_secs_f64());
    println!();

    write_head.store(WARMUP_KEYS, Ordering::Release);

    // --- SYSTEM MONITOR (Background Heartbeat) ---
    let monitor_stop = Arc::clone(&stop_flag);
    let monitor_bytes = Arc::clone(&total_write_bytes);
    let monitor_path = tmp_dir.clone();
    let monitor_handle = thread::spawn(move || {
        system_monitor(monitor_stop, monitor_bytes, monitor_path)
    });

    // --- MAIN BENCHMARK ---
    let benchmark_start = Instant::now();
    let iterations_per_thread = iterations / num_threads as u64;

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let cache_clone = Arc::clone(&cache);
        let entropy_clone = Arc::clone(&entropy);
        let write_head_clone = Arc::clone(&write_head);
        let total_write_bytes_clone = Arc::clone(&total_write_bytes);
        let num_reads_clone = Arc::clone(&num_reads);
        let num_found_clone = Arc::clone(&num_found);

        handles.push(thread::spawn(move || {
            run_worker(
                thread_id,
                iterations_per_thread,
                cache_clone,
                entropy_clone,
                write_head_clone,
                total_write_bytes_clone,
                num_reads_clone,
                num_found_clone,
            )
        }));
    }

    // Collect results
    let mut global_put = Histogram::<u64>::new(3).unwrap();
    let mut global_get = Histogram::<u64>::new(3).unwrap();

    for handle in handles {
        let (put_hist, get_hist) = handle.join().expect("Worker thread panicked");
        global_put.add(&put_hist).unwrap();
        global_get.add(&get_hist).unwrap();
    }

    // Drain and stop
    cache.drain();
    stop_flag.store(true, Ordering::Release);

    let benchmark_elapsed = benchmark_start.elapsed();

    // Wait for monitor
    let peak_rss = monitor_handle.join().unwrap_or(0.0);

    // --- FINAL REPORT ---
    println!();
    println!("=== FINAL LATENCY (clat) REPORT (ns) ===");
    report_latency("GET", &global_get);
    report_latency("PUT", &global_put);

    let total_bytes = total_write_bytes.load(Ordering::Relaxed);
    let reads = num_reads.load(Ordering::Relaxed);
    let found = num_found.load(Ordering::Relaxed);
    let hit_rate = if reads > 0 { found as f64 / reads as f64 * 100.0 } else { 0.0 };

    println!();
    println!("=== SUMMARY ===");
    println!("Total time: {:.2}s", benchmark_elapsed.as_secs_f64());
    println!("Total written: {:.2} GB", total_bytes as f64 / (1 << 30) as f64);
    println!("Write throughput: {:.2} GB/s",
             (total_bytes as f64 / (1 << 30) as f64) / benchmark_elapsed.as_secs_f64());
    println!("Warmup throughput: {:.2} GB/s", warmup_throughput);
    println!("Read hit rate: {:.1}% ({}/{})", hit_rate, found, reads);
    println!("Peak RSS: {:.2} GB", peak_rss);

    // Cleanup
    cache.close().expect("Failed to close cache");
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

fn run_worker(
    thread_id: usize,
    iterations: u64,
    cache: Arc<Cache>,
    entropy: Arc<Vec<u8>>,
    write_head: Arc<AtomicU64>,
    total_write_bytes: Arc<AtomicU64>,
    num_reads: Arc<AtomicU64>,
    num_found: Arc<AtomicU64>,
) -> (Histogram<u64>, Histogram<u64>) {
    let mut rng = rand::thread_rng();
    let zipf = Zipf::new(1 << 25, 1.1).expect("Failed to create Zipf distribution");

    let mut local_put = Histogram::<u64>::new(3).unwrap();
    let mut local_get = Histogram::<u64>::new(3).unwrap();

    let seed = (thread_id as u64).wrapping_mul(0x9E3779B97F4A7C15);
    let _ = seed; // Used for deterministic seeding if needed

    for _ in 0..iterations {
        let mut data_written = false;

        while !data_written {
            let op: u32 = rng.gen_range(0..100);
            let max_id = write_head.load(Ordering::Relaxed);

            // Bias towards writes when not enough keys for reads
            let adjusted_op = if max_id < READ_MIN_KEYS {
                if op < 50 { 0 } else { 99 }
            } else {
                op
            };

            let start = Instant::now();

            if adjusted_op < WRITE_BOUND {
                // Write
                let id = write_head.fetch_add(1, Ordering::Relaxed);
                let key = format!("key-{}", id);
                let blob_size = BLOB_SIZE_LO + rng.gen_range(0..BLOB_SIZE_HI_RNG);
                let offset: usize = rng.gen_range(0..entropy.len().saturating_sub(blob_size));

                cache.put(key.as_bytes(), &entropy[offset..offset + blob_size])
                    .expect("Put failed");

                total_write_bytes.fetch_add(blob_size as u64, Ordering::Relaxed);
                let _ = local_put.record(start.elapsed().as_nanos() as u64);
                data_written = true;

            } else if adjusted_op < HOT_READ_BOUND {
                // Hot read (Zipfian)
                let id = (zipf.sample(&mut rng) as u64) % max_id.max(1);
                let key = format!("key-{}", id);
                let found = cache.get(key.as_bytes()).is_some();

                let _ = local_get.record(start.elapsed().as_nanos() as u64);
                num_reads.fetch_add(1, Ordering::Relaxed);
                if found {
                    num_found.fetch_add(1, Ordering::Relaxed);
                }

            } else if adjusted_op < COLD_READ_BOUND {
                // Cold read (sequential scan of 4 keys)
                let base_id = rng.gen_range(0..max_id.saturating_sub(4).max(1));
                for i in 0..4 {
                    let key = format!("key-{}", base_id + i);
                    let _ = cache.get(key.as_bytes());
                }
                num_reads.fetch_add(4, Ordering::Relaxed);

            } else {
                // Miss (negative lookup)
                let miss_id: u64 = rng.r#gen();
                let key = format!("miss-{}", miss_id);
                let _ = cache.get(key.as_bytes());
                num_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    (local_put, local_get)
}

fn system_monitor(
    stop: Arc<AtomicBool>,
    total_bytes: Arc<AtomicU64>,
    cache_path: PathBuf,
) -> f64 {
    let mut sys = System::new_all();
    let mut max_rss: f64 = 0.0;
    let mut prev_bytes = total_bytes.load(Ordering::Relaxed);
    let interval = Duration::from_secs(10);
    let pid = sysinfo::get_current_pid().ok();

    while !stop.load(Ordering::Relaxed) {
        thread::sleep(interval);

        // Refresh process info
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::everything(),
        );

        let rss_gb = if let Some(pid) = pid {
            if let Some(proc) = sys.process(pid) {
                proc.memory() as f64 / (1 << 30) as f64
            } else {
                0.0
            }
        } else {
            0.0
        };

        if rss_gb > max_rss {
            max_rss = rss_gb;
        }

        // Throughput
        let curr_bytes = total_bytes.load(Ordering::Relaxed);
        let bytes_delta = curr_bytes.saturating_sub(prev_bytes);
        let throughput = (bytes_delta as f64 / (1 << 30) as f64) / interval.as_secs_f64();
        prev_bytes = curr_bytes;

        // Disk usage
        let (physical_size, logical_size) = get_cache_sizes(&cache_path);
        let ratio = if logical_size > 0 {
            physical_size as f64 / logical_size as f64
        } else {
            0.0
        };

        // Get free disk space
        let free_gb = get_free_space(&cache_path);

        println!();
        println!("[HEARTBEAT {}]", chrono_time());
        println!("  MEM:   RSS: {:.2}GB", rss_gb);
        println!("  DISK:  Throughput: {:.2} GB/s | Free: {:.1}GB", throughput, free_gb);
        println!("  SIEVE: Phys: {:.2}GB | Log: {:.2}GB | Ratio: {:.2}",
                 physical_size as f64 / (1 << 30) as f64,
                 logical_size as f64 / (1 << 30) as f64,
                 ratio);
    }

    max_rss
}

fn report_latency(name: &str, h: &Histogram<u64>) {
    let p50 = h.value_at_quantile(0.50);
    let p99 = h.value_at_quantile(0.99);
    let p999 = h.value_at_quantile(0.999);
    let max = h.max();

    println!("{} | p50: {}ns | p99: {}ns | p999: {}ns | max: {}ns",
             name, p50, p99, p999, max);
}

fn random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; size];
    rng.fill(&mut data[..]);
    data
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn get_cache_sizes(path: &PathBuf) -> (u64, u64) {
    let mut physical: u64 = 0;
    let mut logical: u64 = 0;

    fn walk_dir(dir: &std::path::Path, physical: &mut u64, logical: &mut u64) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk_dir(&path, physical, logical);
                } else if let Ok(meta) = entry.metadata() {
                    *logical += meta.len();
                    // On Unix, get actual blocks used
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        *physical += meta.blocks() * 512;
                    }
                    #[cfg(not(unix))]
                    {
                        *physical += meta.len();
                    }
                }
            }
        }
    }

    walk_dir(path, &mut physical, &mut logical);
    (physical, logical)
}

fn get_free_space(path: &PathBuf) -> f64 {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        let c_path = CString::new(path.to_string_lossy().as_bytes()).unwrap();
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
        if ret == 0 {
            return (stat.f_bavail as u64 * stat.f_frsize as u64) as f64 / (1 << 30) as f64;
        }
    }
    0.0
}

fn chrono_time() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, s)
}

fn print_help() {
    println!("BlobCache Benchmark - Rust equivalent of Go's BenchmarkBlobCache");
    println!();
    println!("USAGE:");
    println!("    cargo run --release --example blobcache_bench [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    -n, --iterations <N>    Number of write iterations (default: 10000)");
    println!("    --max-size <GB>         Maximum cache size in GB (default: 400)");
    println!("    -t, --threads <N>       Number of worker threads (default: CPU count)");
    println!("    --path <PATH>           Custom cache directory path");
    println!("    -h, --help              Print this help message");
    println!();
    println!("EXAMPLES:");
    println!("    # Small test (~10GB)");
    println!("    cargo run --release --example blobcache_bench -- -n 10000");
    println!();
    println!("    # Medium test (~100GB)");
    println!("    cargo run --release --example blobcache_bench -- -n 100000");
    println!();
    println!("    # Large test (~256GB)");
    println!("    cargo run --release --example blobcache_bench -- -n 256000");
    println!();
    println!("WORKLOAD DISTRIBUTION:");
    println!("    10% Write  - New data (100KB - 2MB per blob)");
    println!("    40% Hot    - Zipfian reads (top 10-15% keys = 60-70% accesses)");
    println!("    25% Cold   - Sequential scan (4 keys per operation)");
    println!("    25% Miss   - Negative lookups (bloom filter test)");
}
