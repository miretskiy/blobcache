//! Foyer Benchmark - Aligned with BlobCache's BenchmarkBlobCache
//!
//! This benchmark uses the same workload distribution as BlobCache for fair comparison:
//!
//! # Workload Distribution (40/40/10/10 - write-heavy to saturate NVMe)
//!
//! - 40% Write (new data, 100KB - 2MB per blob)
//! - 40% Hot Read (Zipfian: top 10-15% of keys = 60-70% of accesses)
//! - 10% Cold Read (sequential scan pattern)
//! - 10% Miss (negative lookups, tests bloom filter)
//!
//! # Usage
//!
//! ```bash
//! # Small test: ~10GB logical writes
//! cargo run --release -- --iterations 10000
//!
//! # Medium test: ~100GB logical writes
//! cargo run --release -- --iterations 100000
//!
//! # Large test: ~256GB
//! cargo run --release -- --iterations 256000
//! ```

use clap::Parser;
use foyer::{HybridCache, HybridCacheBuilder, HybridCachePolicy};
use foyer_storage::{BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, Throttle};
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{ProcessRefreshKind, System};

// --- WORKLOAD CONFIGURATION (matches BlobCache 40/40/10/10) ---
// Higher write percentage to saturate NVMe bandwidth
const WRITE_WEIGHT: u32 = 40;
const HOT_READ_WEIGHT: u32 = 40;
const COLD_READ_WEIGHT: u32 = 10;

const WRITE_BOUND: u32 = WRITE_WEIGHT;           // 40% writes
const HOT_READ_BOUND: u32 = WRITE_BOUND + HOT_READ_WEIGHT;    // 80% (40+40)
const COLD_READ_BOUND: u32 = HOT_READ_BOUND + COLD_READ_WEIGHT; // 90% (80+10), remaining 10% = miss

const WARMUP_KEYS: u64 = 10000;
const READ_MIN_KEYS: u64 = 5000;

// Blob size range: 100KB - 2MB (matches BlobCache)
const BLOB_SIZE_LO: usize = 100_000;
const BLOB_SIZE_HI_RNG: usize = 1_900_000;

#[derive(Parser, Debug)]
#[command(author, version, about = "Foyer Benchmark - Aligned with BlobCache workload")]
struct Args {
    /// Number of write operations (each ~1MB average)
    #[arg(short = 'n', long, default_value_t = 256000)]
    iterations: u64,

    /// Number of worker threads (default: num_cpus)
    #[arg(short, long)]
    workers: Option<usize>,

    /// Cache directory path
    #[arg(short, long, default_value = "/tmp/foyer-bench")]
    path: PathBuf,

    /// Cache capacity in GB (default: 400 to match BlobCache)
    #[arg(short, long, default_value_t = 400)]
    capacity_gb: usize,

    /// Rate limit writes (MB/s, 0 = unlimited)
    #[arg(long, default_value_t = 0)]
    write_rate_limit_mb: usize,
}

struct WorkerStats {
    write_head: Arc<AtomicU64>,
    num_reads: Arc<AtomicU64>,
    num_found: Arc<AtomicU64>,
    total_bytes_written: Arc<AtomicU64>,
    put_hist: Arc<Mutex<Histogram<u64>>>,
    get_hist: Arc<Mutex<Histogram<u64>>>,
}

impl WorkerStats {
    fn new() -> Self {
        // Histograms: 10ns to 60 seconds, 3 significant digits (matches BlobCache)
        let put_hist = Histogram::<u64>::new_with_bounds(10, 60_000_000_000, 3).unwrap();
        let get_hist = Histogram::<u64>::new_with_bounds(10, 60_000_000_000, 3).unwrap();

        Self {
            write_head: Arc::new(AtomicU64::new(0)),
            num_reads: Arc::new(AtomicU64::new(0)),
            num_found: Arc::new(AtomicU64::new(0)),
            total_bytes_written: Arc::new(AtomicU64::new(0)),
            put_hist: Arc::new(Mutex::new(put_hist)),
            get_hist: Arc::new(Mutex::new(get_hist)),
        }
    }
}

async fn run_worker(
    worker_id: usize,
    cache: Arc<HybridCache<Vec<u8>, Vec<u8>>>,
    stats: Arc<WorkerStats>,
    iterations_per_worker: u64,
    entropy: Arc<Vec<u8>>,
) {
    let mut rng = StdRng::seed_from_u64(42 + worker_id as u64);

    // Zipfian distribution (s=1.1 matches BlobCache)
    // Top 10-15% of keys get 60-70% of accesses
    let zipf = Zipf::new(1 << 25, 1.1).expect("Failed to create Zipf distribution");

    // Local histograms for this worker
    let mut local_put = Histogram::<u64>::new_with_bounds(10, 60_000_000_000, 3).unwrap();
    let mut local_get = Histogram::<u64>::new_with_bounds(10, 60_000_000_000, 3).unwrap();

    for _ in 0..iterations_per_worker {
        let mut data_written = false;

        while !data_written {
            let op: u32 = rng.gen_range(0..100);
            let max_id = stats.write_head.load(Ordering::Relaxed);

            // Bias towards writes when not enough keys for reads
            let adjusted_op = if max_id < READ_MIN_KEYS {
                if op < 50 {
                    0
                } else {
                    99
                }
            } else {
                op
            };

            let start = Instant::now();

            if adjusted_op < WRITE_BOUND {
                // 10% Write - Variable blob sizes (100KB - 2MB)
                let blob_size = BLOB_SIZE_LO + rng.gen_range(0..BLOB_SIZE_HI_RNG);
                let offset: usize = rng.gen_range(0..entropy.len().saturating_sub(blob_size));

                let id = stats.write_head.fetch_add(1, Ordering::Relaxed);
                let key = format!("key-{}", id).into_bytes();
                let value = entropy[offset..offset + blob_size].to_vec();

                cache.insert(key, value);
                stats
                    .total_bytes_written
                    .fetch_add(blob_size as u64, Ordering::Relaxed);
                local_put.record(start.elapsed().as_nanos() as u64).ok();
                data_written = true;
            } else if adjusted_op < HOT_READ_BOUND {
                // 40% Hot Read - Zipfian distribution targeting recent keys
                if max_id > 0 {
                    let zipf_val = zipf.sample(&mut rng) as u64 % max_id;
                    let id = max_id.saturating_sub(1).saturating_sub(zipf_val);
                    let key = format!("key-{}", id).into_bytes();

                    stats.num_reads.fetch_add(1, Ordering::Relaxed);
                    if let Ok(Some(entry)) = cache.get(&key).await {
                        // Touch the value to force any lazy loading
                        let _ = entry.value().len();
                        stats.num_found.fetch_add(1, Ordering::Relaxed);
                    }
                    local_get.record(start.elapsed().as_nanos() as u64).ok();
                }
            } else if adjusted_op < COLD_READ_BOUND {
                // 25% Cold Read - Sequential scan of 4 consecutive keys
                if max_id > 4 {
                    let base_id = rng.gen_range(0..max_id.saturating_sub(4));
                    for i in 0..4u64 {
                        let key = format!("key-{}", base_id + i).into_bytes();
                        let _ = cache.get(&key).await;
                    }
                    stats.num_reads.fetch_add(4, Ordering::Relaxed);
                }
            } else {
                // 25% Miss - Negative lookups (bloom filter test)
                let miss_id: u64 = rng.gen();
                let key = format!("miss-{}", miss_id).into_bytes();

                stats.num_reads.fetch_add(1, Ordering::Relaxed);
                if let Ok(Some(_)) = cache.get(&key).await {
                    // Unexpected hit on miss key
                    stats.num_found.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    // Merge local histograms into global
    stats.put_hist.lock().unwrap().add(&local_put).ok();
    stats.get_hist.lock().unwrap().add(&local_get).ok();
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
        std::thread::sleep(interval);

        // Refresh process info
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::everything(),
        );

        let rss_gb = if let Some(pid) = pid {
            if let Some(proc) = sys.process(pid) {
                proc.memory() as f64 / (1u64 << 30) as f64
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
        let throughput = (bytes_delta as f64 / (1u64 << 30) as f64) / interval.as_secs_f64();
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
        println!("[HEARTBEAT {}]", format_time());
        println!("  MEM:   RSS: {:.2}GB", rss_gb);
        println!(
            "  DISK:  Throughput: {:.2} GB/s | Free: {:.1}GB",
            throughput, free_gb
        );
        println!(
            "  CACHE: Phys: {:.2}GB | Log: {:.2}GB | Ratio: {:.2}",
            physical_size as f64 / (1u64 << 30) as f64,
            logical_size as f64 / (1u64 << 30) as f64,
            ratio
        );
    }

    max_rss
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
        if let Ok(c_path) = CString::new(path.to_string_lossy().as_bytes()) {
            let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
            let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
            if ret == 0 {
                return (stat.f_bavail as u64 * stat.f_frsize as u64) as f64 / (1u64 << 30) as f64;
            }
        }
    }
    0.0
}

fn format_time() -> String {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Determine worker count
    let num_workers = args.workers.unwrap_or_else(num_cpus::get);
    let iterations_per_worker = args.iterations / num_workers as u64;

    println!("=== Foyer Benchmark (BlobCache-aligned workload) ===");
    println!("Iterations: {} (writes)", args.iterations);
    println!("Workers: {}", num_workers);
    println!("Cache path: {:?}", args.path);
    println!("Cache capacity: {} GB", args.capacity_gb);
    println!(
        "Expected data: ~{:.1} GB (avg 1MB/write)",
        args.iterations as f64 / 1024.0
    );
    println!();
    println!("Workload distribution (40/40/10/10 - write-heavy):");
    println!("  40% Write  (100KB - 2MB per blob)");
    println!("  40% Hot    (Zipfian: top 10-15% keys = 60-70% accesses)");
    println!("  10% Cold   (Sequential scan of 4 keys)");
    println!("  10% Miss   (Negative lookups)");
    println!();

    // Clean up old benchmark data
    let _ = std::fs::remove_dir_all(&args.path);
    std::fs::create_dir_all(&args.path)?;

    // Pre-generate entropy buffer (32MB) - same as BlobCache
    println!("Generating entropy buffer (32MB)...");
    let mut entropy = vec![0u8; 32 << 20];
    let mut rng = StdRng::seed_from_u64(12345);
    rng.fill(&mut entropy[..]);
    let entropy = Arc::new(entropy);

    // Build foyer cache
    // Using WriteOnInsertion to ensure data goes to disk (not just memory)
    let device = FsDeviceBuilder::new(&args.path)
        .with_capacity((args.capacity_gb + 100) * 1024 * 1024 * 1024) // usize capacity
        .with_throttle(Throttle::new()) // Unlimited
        .build()?;

    let engine = BlockEngineBuilder::new(device)
        .with_flushers(8)
        .with_buffer_pool_size(8 * 1024 * 1024 * 1024) // 8GB buffer pool
        .with_submit_queue_size_threshold(8 * 1024 * 1024 * 1024);

    let cache: Arc<HybridCache<Vec<u8>, Vec<u8>>> = Arc::new(
        HybridCacheBuilder::new()
            .with_name("foyer-bench")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(100 * 1024 * 1024) // 100MB memory tier (minimal, emphasize disk)
            .with_weighter(|_k: &Vec<u8>, v: &Vec<u8>| v.len())
            .storage()
            .with_engine_config(engine)
            .build()
            .await?,
    );

    let stats = Arc::new(WorkerStats::new());

    // --- WARMUP PHASE (matches BlobCache) ---
    println!(
        ">>> Warmup: Writing {} keys to reach steady-state...",
        WARMUP_KEYS
    );
    let warmup_start = Instant::now();
    let mut warmup_bytes: u64 = 0;

    for i in 0..WARMUP_KEYS {
        let blob_size = 1024 * 1024; // 1MB during warmup
        let key = format!("key-{}", i).into_bytes();
        let value = entropy[..blob_size].to_vec();
        cache.insert(key, value);
        warmup_bytes += blob_size as u64;
    }

    // Wait for warmup writes to flush
    tokio::time::sleep(Duration::from_secs(2)).await;

    let warmup_elapsed = warmup_start.elapsed();
    let warmup_throughput =
        (warmup_bytes as f64 / (1u64 << 30) as f64) / warmup_elapsed.as_secs_f64();
    println!(
        ">>> Warmup complete: {:.2} GB/s ({} keys in {:.2}s)",
        warmup_throughput,
        WARMUP_KEYS,
        warmup_elapsed.as_secs_f64()
    );
    println!();

    stats.write_head.store(WARMUP_KEYS, Ordering::Release);

    // --- SYSTEM MONITOR (Background Heartbeat) ---
    let stop_flag = Arc::new(AtomicBool::new(false));
    let monitor_stop = Arc::clone(&stop_flag);
    let monitor_bytes = Arc::clone(&stats.total_bytes_written);
    let monitor_path = args.path.clone();
    let monitor_handle = std::thread::spawn(move || {
        system_monitor(monitor_stop, monitor_bytes, monitor_path)
    });

    // --- MAIN BENCHMARK ---
    println!("Starting benchmark ({} iterations)...", args.iterations);
    let benchmark_start = Instant::now();

    // Spawn workers
    let mut handles = vec![];
    for worker_id in 0..num_workers {
        let cache_clone = Arc::clone(&cache);
        let stats_clone = Arc::clone(&stats);
        let entropy_clone = Arc::clone(&entropy);

        let handle = tokio::spawn(async move {
            run_worker(
                worker_id,
                cache_clone,
                stats_clone,
                iterations_per_worker,
                entropy_clone,
            )
            .await;
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.await?;
    }

    let workers_done = benchmark_start.elapsed();
    println!(
        "\nWorkers finished in {:.2}s, flushing to disk...",
        workers_done.as_secs_f64()
    );

    // Flush remaining data
    cache.close().await?;

    let total_elapsed = benchmark_start.elapsed();
    let flush_time = total_elapsed - workers_done;
    println!("Flush completed in {:.2}s", flush_time.as_secs_f64());

    // Stop monitor
    stop_flag.store(true, Ordering::Release);
    let peak_rss = monitor_handle.join().unwrap_or(0.0);

    // --- FINAL REPORT ---
    println!();
    println!("=== FINAL LATENCY (clat) REPORT (ns) ===");

    let put_hist = stats.put_hist.lock().unwrap();
    if put_hist.len() > 0 {
        println!(
            "PUT | p50: {}ns | p99: {}ns | p999: {}ns | max: {}ns",
            put_hist.value_at_quantile(0.50),
            put_hist.value_at_quantile(0.99),
            put_hist.value_at_quantile(0.999),
            put_hist.max()
        );
    }

    let get_hist = stats.get_hist.lock().unwrap();
    if get_hist.len() > 0 {
        println!(
            "GET | p50: {}ns | p99: {}ns | p999: {}ns | max: {}ns",
            get_hist.value_at_quantile(0.50),
            get_hist.value_at_quantile(0.99),
            get_hist.value_at_quantile(0.999),
            get_hist.max()
        );
    }

    let total_bytes = stats.total_bytes_written.load(Ordering::Relaxed);
    let reads = stats.num_reads.load(Ordering::Relaxed);
    let found = stats.num_found.load(Ordering::Relaxed);
    let hit_rate = if reads > 0 {
        found as f64 / reads as f64 * 100.0
    } else {
        0.0
    };

    println!();
    println!("=== SUMMARY ===");
    println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("  Worker time: {:.2}s", workers_done.as_secs_f64());
    println!("  Flush time: {:.2}s", flush_time.as_secs_f64());
    println!(
        "Total written: {:.2} GB",
        total_bytes as f64 / (1u64 << 30) as f64
    );
    println!(
        "Write throughput: {:.2} GB/s",
        (total_bytes as f64 / (1u64 << 30) as f64) / total_elapsed.as_secs_f64()
    );
    println!("Warmup throughput: {:.2} GB/s", warmup_throughput);
    println!("Read hit rate: {:.1}% ({}/{})", hit_rate, found, reads);
    println!("Peak RSS: {:.2} GB", peak_rss);

    // Verify disk usage
    if let Ok(output) = std::process::Command::new("du")
        .args(["-sh", args.path.to_str().unwrap()])
        .output()
    {
        if let Ok(usage) = String::from_utf8(output.stdout) {
            println!();
            println!("Disk usage: {}", usage.trim());
        }
    }

    Ok(())
}
