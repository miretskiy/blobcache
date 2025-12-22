use clap::Parser;
use foyer::{HybridCache, HybridCacheBuilder, HybridCachePolicy};
use foyer_storage::{BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, Throttle};
use hdrhistogram::Histogram;
use leaky_bucket::RateLimiter;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of operations (default: 2560000 to match Go benchmark)
    #[arg(short, long, default_value_t = 2560000)]
    iterations: usize,

    /// Number of worker threads (default: num_cpus)
    #[arg(short, long)]
    workers: Option<usize>,

    /// Cache directory path
    #[arg(short, long, default_value = "/tmp/foyer-bench")]
    path: PathBuf,

    /// Cache capacity in GB (default: 256)
    #[arg(short, long, default_value_t = 256)]
    capacity_gb: usize,

    /// Rate limit writes (MB/s, 0 = unlimited)
    #[arg(long, default_value_t = 0)]
    write_rate_limit_mb: usize,
}

struct WorkerStats {
    num_writes: Arc<AtomicI64>,
    completed_writes: Arc<AtomicI64>,
    num_reads: Arc<AtomicI64>,
    num_found: Arc<AtomicI64>,
    total_bytes_written: Arc<AtomicI64>,
    hit_hist: Arc<Mutex<Histogram<u64>>>,
    miss_hist: Arc<Mutex<Histogram<u64>>>,
}

impl WorkerStats {
    fn new() -> Self {
        // Histograms: 1ns to 60 seconds, 3 significant digits
        let hit_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
        let miss_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();

        Self {
            num_writes: Arc::new(AtomicI64::new(0)),
            completed_writes: Arc::new(AtomicI64::new(0)),
            num_reads: Arc::new(AtomicI64::new(0)),
            num_found: Arc::new(AtomicI64::new(0)),
            total_bytes_written: Arc::new(AtomicI64::new(0)),
            hit_hist: Arc::new(Mutex::new(hit_hist)),
            miss_hist: Arc::new(Mutex::new(miss_hist)),
        }
    }
}

async fn run_worker(
    worker_id: usize,
    cache: Arc<HybridCache<Vec<u8>, Vec<u8>>>,
    stats: Arc<WorkerStats>,
    ops_per_worker: usize,
    total_ops: usize,
    rate_limiter: Option<Arc<RateLimiter>>,
) {
    let mut rng = StdRng::seed_from_u64(42 + worker_id as u64);
    let mut my_keys: Vec<i64> = Vec::with_capacity(1024);

    for _i in 0..ops_per_worker {
        let op = rng.gen_range(0..100);

        if op < 10 {
            // Rate limiting: acquire 1MB worth of tokens before write
            if let Some(limiter) = &rate_limiter {
                limiter.acquire(1024 * 1024).await;
            }

            // 10% writes - 1MB blobs
            let mut value = vec![0u8; 1024 * 1024];

            // Randomize some bytes (every 128KB) to prevent compression artifacts
            for i in (0..value.len()).step_by(128 * 1024) {
                value[i] = rng.gen::<u8>();
            }

            let key_id = stats.num_writes.fetch_add(1, Ordering::Relaxed);
            let key = format!("w-{}-key-{}", worker_id, key_id).into_bytes();

            cache.insert(key, value);
            stats.total_bytes_written.fetch_add(1024 * 1024, Ordering::Relaxed);
            my_keys.push(key_id);
            stats.completed_writes.fetch_add(1, Ordering::Relaxed);
        } else if op < 55 {
            // 45% reads from this worker's completed writes (hits)
            if !my_keys.is_empty() {
                let read_start = Instant::now();

                let idx = rng.gen_range(0..my_keys.len());
                let key_id = my_keys[idx];
                let key = format!("w-{}-key-{}", worker_id, key_id).into_bytes();

                stats.num_reads.fetch_add(1, Ordering::Relaxed);
                if let Ok(Some(entry)) = cache.get(&key).await {
                    // Read the value to match Go benchmark behavior
                    let _value = entry.value();
                    stats.num_found.fetch_add(1, Ordering::Relaxed);

                    let latency_ns = read_start.elapsed().as_nanos() as u64;
                    stats.hit_hist.lock().unwrap().record(latency_ns).ok();
                }
            }
        } else {
            // 45% reads with miss prefix (bloom filter test)
            let miss_start = Instant::now();

            let key_id = rng.gen_range(0..total_ops as i64);
            let key = format!("miss-{}", key_id).into_bytes();

            stats.num_reads.fetch_add(1, Ordering::Relaxed);  // Count all reads!
            if let Ok(Some(_)) = cache.get(&key).await {
                eprintln!("Expected missing, but found instead miss-{}", key_id);
                stats.num_found.fetch_add(1, Ordering::Relaxed);  // False positive
            }

            let latency_ns = miss_start.elapsed().as_nanos() as u64;
            stats.miss_hist.lock().unwrap().record(latency_ns).ok();
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // No tracing initialization - use official foyer without modifications

    let args = Args::parse();

    // Determine worker count (like Go's b.RunParallel)
    let num_workers = args.workers.unwrap_or_else(num_cpus::get);
    let ops_per_worker = args.iterations / num_workers;

    println!("=== Foyer Benchmark (Mixed Workload) ===");
    println!("Iterations: {}", args.iterations);
    println!("Workers: {}", num_workers);
    println!("Cache path: {:?}", args.path);
    println!("Cache capacity: {} GB", args.capacity_gb);
    println!("Expected writes: ~{} ({} GB)",
             args.iterations / 10,
             args.iterations / 10 / 1024);
    println!();

    // Clean up old benchmark data
    let _ = std::fs::remove_dir_all(&args.path);
    std::fs::create_dir_all(&args.path)?;

    // Set explicit disk capacity - 512GB to ensure we can hold 256GB + overhead
    // Explicitly disable throttling to ensure unlimited write rate
    let device = FsDeviceBuilder::new(&args.path)
        .with_capacity(512 * 1024 * 1024 * 1024)
        .with_throttle(Throttle::new())  // Unlimited (all None)
        .build()?;

    // Reasonable configuration for disk caching
    let engine = BlockEngineBuilder::new(device)
        .with_flushers(8)  // Reasonable parallelism
        .with_buffer_pool_size(8 * 1024 * 1024 * 1024)  // 8GB total (1GB per flusher)
        .with_submit_queue_size_threshold(8 * 1024 * 1024 * 1024);

    let cache: Arc<HybridCache<Vec<u8>, Vec<u8>>> = Arc::new(
        HybridCacheBuilder::new()
            .with_name("foyer-bench")
            // CRITICAL: WriteOnInsertion writes every entry to disk immediately
            // Default (WriteOnEviction) only writes when memory evicts
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            // Small memory tier (minimize memory-only caching)
            .memory(100 * 1024 * 1024) // 100MB
            .with_weighter(|_k: &Vec<u8>, v: &Vec<u8>| v.len())
            .storage()
            .with_engine_config(engine)
            .build()
            .await?,
    );

    let stats = Arc::new(WorkerStats::new());
    let start = Instant::now();

    // Create rate limiter if enabled
    let rate_limiter = if args.write_rate_limit_mb > 0 {
        println!("Write rate limit: {} MB/s", args.write_rate_limit_mb);
        // Create rate limiter: refills at write_rate_limit_mb tokens/sec
        // Max capacity = 2 seconds of burst
        let refill_rate = (args.write_rate_limit_mb * 1024 * 1024) as usize;
        Some(Arc::new(
            RateLimiter::builder()
                .initial(refill_rate / 10)  // Start with 100ms worth
                .refill(refill_rate / 10)  // Refill 100ms worth at a time
                .interval(std::time::Duration::from_millis(100))  // Refill every 100ms
                .max(refill_rate / 2)  // Max 500ms of burst
                .build()
        ))
    } else {
        None
    };

    println!("Starting benchmark...");

    // Spawn workers (match Go's b.RunParallel pattern)
    let mut handles = vec![];
    for worker_id in 0..num_workers {
        let cache_clone = Arc::clone(&cache);
        let stats_clone = Arc::clone(&stats);
        let total_ops = args.iterations;
        let limiter_clone = rate_limiter.clone();

        let handle = tokio::spawn(async move {
            run_worker(
                worker_id,
                cache_clone,
                stats_clone,
                ops_per_worker,
                total_ops,
                limiter_clone,
            ).await;
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.await?;
    }

    let workers_done = start.elapsed();
    println!("Workers finished in {:.2}s, now flushing buffer to disk...", workers_done.as_secs_f64());

    // Wait for pending writes to flush (like cache.Drain())
    // This is where actual disk I/O happens!
    cache.close().await?;

    let duration = start.elapsed();
    let flush_time = duration - workers_done;

    println!("Flush completed in {:.2}s", flush_time.as_secs_f64());

    // Report results
    println!();
    println!("=== Results ===");
    println!("Duration: {:.2}s", duration.as_secs_f64());
    println!("Total operations: {}", args.iterations);
    println!("Writes: {}", stats.num_writes.load(Ordering::Relaxed));
    println!("Completed writes: {}", stats.completed_writes.load(Ordering::Relaxed));
    println!("Reads: {}", stats.num_reads.load(Ordering::Relaxed));
    println!("Found (hits): {}", stats.num_found.load(Ordering::Relaxed));

    let writes = stats.num_writes.load(Ordering::Relaxed);
    let write_throughput_gb_s = (writes as f64) / duration.as_secs_f64() / 1024.0;
    let avg_latency_us = duration.as_micros() as f64 / args.iterations as f64;

    println!();
    println!("Write throughput: {:.2} GB/s", write_throughput_gb_s);
    println!("Average latency: {:.2} µs", avg_latency_us);
    println!("Operations/sec: {:.0}", args.iterations as f64 / duration.as_secs_f64());
    println!("Worker time: {:.2}s", workers_done.as_secs_f64());
    println!("Flush time: {:.2}s", flush_time.as_secs_f64());

    // Read latency percentiles
    println!();
    let hit_hist = stats.hit_hist.lock().unwrap();
    if hit_hist.len() > 0 {
        println!("Hit latencies (µs):");
        println!("  avg: {:.2}", hit_hist.mean() / 1000.0);
        println!("  p50: {:.2}", hit_hist.value_at_quantile(0.50) as f64 / 1000.0);
        println!("  p90: {:.2}", hit_hist.value_at_quantile(0.90) as f64 / 1000.0);
        println!("  p95: {:.2}", hit_hist.value_at_quantile(0.95) as f64 / 1000.0);
        println!("  p99: {:.2}", hit_hist.value_at_quantile(0.99) as f64 / 1000.0);
        println!("  max: {:.2}", hit_hist.max() as f64 / 1000.0);
    }

    let miss_hist = stats.miss_hist.lock().unwrap();
    if miss_hist.len() > 0 {
        println!("Miss latencies (ns):");
        println!("  avg: {:.0}", miss_hist.mean());
        println!("  p50: {}", miss_hist.value_at_quantile(0.50));
        println!("  p90: {}", miss_hist.value_at_quantile(0.90));
        println!("  p95: {}", miss_hist.value_at_quantile(0.95));
        println!("  p99: {}", miss_hist.value_at_quantile(0.99));
        println!("  max: {}", miss_hist.max());
    }

    let total_reads = stats.num_reads.load(Ordering::Relaxed);
    let hits = stats.num_found.load(Ordering::Relaxed);
    if total_reads > 0 {
        let hit_rate = hits as f64 / total_reads as f64 * 100.0;
        println!("Hit rate: {:.1}%", hit_rate);
    }

    // Verify disk usage
    if let Ok(output) = std::process::Command::new("du")
        .args(&["-sh", args.path.to_str().unwrap()])
        .output()
    {
        if let Ok(usage) = String::from_utf8(output.stdout) {
            println!();
            println!("Disk usage: {}", usage.trim());
        }
    }

    Ok(())
}
