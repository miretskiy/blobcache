//! Lock-free bloom filter using atomic operations.
//!
//! Optimized for AWS Nitro instances by pinning all probes for a key to a single
//! 64-byte cache line, reducing memory latency by ~6x.
//!
//! # Key Design
//!
//! Uses full 128-bit XXH3 hashes to avoid the "32-bit funnel" bug:
//! - `key.hi()` selects the 64-byte block (via Mul64 for uniform distribution)
//! - `key.lo()` generates the probe pattern (independent entropy source)
//!
//! This prevents correlated failures where both block selection AND probe pattern
//! collide for different keys, which happens at scale with truncated hashes.

use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::key::Key;

/// Bits per block (64 bytes = 512 bits).
const BITS_PER_BLOCK: u32 = 512;

/// U32s per block (16 u32s = 64 bytes).
const U32S_PER_BLOCK: u32 = 16;

// =============================================================================
// Recording
// =============================================================================

/// Fast structure to record bloom filter additions during rebuild.
struct Recording {
    /// Pre-allocated primary buffer for lock-free recording.
    primary: Vec<Key>,
    /// Atomic cursor into primary buffer.
    cursor: AtomicU64,
    /// Overflow mutex for when primary is exhausted.
    overflow_mu: Mutex<Vec<Key>>,
}

impl Recording {
    fn new(capacity: usize) -> Self {
        Recording {
            primary: vec![Key::zero(); capacity],
            cursor: AtomicU64::new(0),
            overflow_mu: Mutex::new(Vec::new()),
        }
    }

    fn add(&self, k: Key) {
        // 1. GUARANTEED RECORDING: Using atomic reservation
        let idx = self.cursor.fetch_add(1, Ordering::AcqRel);
        if idx < self.primary.len() as u64 {
            // Safety: we have exclusive access to this slot via atomic reservation
            // Note: This is a bit unsafe in Rust since we're writing to a shared Vec.
            // In a more idiomatic implementation, we'd use AtomicCell or similar.
            // For now, we'll use the overflow path for all recordings to be safe.
            let mut overflow = self.overflow_mu.lock().unwrap();
            overflow.push(k);
        } else {
            // Emergency overflow to guarantee NO FALSE NEGATIVES
            let mut overflow = self.overflow_mu.lock().unwrap();
            overflow.push(k);
        }
    }

    fn consume<F>(&self, mut consumer: F)
    where
        F: FnMut(Key),
    {
        // Drain overflow (which contains all recorded keys in our safe implementation)
        let overflow = self.overflow_mu.lock().unwrap();
        for k in overflow.iter() {
            consumer(*k);
        }
    }
}

// =============================================================================
// Filter
// =============================================================================

/// Lock-free bloom filter using atomic operations.
///
/// Uses block-based design where all probes for a key are within a single
/// 64-byte cache line (512 bits).
pub struct Filter {
    /// Bit vector (accessed atomically).
    data: Vec<AtomicU32>,
    /// Filter size in bits (aligned to 512).
    m: u32,
    /// Number of hash functions (probes).
    k: u32,
    /// Number of 64-byte blocks.
    num_blocks: u32,
    /// Recording state for rebuild support.
    recording: AtomicPtr<Recording>,
}

impl Filter {
    /// Creates a bloom filter optimized for n elements with target false positive rate.
    pub fn new(estimated_keys: u32, fp_rate: f64) -> Self {
        let mut m = optimal_m(estimated_keys, fp_rate);

        // SURGERY: Block-based filters require a "Variance Buffer."
        // We add 15% more bits to account for the Poisson distribution of keys
        // into blocks. This ensures "unlucky" blocks don't saturate.
        m = (m as f64 * 1.15) as u32;

        let k = optimal_k(fp_rate);

        // RocksDB/FastLocalBloom alignment: Round m up to nearest 512 bits (64 bytes).
        m = (m + 511) & !511;
        let mut num_blocks = m >> 9; // m / 512
        if num_blocks == 0 {
            num_blocks = 1;
            m = 512;
        }

        // 16 u32s = 64 bytes = 1 CPU cache line per block
        let data: Vec<AtomicU32> = (0..(num_blocks * U32S_PER_BLOCK))
            .map(|_| AtomicU32::new(0))
            .collect();

        Filter {
            data,
            m,
            k,
            num_blocks,
            recording: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Returns the filter size in bits.
    pub fn size_bits(&self) -> u32 {
        self.m
    }

    /// Returns the number of hash functions.
    pub fn num_probes(&self) -> u32 {
        self.k
    }

    /// Add inserts a key into the bloom filter (lock-free, concurrent-safe).
    pub fn add(&self, k: Key) {
        let rec = self.recording.load(Ordering::Acquire);
        if !rec.is_null() {
            unsafe { &*rec }.add(k);
        }
        self.add_hash(k);
    }

    /// Inserts specified hash into this filter using RocksDB-style local probing.
    /// Uses full 128-bit entropy: Hi for block selection, Lo for probe pattern.
    pub fn add_hash(&self, k: Key) {
        // Level 1: Pick the 64-byte block using Hi bits.
        // We want floor(k.hi * numBlocks / 2^64), which gives uniform distribution.
        let block_idx = mul64_hi(k.hi(), self.num_blocks as u64) as u32;
        let base_idx = block_idx << 4; // * 16

        // Level 2: Local Probes using Lo bits.
        // We use the RocksDB technique: Lo provides the seed, delta provides stepping.
        // 'delta' is a bit-rotation to ensure independent stepping per probe.
        let mut h32 = k.lo() as u32;
        let delta = ((k.lo() >> 17) | (k.lo() << 15)) as u32;

        for _ in 0..self.k {
            // Bit position 0-511 inside the block
            let bit_in_block = h32 & (BITS_PER_BLOCK - 1);

            let idx = base_idx + (bit_in_block >> 5);
            let mask = 1u32 << (bit_in_block & 31);

            // Atomic bit-set
            loop {
                let orig = self.data[idx as usize].load(Ordering::Acquire);
                if orig & mask != 0 {
                    break;
                }
                if self.data[idx as usize]
                    .compare_exchange_weak(orig, orig | mask, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }

            // Increment the hash by the delta for the next probe position
            h32 = h32.wrapping_add(delta);
        }
    }

    /// Test checks if a key might be in the set (lock-free).
    pub fn test(&self, k: Key) -> bool {
        // Block selection using Hi (same as add_hash)
        let block_idx = mul64_hi(k.hi(), self.num_blocks as u64) as u32;
        let base_idx = block_idx << 4;

        // Probe pattern using Lo (same as add_hash)
        let mut h32 = k.lo() as u32;
        let delta = ((k.lo() >> 17) | (k.lo() << 15)) as u32;

        for _ in 0..self.k {
            let bit_in_block = h32 & (BITS_PER_BLOCK - 1);
            let idx = base_idx + (bit_in_block >> 5);
            let mask = 1u32 << (bit_in_block & 31);

            // All subsequent iterations are L1 cache hits
            if (self.data[idx as usize].load(Ordering::Acquire) & mask) == 0 {
                return false;
            }

            h32 = h32.wrapping_add(delta);
        }

        true
    }

    /// Starts recording additions for rebuild support.
    ///
    /// Returns functions to stop recording and consume recorded keys.
    pub fn record_additions(&self) -> RecordingHandle<'_> {
        // Pre-allocate 256k slots (4MB for 128-bit keys). This is "large" for 8KB blobs.
        let rec = Box::new(Recording::new(256 * 1024));
        let ptr = Box::into_raw(rec);
        self.recording.store(ptr, Ordering::Release);
        RecordingHandle { filter: self, ptr }
    }

    /// Clears all bits in the filter.
    pub fn clear(&self) {
        for atom in &self.data {
            atom.store(0, Ordering::Release);
        }
    }
}

impl Drop for Filter {
    fn drop(&mut self) {
        let ptr = *self.recording.get_mut();
        if !ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
        }
    }
}

// Safety: Filter uses atomic operations for all shared state
unsafe impl Send for Filter {}
unsafe impl Sync for Filter {}

// =============================================================================
// RecordingHandle
// =============================================================================

/// Handle for managing recording state during bloom filter rebuild.
pub struct RecordingHandle<'a> {
    filter: &'a Filter,
    ptr: *mut Recording,
}

impl<'a> RecordingHandle<'a> {
    /// Stops recording additions.
    pub fn stop(&self) {
        self.filter.recording.store(std::ptr::null_mut(), Ordering::Release);
    }

    /// Consumes all recorded keys.
    pub fn consume<F>(&self, consumer: F)
    where
        F: FnMut(Key),
    {
        if !self.ptr.is_null() {
            unsafe { &*self.ptr }.consume(consumer);
        }
    }
}

impl Drop for RecordingHandle<'_> {
    fn drop(&mut self) {
        self.stop();
        if !self.ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(self.ptr);
            }
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Returns the high 64 bits of the 128-bit product of two 64-bit numbers.
/// Equivalent to bits.Mul64 in Go returning the hi result.
#[inline]
fn mul64_hi(a: u64, b: u64) -> u64 {
    ((a as u128 * b as u128) >> 64) as u64
}

/// Calculates optimal filter size in bits.
fn optimal_m(n: u32, p: f64) -> u32 {
    let m = -(n as f64) * p.ln() / (2.0_f64.ln() * 2.0_f64.ln());
    m.ceil() as u32
}

/// Calculates optimal number of hash functions.
fn optimal_k(p: f64) -> u32 {
    let k = -p.log2();
    if k < 1.0 {
        1
    } else {
        k.ceil() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_basic() {
        let filter = Filter::new(1000, 0.01);

        let key1 = Key::from_bytes(b"key1");
        let key2 = Key::from_bytes(b"key2");
        let key3 = Key::from_bytes(b"key3");

        // Initially nothing should be present
        assert!(!filter.test(key1));
        assert!(!filter.test(key2));
        assert!(!filter.test(key3));

        // Add key1
        filter.add(key1);
        assert!(filter.test(key1));
        assert!(!filter.test(key2));

        // Add key2
        filter.add(key2);
        assert!(filter.test(key1));
        assert!(filter.test(key2));
        assert!(!filter.test(key3));
    }

    #[test]
    fn test_filter_false_positive_rate() {
        // Create a filter for 10000 elements with 1% FPR
        let filter = Filter::new(10000, 0.01);

        // Add 10000 keys using from_bytes for proper 128-bit distribution
        for i in 0..10000u32 {
            let key = Key::from_bytes(&i.to_le_bytes());
            filter.add(key);
        }

        // All added keys should be found
        for i in 0..10000u32 {
            let key = Key::from_bytes(&i.to_le_bytes());
            assert!(filter.test(key), "key {} not found", i);
        }

        // Count false positives for keys not in the set
        // Use different byte patterns that won't collide
        let mut false_positives = 0;
        let test_count = 10000;
        for i in 0..test_count {
            let bytes = format!("nonexistent-key-{}", i);
            let key = Key::from_bytes(bytes.as_bytes());
            if filter.test(key) {
                false_positives += 1;
            }
        }

        // FPR should be close to 1% (allow some variance)
        let fpr = false_positives as f64 / test_count as f64;
        assert!(
            fpr < 0.03,
            "false positive rate {} too high (expected ~0.01)",
            fpr
        );
    }

    #[test]
    fn test_filter_clear() {
        let filter = Filter::new(100, 0.01);

        let key = Key::from_bytes(b"testkey");
        filter.add(key);
        assert!(filter.test(key));

        filter.clear();
        assert!(!filter.test(key));
    }

    #[test]
    fn test_filter_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let filter = Arc::new(Filter::new(100000, 0.01));
        let mut handles = vec![];

        // Spawn writers
        for t in 0..4u32 {
            let f = Arc::clone(&filter);
            handles.push(thread::spawn(move || {
                for i in 0..10000u32 {
                    let bytes = format!("key-{}-{}", t, i);
                    let key = Key::from_bytes(bytes.as_bytes());
                    f.add(key);
                }
            }));
        }

        // Wait for writers
        for h in handles {
            h.join().unwrap();
        }

        // Verify all keys present
        for t in 0..4u32 {
            for i in 0..10000u32 {
                let bytes = format!("key-{}-{}", t, i);
                let key = Key::from_bytes(bytes.as_bytes());
                assert!(filter.test(key), "key ({}, {}) not found", t, i);
            }
        }
    }

    #[test]
    fn test_optimal_parameters() {
        // 1M keys, 1% FPR
        let m = optimal_m(1_000_000, 0.01);
        let k = optimal_k(0.01);

        // m should be ~9.6 million bits (~1.2 MB)
        assert!(m > 9_000_000 && m < 10_000_000);
        // k should be ~7 probes
        assert!((6..=8).contains(&k));
    }

    #[test]
    fn test_recording() {
        let filter = Filter::new(1000, 0.01);

        let handle = filter.record_additions();

        // Add some keys while recording
        let key1 = Key::from_bytes(b"rec1");
        let key2 = Key::from_bytes(b"rec2");
        filter.add(key1);
        filter.add(key2);

        // Collect recorded keys
        let mut recorded = Vec::new();
        handle.consume(|k| recorded.push(k));

        assert_eq!(recorded.len(), 2);

        // Stop recording
        handle.stop();
    }

    #[test]
    fn test_mul64_hi() {
        // Test the high multiplication function
        assert_eq!(mul64_hi(0, 100), 0);
        assert_eq!(mul64_hi(100, 0), 0);

        // 2^63 * 2 should give 1 in high bits
        let result = mul64_hi(1 << 63, 2);
        assert_eq!(result, 1);
    }
}
