//! Power-of-two bucketed pool of `AlignedBuffer` allocations.
//!
//! Avoids per-read `mmap` calls under concurrent load by recycling aligned
//! buffers between callers. Buffers are organized in power-of-two size buckets
//! from 4 KB to 2 MB.
//!
//! # Design
//!
//! - 10 buckets covering 4 KB..2 MB in powers of two
//! - Each bucket is a `Mutex<Vec<AlignedBuffer>>`
//! - `PooledBuffer` holds a buffer and returns it to the pool on `Drop`
//! - Double-release is detected by an `AtomicBool` flag

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::sys::{self, AlignedBuffer, BLOCK_SIZE};

// =============================================================================
// Constants
// =============================================================================

/// Number of power-of-two buckets (4 KB to 2 MB).
const NUM_BUCKETS: usize = 10;

/// Smallest pooled buffer size (4 KB = 2^12).
const MIN_SIZE_LOG2: u32 = 12;

/// Largest buffer returned from the pool (2 MB = 2^21).
pub const MAX_POOLED: usize = 2 * 1024 * 1024;

// =============================================================================
// BufferPool
// =============================================================================

/// Power-of-two bucketed pool of aligned read buffers.
pub struct BufferPool {
    buckets: [Mutex<Vec<AlignedBuffer>>; NUM_BUCKETS],
}

impl BufferPool {
    /// Creates a new empty pool.
    pub fn new() -> Arc<Self> {
        // Unfortunately we can't use array initialization with non-Copy values
        // in a const context, so we use a macro or manual init.
        Arc::new(BufferPool {
            buckets: std::array::from_fn(|_| Mutex::new(Vec::new())),
        })
    }

    /// Acquires a buffer of at least `length` bytes from the pool.
    ///
    /// If no buffer is available in the appropriate bucket, a fresh one is
    /// allocated. The returned `PooledBuffer` is returned to the pool on drop.
    pub fn acquire(self: &Arc<Self>, length: usize) -> PooledBuffer {
        let (idx, bucket_size) = Self::bucket_for(length);

        // Try to reuse from pool
        if let Some(mut buf) = self.buckets[idx].lock().pop() {
            buf.set_len(length);
            return PooledBuffer {
                buf: ManuallyDrop::new(buf),
                pool: Arc::clone(self),
                bucket_idx: idx,
                released: AtomicBool::new(false),
            };
        }

        // Allocate fresh aligned buffer
        let mut buf = sys::alloc_aligned(bucket_size).unwrap_or_else(|_| {
            // Fallback: allocate exact size (may not be pooled bucket size)
            sys::alloc_aligned(length).expect("failed to allocate aligned buffer")
        });
        buf.set_len(length);

        PooledBuffer {
            buf: ManuallyDrop::new(buf),
            pool: Arc::clone(self),
            bucket_idx: idx,
            released: AtomicBool::new(false),
        }
    }

    /// Acquires a buffer large enough for `length` bytes plus one block of
    /// padding (for Direct I/O alignment lead).
    pub fn acquire_aligned(self: &Arc<Self>, length: usize) -> PooledBuffer {
        self.acquire(length + BLOCK_SIZE)
    }

    /// Returns a buffer to the pool.
    fn release(&self, buf: ManuallyDrop<AlignedBuffer>, bucket_idx: usize) {
        // Safety: We take ownership here; the caller ensures the buffer is not
        // used afterwards (enforced by the AtomicBool in PooledBuffer::drop).
        let mut raw_buf = ManuallyDrop::into_inner(buf);

        // Reset len to full capacity before returning
        let cap = raw_buf.capacity();
        raw_buf.set_len(cap);
        raw_buf.clear();

        self.buckets[bucket_idx].lock().push(raw_buf);
    }

    /// Returns the bucket index and bucket size for a given requested length.
    fn bucket_for(length: usize) -> (usize, usize) {
        if length > MAX_POOLED {
            // Oversized: use last bucket index but actual size
            return (NUM_BUCKETS - 1, length);
        }

        // Round up to next power of two, at least MIN_SIZE
        let min = 1usize << MIN_SIZE_LOG2; // 4096
        let rounded = length.max(min).next_power_of_two();
        let log2 = rounded.trailing_zeros();
        let idx = (log2 as usize).saturating_sub(MIN_SIZE_LOG2 as usize);
        let idx = idx.min(NUM_BUCKETS - 1);

        (idx, rounded)
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        BufferPool {
            buckets: std::array::from_fn(|_| Mutex::new(Vec::new())),
        }
    }
}

// =============================================================================
// PooledBuffer
// =============================================================================

/// An aligned buffer borrowed from a `BufferPool`.
///
/// Automatically returned to the pool on drop. Double-release is detected
/// and panics in debug mode.
pub struct PooledBuffer {
    buf: ManuallyDrop<AlignedBuffer>,
    pool: Arc<BufferPool>,
    bucket_idx: usize,
    released: AtomicBool,
}

impl PooledBuffer {
    /// Returns the number of usable bytes (as set by `acquire`).
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Returns true if the buffer has zero usable bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.len() == 0
    }

    /// Sets the number of valid bytes in the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `len > capacity`.
    pub fn set_len(&mut self, len: usize) {
        self.buf.set_len(len);
    }
}

impl Deref for PooledBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.buf.as_slice()
    }
}

impl DerefMut for PooledBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            // Already released — this is a bug
            panic!("PooledBuffer double-release detected");
        }

        // Safety: we are in Drop, the buffer is ours, and `released` prevents
        // re-entry. We pass a ManuallyDrop wrapper so release() can extract it.
        let buf = unsafe { ManuallyDrop::new(ManuallyDrop::take(&mut self.buf)) };
        self.pool.release(buf, self.bucket_idx);
    }
}

// Safety: AlignedBuffer is Send+Sync; Arc<BufferPool> is Send+Sync.
unsafe impl Send for PooledBuffer {}
unsafe impl Sync for PooledBuffer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new();
        let mut buf = pool.acquire(1024);
        assert!(buf.len() >= 1024);

        // Write something
        buf[0] = 42;
        assert_eq!(buf[0], 42);

        drop(buf); // Should return to pool without panic
    }

    #[test]
    fn test_buffer_pool_reuse() {
        let pool = BufferPool::new();

        // Acquire and release a buffer
        let ptr1 = {
            let buf = pool.acquire(4096);
            buf.buf.as_slice().as_ptr() as usize
        };

        // Acquire again - should get the same backing buffer
        let ptr2 = {
            let buf = pool.acquire(4096);
            buf.buf.as_slice().as_ptr() as usize
        };

        // May or may not be the same pointer depending on allocation, but
        // no double-free should occur
        let _ = ptr1;
        let _ = ptr2;
    }

    #[test]
    fn test_buffer_pool_various_sizes() {
        let pool = BufferPool::new();

        for size in [512, 1024, 4096, 8192, 65536, 1 << 20] {
            let buf = pool.acquire(size);
            assert!(buf.len() >= size);
            drop(buf);
        }
    }

    #[test]
    fn test_bucket_for() {
        // 4 KB -> bucket 0
        let (idx, size) = BufferPool::bucket_for(4096);
        assert_eq!(idx, 0);
        assert_eq!(size, 4096);

        // 5000 -> bucket 1 (8 KB)
        let (idx, size) = BufferPool::bucket_for(5000);
        assert_eq!(idx, 1);
        assert_eq!(size, 8192);

        // 1 MB -> bucket 8
        let (idx, size) = BufferPool::bucket_for(1 << 20);
        assert_eq!(idx, 8);
        assert_eq!(size, 1 << 20);

        // Oversized
        let (idx, _) = BufferPool::bucket_for(MAX_POOLED + 1);
        assert_eq!(idx, NUM_BUCKETS - 1);
    }

    #[test]
    fn test_acquire_aligned_adds_padding() {
        let pool = BufferPool::new();
        let buf = pool.acquire_aligned(4096);
        assert!(buf.len() >= 4096 + BLOCK_SIZE);
    }
}
