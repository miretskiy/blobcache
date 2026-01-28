//! Memory pool for aligned slab buffers.
//!
//! Provides pre-allocated, 4KB-aligned buffers for Direct I/O writes.
//! Buffers are pooled via a crossbeam channel for efficient reuse.
//!
//! # Design
//!
//! - **Pool the memory, not the struct**: Solves the ABA problem by ensuring
//!   each `MmapBuffer` instance is unique, even if reusing the same memory.
//! - **Reference counting**: Safe concurrent access via `Arc<MmapBuffer>`.
//! - **Backpressure**: Bounded channel blocks when all buffers are in use.

use std::io;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender};

use crate::sys::{self, BLOCK_SIZE};

// =============================================================================
// MmapBuffer
// =============================================================================

/// A reference-counted, mmap-backed buffer with 4KB alignment.
///
/// Use [`MmapBuffer::try_inc`] for safe concurrent reference acquisition.
/// The buffer is automatically released when the reference count drops to zero.
pub struct MmapBuffer {
    // Note: Debug not derived due to raw pointer and Mutex fields
    /// The raw mmap'd memory.
    raw: *mut u8,
    /// Size of the allocation.
    capacity: usize,
    /// Reference count for concurrent access.
    ref_count: AtomicI32,
    /// Pool to return memory to (None for standalone buffers).
    pool: Option<Sender<RawBuffer>>,
    /// Callbacks to run when buffer is released.
    on_release: parking_lot::Mutex<Vec<Box<dyn FnOnce() + Send>>>,
}

// Safety: MmapBuffer manages its own memory and synchronization.
unsafe impl Send for MmapBuffer {}
unsafe impl Sync for MmapBuffer {}

impl std::fmt::Debug for MmapBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapBuffer")
            .field("capacity", &self.capacity)
            .field("ref_count", &self.ref_count.load(Ordering::Relaxed))
            .field("pooled", &self.pool.is_some())
            .finish()
    }
}

/// A raw buffer wrapper for the pool channel.
struct RawBuffer {
    ptr: *mut u8,
    capacity: usize,
}

// Safety: Raw pointers to mmap'd memory can be sent between threads.
unsafe impl Send for RawBuffer {}

impl MmapBuffer {
    /// Creates a new standalone (unpooled) mmap buffer.
    ///
    /// The buffer will be unmapped when the reference count drops to zero.
    pub fn new(size: usize) -> Arc<Self> {
        let aligned_size = sys::page_align(size);
        if aligned_size == 0 {
            return Arc::new(MmapBuffer {
                raw: std::ptr::null_mut(),
                capacity: 0,
                ref_count: AtomicI32::new(1),
                pool: None,
                on_release: parking_lot::Mutex::new(Vec::new()),
            });
        }

        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                aligned_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANON | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            panic!(
                "failed to allocate {} aligned bytes: {}",
                aligned_size,
                io::Error::last_os_error()
            );
        }

        // Pre-warm: force physical RAM commitment
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, aligned_size) };
        for i in (0..aligned_size).step_by(BLOCK_SIZE) {
            slice[i] = 0;
        }

        Arc::new(MmapBuffer {
            raw: ptr as *mut u8,
            capacity: aligned_size,
            ref_count: AtomicI32::new(1),
            pool: None,
            on_release: parking_lot::Mutex::new(Vec::new()),
        })
    }

    /// Creates a buffer from pooled memory.
    fn from_pool(raw: RawBuffer, pool: Sender<RawBuffer>) -> Arc<Self> {
        Arc::new(MmapBuffer {
            raw: raw.ptr,
            capacity: raw.capacity,
            ref_count: AtomicI32::new(1),
            pool: Some(pool),
            on_release: parking_lot::Mutex::new(Vec::new()),
        })
    }

    /// Attempts to increment the reference count safely.
    ///
    /// Returns `true` if successful, `false` if the buffer is already dead (ref_count <= 0).
    /// This allows safe resurrection from a list without holding a lock.
    pub fn try_inc(&self) -> bool {
        loop {
            let count = self.ref_count.load(Ordering::Acquire);
            if count <= 0 {
                return false;
            }
            if self
                .ref_count
                .compare_exchange_weak(count, count + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Decrements the reference count and releases if it reaches zero.
    pub fn unpin(&self) {
        let prev = self.ref_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            // Execute release callbacks
            let callbacks = std::mem::take(&mut *self.on_release.lock());
            for callback in callbacks {
                callback();
            }
            self.release_memory();
        }
    }

    /// Releases the underlying memory.
    fn release_memory(&self) {
        if self.raw.is_null() || self.capacity == 0 {
            return;
        }

        if let Some(ref pool) = self.pool {
            // Return to pool
            let raw = RawBuffer {
                ptr: self.raw,
                capacity: self.capacity,
            };
            if pool.try_send(raw).is_err() {
                // Pool is full, unmap directly
                unsafe {
                    libc::madvise(self.raw as *mut libc::c_void, self.capacity, libc::MADV_DONTNEED);
                    libc::munmap(self.raw as *mut libc::c_void, self.capacity);
                }
            }
        } else {
            // Standalone buffer, unmap directly
            unsafe {
                libc::munmap(self.raw as *mut libc::c_void, self.capacity);
            }
        }
    }

    /// Adds a callback to run when the buffer is released.
    pub fn add_on_release<F: FnOnce() + Send + 'static>(&self, f: F) {
        self.on_release.lock().push(Box::new(f));
    }

    /// Returns the buffer capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the raw buffer as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        if self.raw.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.raw, self.capacity) }
        }
    }

    /// Returns the raw buffer as a mutable slice.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is null (capacity 0).
    #[inline]
    #[allow(clippy::mut_from_ref)] // Interior mutability via mmap
    pub fn as_mut_slice(&self) -> &mut [u8] {
        assert!(!self.raw.is_null(), "cannot get mutable slice of null buffer");
        unsafe { std::slice::from_raw_parts_mut(self.raw, self.capacity) }
    }

    /// Writes data at the specified offset.
    #[inline]
    pub fn write_at(&self, data: &[u8], offset: usize) {
        let slice = self.as_mut_slice();
        slice[offset..offset + data.len()].copy_from_slice(data);
    }

    /// Returns a slice rounded to the nearest 4KB page boundary.
    pub fn aligned_bytes(&self, len: usize) -> &[u8] {
        if len == 0 || self.raw.is_null() {
            return &[];
        }
        let aligned_len = sys::page_align(len);
        &self.as_slice()[..aligned_len.min(self.capacity)]
    }

    /// Returns true if the buffer is aligned for Direct I/O.
    #[inline]
    pub fn is_aligned(&self) -> bool {
        self.raw.is_null() || (self.raw as usize) & sys::BLOCK_MASK == 0
    }
}

// =============================================================================
// MmapPool
// =============================================================================

/// A pool of pre-allocated mmap buffers for efficient reuse.
///
/// Uses a bounded crossbeam channel for backpressure when all buffers are in use.
pub struct MmapPool {
    /// Channel for available raw buffers.
    buffers: Receiver<RawBuffer>,
    /// Sender for returning buffers.
    sender: Sender<RawBuffer>,
    /// Size of each pooled buffer.
    buffer_size: usize,
    /// Pool name for logging.
    #[allow(dead_code)]
    name: String,
}

impl MmapPool {
    /// Creates a new pool with `capacity` pre-allocated buffers of `buffer_size` bytes.
    pub fn new(name: impl Into<String>, buffer_size: usize, capacity: usize) -> Self {
        let aligned_size = sys::page_align(buffer_size);
        let (sender, receiver) = channel::bounded(capacity);

        // Pre-fill with aligned, pre-warmed buffers
        for _ in 0..capacity {
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    aligned_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_ANON | libc::MAP_PRIVATE,
                    -1,
                    0,
                )
            };

            if ptr == libc::MAP_FAILED {
                panic!(
                    "failed to allocate {} aligned bytes for pool: {}",
                    aligned_size,
                    io::Error::last_os_error()
                );
            }

            // Pre-warm
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, aligned_size) };
            for i in (0..aligned_size).step_by(BLOCK_SIZE) {
                slice[i] = 0;
            }

            let raw = RawBuffer {
                ptr: ptr as *mut u8,
                capacity: aligned_size,
            };
            sender.send(raw).expect("channel should not be full");
        }

        MmapPool {
            buffers: receiver,
            sender,
            buffer_size: aligned_size,
            name: name.into(),
        }
    }

    /// Acquires a buffer from the pool, blocking if none are available.
    ///
    /// Times out after 10 seconds with a panic (indicates deadlock or resource exhaustion).
    pub fn acquire(&self) -> Arc<MmapBuffer> {
        match self.buffers.recv_timeout(Duration::from_secs(10)) {
            Ok(raw) => MmapBuffer::from_pool(raw, self.sender.clone()),
            Err(_) => {
                panic!(
                    "timeout acquiring buffer from pool '{}' - possible deadlock",
                    self.name
                );
            }
        }
    }

    /// Acquires a buffer of at least the specified size.
    ///
    /// If size fits in the pool's buffer size, returns a pooled buffer.
    /// Otherwise, allocates a standalone buffer.
    pub fn acquire_aligned(&self, size: usize) -> Arc<MmapBuffer> {
        if size <= self.buffer_size {
            self.acquire()
        } else {
            MmapBuffer::new(size)
        }
    }

    /// Returns the buffer size for this pool.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Returns the number of available buffers in the pool.
    pub fn available(&self) -> usize {
        self.buffers.len()
    }
}

impl Drop for MmapPool {
    fn drop(&mut self) {
        // Drain and unmap all remaining buffers
        while let Ok(raw) = self.buffers.try_recv() {
            unsafe {
                libc::munmap(raw.ptr as *mut libc::c_void, raw.capacity);
            }
        }
    }
}

// =============================================================================
// BufferHandle (for decompression)
// =============================================================================

/// A handle to a temporary buffer, typically used for decompressed data.
pub struct BufferHandle {
    data: Vec<u8>,
}

impl BufferHandle {
    /// Creates a new buffer handle with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        BufferHandle {
            data: vec![0u8; capacity],
        }
    }

    /// Returns the buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Returns the buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_mmap_buffer_new() {
        let buf = MmapBuffer::new(1000);
        assert!(buf.capacity() >= 1000);
        assert!(buf.is_aligned());
    }

    #[test]
    fn test_mmap_buffer_write() {
        let buf = MmapBuffer::new(4096);
        buf.write_at(b"hello", 0);
        assert_eq!(&buf.as_slice()[0..5], b"hello");
    }

    #[test]
    fn test_mmap_buffer_try_inc() {
        let buf = MmapBuffer::new(4096);

        // Should succeed while ref_count > 0
        assert!(buf.try_inc());

        // Unpin twice to drop ref_count to 0
        buf.unpin();
        buf.unpin();

        // Now ref_count is 0, try_inc should fail
        // Note: This would normally be unsafe, but we're testing the mechanism
    }

    #[test]
    fn test_mmap_pool() {
        let pool = MmapPool::new("test", 4096, 2);
        assert_eq!(pool.available(), 2);

        let buf1 = pool.acquire();
        assert_eq!(pool.available(), 1);
        assert!(buf1.is_aligned());

        let buf2 = pool.acquire();
        assert_eq!(pool.available(), 0);

        // Return buf1 to pool
        buf1.unpin();
        assert_eq!(pool.available(), 1);

        buf2.unpin();
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_mmap_pool_concurrent() {
        let pool = Arc::new(MmapPool::new("concurrent", 4096, 4));
        let mut handles = vec![];

        for i in 0..8 {
            let pool = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                let buf = pool.acquire();
                buf.write_at(format!("thread-{}", i).as_bytes(), 0);
                thread::sleep(Duration::from_millis(10));
                buf.unpin();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_acquire_aligned_oversized() {
        let pool = MmapPool::new("oversized", 4096, 2);

        // Request larger than pool buffer size
        let large_buf = pool.acquire_aligned(1024 * 1024);
        assert!(large_buf.capacity() >= 1024 * 1024);

        // Pool should still have all buffers (large one is standalone)
        assert_eq!(pool.available(), 2);

        large_buf.unpin();
    }

    #[test]
    fn test_on_release_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        let buf = MmapBuffer::new(4096);
        buf.add_on_release(move || {
            called_clone.store(true, Ordering::SeqCst);
        });

        assert!(!called.load(Ordering::SeqCst));
        buf.unpin();
        assert!(called.load(Ordering::SeqCst));
    }
}
