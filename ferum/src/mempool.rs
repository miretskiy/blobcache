//! Memory pool for aligned slab buffers.
//!
//! Provides pre-allocated, 4KB-aligned buffers for Direct I/O writes.
//! Buffers are pooled via a crossbeam channel for efficient reuse.
//!
//! # Design
//!
//! - **Pool the memory, not the struct**: Solves the ABA problem by ensuring
//!   each `MmapBuffer` instance is unique, even if reusing the same memory.
//! - **Arc-based lifetime**: Use Rust's `Arc<MmapBuffer>` for reference counting.
//!   When the last Arc is dropped, the buffer's memory is returned to the pool.
//! - **Backpressure**: Bounded channel blocks when all buffers are in use.

use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender};

use crate::error::{Error, Result};
use crate::sys::{self, BLOCK_SIZE};

// =============================================================================
// MmapBuffer
// =============================================================================

/// An mmap-backed buffer with 4KB alignment for Direct I/O.
///
/// Lifetime is managed by `Arc<MmapBuffer>`. When the last Arc reference is
/// dropped, the buffer's memory is automatically returned to the pool (if pooled)
/// or unmapped (if standalone).
pub struct MmapBuffer {
    /// The raw mmap'd memory.
    raw: *mut u8,
    /// Size of the allocation.
    capacity: usize,
    /// Pool to return memory to (None for standalone buffers).
    pool: Option<Sender<RawBuffer>>,
    /// Callbacks to run when buffer is released.
    on_release: parking_lot::Mutex<Vec<Box<dyn FnOnce() + Send>>>,
}

// Safety: MmapBuffer manages its own memory and synchronization.
// The raw pointer is only accessed through safe slice methods.
unsafe impl Send for MmapBuffer {}
unsafe impl Sync for MmapBuffer {}

impl std::fmt::Debug for MmapBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapBuffer")
            .field("capacity", &self.capacity)
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
    /// The buffer will be unmapped when the Arc is dropped.
    /// Returns an error if memory allocation fails.
    pub fn new(size: usize) -> Result<Arc<Self>> {
        let aligned_size = sys::page_align(size);
        if aligned_size == 0 {
            return Ok(Arc::new(MmapBuffer {
                raw: std::ptr::null_mut(),
                capacity: 0,
                pool: None,
                on_release: parking_lot::Mutex::new(Vec::new()),
            }));
        }

        let ptr = sys::mmap_anon(aligned_size)?;

        // Pre-warm: force physical RAM commitment
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, aligned_size) };
        for i in (0..aligned_size).step_by(BLOCK_SIZE) {
            slice[i] = 0;
        }

        Ok(Arc::new(MmapBuffer {
            raw: ptr,
            capacity: aligned_size,
            pool: None,
            on_release: parking_lot::Mutex::new(Vec::new()),
        }))
    }

    /// Creates a buffer from pooled memory.
    fn from_pool(raw: RawBuffer, pool: Sender<RawBuffer>) -> Arc<Self> {
        Arc::new(MmapBuffer {
            raw: raw.ptr,
            capacity: raw.capacity,
            pool: Some(pool),
            on_release: parking_lot::Mutex::new(Vec::new()),
        })
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

    /// Writes data at the specified offset using raw pointers.
    ///
    /// # Safety
    ///
    /// This method is safe to call from multiple threads writing to
    /// NON-OVERLAPPING regions. It uses raw pointer operations to avoid
    /// creating multiple `&mut` references to the same buffer, which would
    /// be undefined behavior.
    ///
    /// # Panics
    ///
    /// Panics if `offset + data.len() > capacity` or if the buffer is null.
    #[inline]
    pub fn write_at(&self, offset: usize, data: &[u8]) {
        assert!(!self.raw.is_null(), "cannot write to null buffer");
        assert!(
            offset + data.len() <= self.capacity,
            "write_at: offset {} + len {} exceeds capacity {}",
            offset, data.len(), self.capacity
        );

        // SAFETY: We've verified bounds above. Using raw pointer copy avoids
        // creating overlapping &mut references when multiple threads write
        // to different offsets in the same buffer.
        unsafe {
            let dst = self.raw.add(offset);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
    }

    /// Returns a raw pointer to the buffer for low-level operations.
    ///
    /// # Safety
    ///
    /// The caller must ensure that any writes through this pointer do not
    /// overlap with concurrent writes, and that all accesses are within bounds.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.raw
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

impl Drop for MmapBuffer {
    fn drop(&mut self) {
        // Execute release callbacks
        let callbacks = std::mem::take(&mut *self.on_release.lock());
        for callback in callbacks {
            callback();
        }

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
                // Pool is full or closed, unmap directly
                sys::munmap(self.raw, self.capacity);
            }
        } else {
            // Standalone buffer, unmap directly
            sys::munmap(self.raw, self.capacity);
        }
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
    ///
    /// Returns an error if memory allocation fails during pool creation.
    pub fn new(name: impl Into<String>, buffer_size: usize, capacity: usize) -> Result<Self> {
        let aligned_size = sys::page_align(buffer_size);
        let (sender, receiver) = channel::bounded(capacity);

        // Pre-fill with aligned, pre-warmed buffers
        for _ in 0..capacity {
            let ptr = sys::mmap_anon(aligned_size)?;

            // Pre-warm
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, aligned_size) };
            for i in (0..aligned_size).step_by(BLOCK_SIZE) {
                slice[i] = 0;
            }

            let raw = RawBuffer {
                ptr,
                capacity: aligned_size,
            };
            sender.send(raw).expect("channel should not be full");
        }

        Ok(MmapPool {
            buffers: receiver,
            sender,
            buffer_size: aligned_size,
            name: name.into(),
        })
    }

    /// Acquires a buffer from the pool, blocking if none are available.
    ///
    /// Returns `Error::Backpressure` if the pool is exhausted after a 10s timeout.
    pub fn acquire(&self) -> Result<Arc<MmapBuffer>> {
        match self.buffers.recv_timeout(Duration::from_secs(10)) {
            Ok(raw) => Ok(MmapBuffer::from_pool(raw, self.sender.clone())),
            Err(_) => Err(Error::Backpressure {
                message: format!(
                    "timeout acquiring buffer from pool '{}' after 10s - pool exhausted",
                    self.name
                ),
            }),
        }
    }

    /// Acquires a buffer of at least the specified size.
    ///
    /// If size fits in the pool's buffer size, returns a pooled buffer.
    /// Otherwise, allocates a standalone buffer.
    pub fn acquire_aligned(&self, size: usize) -> Result<Arc<MmapBuffer>> {
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
            sys::munmap(raw.ptr, raw.capacity);
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_mmap_buffer_new() {
        let buf = MmapBuffer::new(4096).unwrap();
        assert_eq!(buf.capacity(), 4096);
        assert!(buf.is_aligned());

        // Write some data
        buf.write_at(0, b"hello");
        assert_eq!(&buf.as_slice()[0..5], b"hello");
    }

    #[test]
    fn test_mmap_buffer_write() {
        let buf = MmapBuffer::new(4096).unwrap();
        buf.write_at(0, b"hello");
        buf.write_at(100, b"world");

        assert_eq!(&buf.as_slice()[0..5], b"hello");
        assert_eq!(&buf.as_slice()[100..105], b"world");
    }

    #[test]
    fn test_mmap_pool() {
        let pool = MmapPool::new("test", 4096, 2).unwrap();
        assert_eq!(pool.available(), 2);

        let buf1 = pool.acquire().unwrap();
        assert_eq!(pool.available(), 1);
        assert!(buf1.is_aligned());

        let buf2 = pool.acquire().unwrap();
        assert_eq!(pool.available(), 0);

        // Drop buf1 to return to pool
        drop(buf1);
        assert_eq!(pool.available(), 1);

        drop(buf2);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_mmap_pool_concurrent() {
        let pool = Arc::new(MmapPool::new("concurrent", 4096, 4).unwrap());
        let mut handles = vec![];

        for i in 0..8 {
            let pool = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                let buf = pool.acquire().unwrap();
                buf.write_at(0, format!("thread-{}", i).as_bytes());
                thread::sleep(Duration::from_millis(10));
                // buf is dropped here, returning to pool
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_acquire_aligned_oversized() {
        let pool = MmapPool::new("oversized", 4096, 2).unwrap();

        // Request larger than pool buffer size
        let large_buf = pool.acquire_aligned(1024 * 1024).unwrap();
        assert!(large_buf.capacity() >= 1024 * 1024);

        // Pool should still have all buffers (large one is standalone)
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_on_release_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        {
            let buf = MmapBuffer::new(4096).unwrap();
            buf.add_on_release(move || {
                called_clone.store(true, Ordering::Release);
            });
            // buf is dropped here
        }

        assert!(called.load(Ordering::Acquire));
    }
}
