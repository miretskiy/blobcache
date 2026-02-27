//! I/O Scheduler abstraction for segment reads.
//!
//! Provides a `IOScheduler` trait with two implementations:
//! - `PreadScheduler`: Synchronous pread(2) — portable, works everywhere
//! - `URingScheduler`: io_uring — Linux only, lower latency at high concurrency
//!
//! The default scheduler is `PreadScheduler`. io_uring is opt-in via config.

use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};


// =============================================================================
// IOStats
// =============================================================================

/// Statistics for the I/O scheduler.
#[derive(Debug, Default, Clone, Copy)]
pub struct IOStats {
    /// Total number of read requests issued.
    pub requests: u64,
    /// Total number of submission batches (1 for PreadScheduler).
    pub batches: u64,
}

// =============================================================================
// IOScheduler trait
// =============================================================================

/// Abstraction over the read I/O path.
///
/// Implementations may use blocking pread(2) or async io_uring.
pub trait IOScheduler: Send + Sync {
    /// Reads `buf.len()` bytes from `fd` at `offset` into `buf`.
    ///
    /// Returns the number of bytes read on success. Implementations should
    /// retry on `EINTR` automatically.
    fn read_at(&self, fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Returns accumulated I/O statistics.
    fn stats(&self) -> IOStats;

    /// Called when the scheduler is no longer needed (graceful shutdown).
    fn close(&self) {}
}

// =============================================================================
// PreadScheduler
// =============================================================================

/// Synchronous `pread(2)`-based scheduler.
///
/// Simple, portable, and sufficient for most workloads. The kernel issues
/// read-ahead automatically when sequential access is detected.
pub struct PreadScheduler {
    requests: AtomicU64,
}

impl PreadScheduler {
    /// Creates a new PreadScheduler.
    pub fn new() -> Self {
        PreadScheduler {
            requests: AtomicU64::new(0),
        }
    }
}

impl Default for PreadScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl IOScheduler for PreadScheduler {
    fn read_at(&self, fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.requests.fetch_add(1, Ordering::Relaxed);

        loop {
            let ret = unsafe {
                libc::pread(
                    fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };

            if ret >= 0 {
                return Ok(ret as usize);
            }

            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
            // EINTR: retry
        }
    }

    fn stats(&self) -> IOStats {
        let reqs = self.requests.load(Ordering::Relaxed);
        IOStats {
            requests: reqs,
            batches: reqs, // Each pread is its own "batch"
        }
    }
}

// =============================================================================
// IOSchedulerKind
// =============================================================================

/// Selects which I/O scheduler to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IOSchedulerKind {
    /// Synchronous pread(2) (default, portable).
    #[default]
    Pread,
    /// io_uring (Linux only, lower latency at high queue depth).
    #[cfg(target_os = "linux")]
    URing { ring_depth: u32 },
}

impl IOSchedulerKind {
    /// Creates the scheduler specified by this kind.
    pub fn build(self) -> io::Result<Box<dyn IOScheduler>> {
        match self {
            IOSchedulerKind::Pread => Ok(Box::new(PreadScheduler::new())),
            #[cfg(target_os = "linux")]
            IOSchedulerKind::URing { ring_depth } => {
                Ok(Box::new(URingScheduler::new(ring_depth)?))
            }
        }
    }
}

// =============================================================================
// URingScheduler (Linux only)
// =============================================================================

#[cfg(target_os = "linux")]
pub use uring::URingScheduler;

#[cfg(target_os = "linux")]
mod uring {
    use super::*;
    use std::sync::Mutex;

    /// io_uring-based I/O scheduler.
    ///
    /// Maintains a fixed-size submission queue. Each `read_at` call submits a
    /// sqe and synchronously waits for the matching cqe. This adds ~1µs of
    /// overhead vs pread but enables true async batching when multiple threads
    /// submit simultaneously.
    pub struct URingScheduler {
        // Wrap in Mutex for now — true async submission needs a coordinator
        // goroutine pattern which is a follow-up optimization.
        inner: Mutex<io_uring::IoUring>,
        requests: AtomicU64,
        batches: AtomicU64,
    }

    impl URingScheduler {
        /// Creates a new URingScheduler with `ring_depth` entries.
        pub fn new(ring_depth: u32) -> io::Result<Self> {
            let ring = io_uring::IoUring::new(ring_depth)?;
            Ok(URingScheduler {
                inner: Mutex::new(ring),
                requests: AtomicU64::new(0),
                batches: AtomicU64::new(0),
            })
        }
    }

    impl IOScheduler for URingScheduler {
        fn read_at(&self, fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.batches.fetch_add(1, Ordering::Relaxed);

            let mut ring = self.inner.lock().unwrap();

            // Build a read sqe
            let read_e = io_uring::opcode::Read::new(
                io_uring::types::Fd(fd),
                buf.as_mut_ptr(),
                buf.len() as u32,
            )
            .offset(offset)
            .build()
            .user_data(0x42);

            // Safety: The buffer is valid for the duration of this synchronous call.
            unsafe {
                ring.submission().push(&read_e).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "io_uring submission queue full")
                })?;
            }

            ring.submit_and_wait(1)?;

            // Drain completion queue
            let cqe = ring.completion().next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "io_uring: no completion entry")
            })?;

            let result = cqe.result();
            if result < 0 {
                Err(io::Error::from_raw_os_error(-result))
            } else {
                Ok(result as usize)
            }
        }

        fn stats(&self) -> IOStats {
            IOStats {
                requests: self.requests.load(Ordering::Relaxed),
                batches: self.batches.load(Ordering::Relaxed),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempfile;

    #[test]
    fn test_pread_scheduler_basic() {
        let mut f = tempfile().unwrap();
        f.write_all(b"hello world").unwrap();

        use std::os::unix::io::IntoRawFd;
        let fd = f.into_raw_fd();

        let sched = PreadScheduler::new();
        let mut buf = vec![0u8; 5];
        let n = sched.read_at(fd, &mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        let n = sched.read_at(fd, &mut buf, 6).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        unsafe { libc::close(fd) };

        let stats = sched.stats();
        assert_eq!(stats.requests, 2);
    }

    #[test]
    fn test_pread_scheduler_stats() {
        let sched = PreadScheduler::new();
        assert_eq!(sched.stats().requests, 0);
    }

    #[test]
    fn test_iosched_kind_default() {
        assert_eq!(IOSchedulerKind::default(), IOSchedulerKind::Pread);
    }
}
