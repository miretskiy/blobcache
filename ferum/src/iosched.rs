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
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;

    use crossbeam_channel::{Receiver, Sender};

    // -------------------------------------------------------------------------
    // ReadRequest: a single positioned read submitted to the coordinator.
    //
    // Safety invariant: `buf_ptr` is a raw pointer into the caller's buffer.
    // The caller blocks on `done` until the coordinator signals completion,
    // so the buffer is guaranteed valid for the entire duration of the I/O.
    // -------------------------------------------------------------------------
    struct ReadRequest {
        fd:      RawFd,
        buf_ptr: u64,   // *mut u8 as u64 — stable across Send boundary
        len:     u32,
        offset:  u64,
        done:    Arc<(Mutex<Option<io::Result<usize>>>, Condvar)>,
    }

    // Safety: buf_ptr is valid because the submitting thread blocks until done.
    unsafe impl Send for ReadRequest {}

    enum CoordMsg {
        Read(ReadRequest),
        Shutdown,
    }

    // -------------------------------------------------------------------------
    // URingScheduler
    //
    // A single coordinator thread exclusively owns the io_uring ring.
    // Callers submit ReadRequests via an unbounded channel, then block on a
    // Condvar.  The coordinator uses a sliding-window loop:
    //
    //   1. Collect pending requests from the channel (block if idle).
    //   2. Fill as many SQEs as the ring has free slots.
    //   3. Call submit_and_wait(1) — submit all SQEs, wait for ≥1 CQE.
    //   4. Reap all ready CQEs and wake the corresponding callers.
    //   5. Repeat.
    //
    // Result: N concurrent callers → N Condvar waits (no OS thread held per
    // outstanding read) + 1 coordinator OS thread.  Thread count is O(1)
    // regardless of read concurrency.
    // -------------------------------------------------------------------------
    pub struct URingScheduler {
        tx:       Sender<CoordMsg>,
        coord:    Mutex<Option<thread::JoinHandle<()>>>,
        requests: Arc<AtomicU64>,
        batches:  Arc<AtomicU64>,
    }

    impl URingScheduler {
        pub fn new(ring_depth: u32) -> io::Result<Self> {
            let ring = io_uring::IoUring::new(ring_depth)?;
            let (tx, rx) = crossbeam_channel::unbounded::<CoordMsg>();

            let requests = Arc::new(AtomicU64::new(0));
            let batches  = Arc::new(AtomicU64::new(0));

            let handle = {
                let req = Arc::clone(&requests);
                let bat = Arc::clone(&batches);
                thread::Builder::new()
                    .name("ferum-uring-coord".into())
                    .spawn(move || coordinator(ring, rx, req, bat))?
            };

            Ok(URingScheduler {
                tx,
                coord: Mutex::new(Some(handle)),
                requests,
                batches,
            })
        }
    }

    impl IOScheduler for URingScheduler {
        fn read_at(&self, fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            let done = Arc::new((Mutex::new(None::<io::Result<usize>>), Condvar::new()));

            let req = ReadRequest {
                fd,
                buf_ptr: buf.as_mut_ptr() as u64,
                len:     buf.len() as u32,
                offset,
                done:    Arc::clone(&done),
            };

            self.tx.send(CoordMsg::Read(req)).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "uring coordinator shut down")
            })?;

            // Block until the coordinator completes the I/O and signals us.
            // Safety: `buf` is valid here and stays valid because we do not
            // return until `done` is signalled.
            let (lock, cvar) = &*done;
            let mut guard = lock.lock().unwrap();
            while guard.is_none() {
                guard = cvar.wait(guard).unwrap();
            }
            guard.take().unwrap()
        }

        fn stats(&self) -> IOStats {
            IOStats {
                requests: self.requests.load(Ordering::Relaxed),
                batches:  self.batches.load(Ordering::Relaxed),
            }
        }

        fn close(&self) {
            // Signal coordinator to stop and wait for it to exit.
            let _ = self.tx.send(CoordMsg::Shutdown);
            if let Some(handle) = self.coord.lock().unwrap().take() {
                let _ = handle.join();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Coordinator loop
    // -------------------------------------------------------------------------
    fn coordinator(
        mut ring:  io_uring::IoUring,
        rx:        Receiver<CoordMsg>,
        requests:  Arc<AtomicU64>,
        batches:   Arc<AtomicU64>,
    ) {
        let ring_depth = ring.params().sq_entries() as usize;

        // pending: received but not yet submitted to the kernel.
        // in_flight: submitted to kernel, waiting for CQE; keyed by user_data.
        let mut pending:   VecDeque<ReadRequest>     = VecDeque::new();
        let mut in_flight: HashMap<u64, ReadRequest> = HashMap::with_capacity(ring_depth);
        let mut next_id: u64 = 0;

        'outer: loop {
            // ── 1. Collect requests ─────────────────────────────────────────
            // Block only when there is genuinely nothing to do.
            if in_flight.is_empty() && pending.is_empty() {
                match rx.recv() {
                    Ok(CoordMsg::Read(r))  => pending.push_back(r),
                    Ok(CoordMsg::Shutdown) | Err(_) => break 'outer,
                }
            }
            // Drain any additionally buffered messages without blocking.
            loop {
                match rx.try_recv() {
                    Ok(CoordMsg::Read(r))  => pending.push_back(r),
                    Ok(CoordMsg::Shutdown) => break 'outer,
                    Err(_)                 => break,
                }
            }

            // ── 2. Fill submission queue ────────────────────────────────────
            // available = ring capacity minus already-in-flight requests.
            // After submit_and_wait the SQ is empty (all SQEs consumed by
            // kernel), so sq.len() == 0 and we can push up to
            // (ring_depth - in_flight.len()) new SQEs.
            {
                let mut sq = ring.submission();
                let available = ring_depth.saturating_sub(in_flight.len());
                let to_fill   = pending.len().min(available);

                for _ in 0..to_fill {
                    let req = pending.pop_front().unwrap();
                    let id  = next_id;
                    next_id += 1;

                    let sqe = io_uring::opcode::Read::new(
                        io_uring::types::Fd(req.fd),
                        req.buf_ptr as *mut u8,
                        req.len,
                    )
                    .offset(req.offset)
                    .build()
                    .user_data(id);

                    // Safety: buf_ptr is valid because the submitting caller
                    // is blocked on req.done and cannot free the buffer.
                    if unsafe { sq.push(&sqe) }.is_err() {
                        // Shouldn't happen given our available check, but
                        // be defensive: put the request back and stop.
                        pending.push_front(req);
                        break;
                    }
                    in_flight.insert(id, req);
                }
                // sq drops here → sync() called, kernel sees updated tail.
            }

            if in_flight.is_empty() {
                continue;
            }

            // ── 3. Submit + wait for ≥1 completion ─────────────────────────
            if let Err(e) = ring.submit_and_wait(1) {
                fail_all(&mut in_flight, &mut pending, &e.to_string());
                break;
            }
            batches.fetch_add(1, Ordering::Relaxed);

            // ── 4. Reap all ready CQEs ──────────────────────────────────────
            // Collect first to avoid holding the CQ iterator across the
            // in_flight HashMap lookup (borrow checker).
            let cqes: Vec<(u64, i32)> = ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result()))
                .collect();

            requests.fetch_add(cqes.len() as u64, Ordering::Relaxed);

            for (id, res) in cqes {
                if let Some(req) = in_flight.remove(&id) {
                    let result = if res >= 0 {
                        Ok(res as usize)
                    } else {
                        Err(io::Error::from_raw_os_error(-res))
                    };
                    let (lock, cvar) = &*req.done;
                    *lock.lock().unwrap() = Some(result);
                    cvar.notify_one();
                }
            }
        }

        // Coordinator shutting down: wake all blocked callers with an error.
        fail_all(&mut in_flight, &mut pending, "uring coordinator shut down");
    }

    fn fail_all(
        in_flight: &mut HashMap<u64, ReadRequest>,
        pending:   &mut VecDeque<ReadRequest>,
        msg:       &str,
    ) {
        let make_err = || Err(io::Error::new(io::ErrorKind::BrokenPipe, msg));
        for (_, req) in in_flight.drain() {
            let (lock, cvar) = &*req.done;
            *lock.lock().unwrap() = Some(make_err());
            cvar.notify_one();
        }
        for req in pending.drain(..) {
            let (lock, cvar) = &*req.done;
            *lock.lock().unwrap() = Some(make_err());
            cvar.notify_one();
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

    // URingScheduler tests — Linux only
    #[cfg(target_os = "linux")]
    mod uring_tests {
        use super::*;
        use std::io::Write;
        use std::sync::{Arc, Barrier};
        use tempfile::tempfile;

        fn make_test_file(data: &[u8]) -> std::fs::File {
            let mut f = tempfile().unwrap();
            f.write_all(data).unwrap();
            f
        }

        #[test]
        fn test_uring_basic_read() {
            let f = make_test_file(b"hello world");
            use std::os::unix::io::IntoRawFd;
            let fd = f.into_raw_fd();

            let sched = URingScheduler::new(16).unwrap();
            // Buffer must be 4KB-aligned for O_DIRECT; plain Vec is fine for
            // buffered files in tests.
            let mut buf = vec![0u8; 5];
            let n = sched.read_at(fd, &mut buf, 0).unwrap();
            assert_eq!(n, 5);
            assert_eq!(&buf, b"hello");

            let mut buf2 = vec![0u8; 5];
            let n = sched.read_at(fd, &mut buf2, 6).unwrap();
            assert_eq!(n, 5);
            assert_eq!(&buf2, b"world");

            unsafe { libc::close(fd) };
            sched.close();

            let stats = sched.stats();
            assert_eq!(stats.requests, 2);
            assert!(stats.batches >= 1);
        }

        #[test]
        fn test_uring_concurrent_reads() {
            // Write a 1MB file and have 16 threads read different regions
            // concurrently.  Validates that the coordinator correctly routes
            // completions back to the right callers.
            const CHUNK: usize = 4096;
            const N: usize = 16;
            let data: Vec<u8> = (0..N * CHUNK).map(|i| (i % 251) as u8).collect();

            let f = make_test_file(&data);
            use std::os::unix::io::{AsRawFd, IntoRawFd};
            let fd = f.into_raw_fd();

            let sched = Arc::new(URingScheduler::new(32).unwrap());
            let barrier = Arc::new(Barrier::new(N));

            let handles: Vec<_> = (0..N).map(|i| {
                let sched = Arc::clone(&sched);
                let barrier = Arc::clone(&barrier);
                let expected: Vec<u8> = (i * CHUNK..(i + 1) * CHUNK)
                    .map(|j| (j % 251) as u8)
                    .collect();
                std::thread::spawn(move || {
                    barrier.wait(); // all threads start simultaneously
                    let mut buf = vec![0u8; CHUNK];
                    let n = sched.read_at(fd, &mut buf, (i * CHUNK) as u64).unwrap();
                    assert_eq!(n, CHUNK);
                    assert_eq!(buf, expected, "chunk {} mismatch", i);
                })
            }).collect();

            for h in handles {
                h.join().unwrap();
            }

            unsafe { libc::close(fd) };
            sched.close();

            // All 16 reads should be recorded
            assert_eq!(sched.stats().requests, N as u64);
            // Should have batched into fewer than N submit_and_wait calls
            println!("uring batches for {} concurrent reads: {}", N, sched.stats().batches);
        }

        #[test]
        fn test_uring_shutdown_wakes_blocked_callers() {
            // Verify that close() wakes any callers blocked waiting for I/O
            // (simulates coordinator crash / ring error).
            let sched = Arc::new(URingScheduler::new(4).unwrap());
            sched.close(); // shut down immediately

            // A read after close should return an error, not deadlock.
            use std::os::unix::io::IntoRawFd;
            let f = make_test_file(b"data");
            let fd = f.into_raw_fd();
            let mut buf = vec![0u8; 4];
            let result = sched.read_at(fd, &mut buf, 0);
            assert!(result.is_err());
            unsafe { libc::close(fd) };
        }
    }
}
