//! Platform-specific I/O operations.
//!
//! Provides Direct I/O (O_DIRECT on Linux, F_NOCACHE on Darwin), aligned memory
//! allocation, hole punching, and sync operations.
//!
//! # Design Philosophy
//!
//! - **Write path**: Direct I/O bypasses kernel page cache for predictable memory
//! - **Read path**: Buffered I/O leverages kernel page cache expertise
//! - **Alignment**: All Direct I/O requires 4KB-aligned memory, offset, and size

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

use crate::error::{Error, Result};

// =============================================================================
// Constants
// =============================================================================

/// Block alignment for O_DIRECT I/O (4KB on most systems).
pub const BLOCK_SIZE: usize = 4096;

/// Block alignment mask.
pub const BLOCK_MASK: usize = BLOCK_SIZE - 1;

// =============================================================================
// Platform Detection
// =============================================================================

/// Whether fadvise is effective on this platform.
#[cfg(target_os = "linux")]
pub const USE_FADVISE: bool = true;
#[cfg(not(target_os = "linux"))]
pub const USE_FADVISE: bool = false;

/// Whether O_DIRECT requires 4KB-aligned buffers.
#[cfg(target_os = "linux")]
pub const REQUIRES_ALIGNMENT: bool = true;
#[cfg(not(target_os = "linux"))]
pub const REQUIRES_ALIGNMENT: bool = false;

/// Whether explicit sync calls are needed for durable writes.
#[cfg(target_os = "linux")]
pub const REQUIRES_EXPLICIT_SYNC: bool = false;
#[cfg(not(target_os = "linux"))]
pub const REQUIRES_EXPLICIT_SYNC: bool = true;

// =============================================================================
// Open Flags
// =============================================================================

/// Flags controlling file opening behavior.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFlags {
    /// Bypass page cache (O_DIRECT on Linux, F_NOCACHE on Darwin).
    pub direct_io: bool,
    /// Sync data before write returns (O_DSYNC on Linux).
    pub dsync: bool,
    /// Sync data + metadata before write returns (O_SYNC on Linux).
    pub sync: bool,
}

impl OpenFlags {
    /// Creates flags for Direct I/O with data sync.
    pub fn direct_dsync() -> Self {
        OpenFlags {
            direct_io: true,
            dsync: true,
            sync: false,
        }
    }

    /// Creates flags for buffered I/O (reads).
    pub fn buffered() -> Self {
        OpenFlags::default()
    }

    /// Converts to platform-specific open flags (Linux only).
    #[cfg(target_os = "linux")]
    fn to_os_flags(self) -> i32 {
        let mut flags = 0i32;
        if self.direct_io {
            flags |= libc::O_DIRECT;
        }
        if self.dsync {
            flags |= libc::O_DSYNC;
        }
        if self.sync {
            flags |= libc::O_SYNC;
        }
        flags
    }
}

// =============================================================================
// Alignment Utilities
// =============================================================================

/// Rounds size up to the nearest BLOCK_SIZE (4KB) boundary.
#[inline]
pub fn page_align(size: usize) -> usize {
    (size + BLOCK_MASK) & !BLOCK_MASK
}

/// Checks if a buffer is aligned for Direct I/O.
#[inline]
pub fn is_aligned(buf: &[u8]) -> bool {
    if buf.is_empty() {
        return true;
    }
    (buf.as_ptr() as usize) & BLOCK_MASK == 0
}

/// Checks if a buffer address, length, and offset are all aligned.
#[inline]
pub fn is_fully_aligned(buf: &[u8], offset: u64) -> bool {
    is_aligned(buf) && (buf.len() & BLOCK_MASK == 0) && (offset as usize & BLOCK_MASK == 0)
}

/// Aligns offset and length for hole punching.
/// Returns (aligned_offset, aligned_length, can_punch).
/// can_punch is false if there are no complete blocks to punch.
pub fn align_for_hole_punch(offset: i64, length: i64) -> (i64, i64, bool) {
    // Round offset UP to next block boundary (don't punch into previous blob)
    let aligned_offset = (offset + BLOCK_MASK as i64) & !(BLOCK_MASK as i64);
    let mut adjusted_length = length - (aligned_offset - offset);

    // Skip if blob smaller than one block after adjustment
    if adjusted_length < BLOCK_SIZE as i64 {
        return (0, 0, false);
    }

    // Round length DOWN to block multiple (don't punch into next blob)
    adjusted_length &= !(BLOCK_MASK as i64);

    (aligned_offset, adjusted_length, true)
}

// =============================================================================
// Raw Memory Mapping
// =============================================================================

/// Allocates anonymous memory-mapped region with 4KB alignment.
///
/// The returned pointer is page-aligned and suitable for Direct I/O.
/// Memory is pre-warmed to force physical RAM commitment.
///
/// Returns an error if mmap fails (out of memory).
pub fn mmap_anon(size: usize) -> Result<*mut u8> {
    if size == 0 {
        return Ok(std::ptr::null_mut());
    }

    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANON | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        return Err(Error::io("mmap anonymous memory", io::Error::last_os_error()));
    }

    Ok(ptr as *mut u8)
}

/// Unmaps a memory region previously allocated with `mmap_anon`.
///
/// # Safety
///
/// The caller must ensure:
/// - `ptr` was returned by `mmap_anon`
/// - `size` matches the original allocation size
/// - The memory is not accessed after this call
pub fn munmap(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }

    let result = unsafe { libc::munmap(ptr as *mut libc::c_void, size) };
    if result == -1 {
        // Log error but don't panic - we're likely in a Drop impl
        eprintln!(
            "warning: munmap failed: {}",
            io::Error::last_os_error()
        );
    }
}

// =============================================================================
// Aligned Memory Allocation
// =============================================================================

/// Allocates a byte buffer with 4KB-aligned memory address using mmap.
///
/// The returned buffer size is rounded UP to the nearest page boundary.
/// This is optimal for O_DIRECT I/O which requires page-aligned memory.
///
/// Returns an error if mmap fails (out of memory).
pub fn alloc_aligned(size: usize) -> Result<AlignedBuffer> {
    let aligned_size = page_align(size);
    if aligned_size == 0 {
        return Ok(AlignedBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
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
        return Err(Error::io("mmap aligned buffer", io::Error::last_os_error()));
    }

    // Pre-warm: force physical RAM commitment
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, aligned_size) };
    for i in (0..aligned_size).step_by(BLOCK_SIZE) {
        slice[i] = 0;
    }

    Ok(AlignedBuffer {
        ptr: ptr as *mut u8,
        len: 0,
        capacity: aligned_size,
    })
}

/// A buffer backed by mmap'd memory with 4KB alignment.
///
/// Provides `&[u8]` and `&mut [u8]` access, automatically unmapped on drop.
pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

// Safety: The buffer is a contiguous block of memory that can be sent between threads.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Creates an empty aligned buffer.
    pub fn empty() -> Self {
        AlignedBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }

    /// Returns the current length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Sets the length of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `new_len > capacity`.
    pub fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity, "length exceeds capacity");
        self.len = new_len;
    }

    /// Returns the buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        if self.ptr.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }

    /// Returns the buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        if self.ptr.is_null() {
            &mut []
        } else {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }

    /// Returns the full capacity as a mutable slice.
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        if self.ptr.is_null() {
            &mut []
        } else {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
        }
    }

    /// Extends the buffer by copying from a slice.
    ///
    /// # Panics
    ///
    /// Panics if there isn't enough capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        assert!(new_len <= self.capacity, "extend would exceed capacity");
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len = new_len;
    }

    /// Clears the buffer, setting length to 0.
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.capacity > 0 {
            unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, self.capacity);
            }
        }
    }
}

impl AsRef<[u8]> for AlignedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for AlignedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

// =============================================================================
// File Operations
// =============================================================================

/// Creates a file for writing with the specified flags.
///
/// On Linux, uses O_DIRECT/O_DSYNC/O_SYNC directly.
/// On Darwin, uses fcntl(F_NOCACHE) after opening.
pub fn create_file(path: &Path, flags: OpenFlags) -> Result<File> {
    #[cfg(target_os = "linux")]
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .custom_flags(flags.to_os_flags())
            .open(path)
            .map_err(|e| Error::io("create file", e))?;
        Ok(file)
    }

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::io("create file", e))?;

        if flags.direct_io {
            // F_NOCACHE = 48 on Darwin
            let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
            if result == -1 {
                return Err(Error::io("set F_NOCACHE", io::Error::last_os_error()));
            }
        }
        Ok(file)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = flags;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::io("create file", e))
    }
}

/// Opens an existing file for reading.
pub fn open_file(path: &Path) -> Result<File> {
    File::open(path).map_err(|e| Error::io("open file", e))
}

/// Pre-allocates disk space for a file.
#[cfg(target_os = "linux")]
pub fn fallocate(file: &File, size: i64) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size) };
    if result == -1 {
        return Err(Error::io("fallocate", io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn fallocate(file: &File, size: i64) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    // F_PREALLOCATE on Darwin
    #[repr(C)]
    struct FStore {
        fst_flags: u32,
        fst_posmode: i32,
        fst_offset: i64,
        fst_length: i64,
        fst_bytesalloc: i64,
    }

    let mut fstore = FStore {
        fst_flags: 0x02, // F_ALLOCATECONTIG
        fst_posmode: 3,  // F_PEOFPOSMODE
        fst_offset: 0,
        fst_length: size,
        fst_bytesalloc: 0,
    };

    // Try contiguous first
    let result = unsafe {
        libc::fcntl(
            file.as_raw_fd(),
            libc::F_PREALLOCATE,
            &mut fstore as *mut FStore,
        )
    };

    if result == -1 {
        // Fall back to non-contiguous
        fstore.fst_flags = 0x04; // F_ALLOCATEALL
        let result = unsafe {
            libc::fcntl(
                file.as_raw_fd(),
                libc::F_PREALLOCATE,
                &mut fstore as *mut FStore,
            )
        };
        if result == -1 {
            return Err(Error::io("fallocate", io::Error::last_os_error()));
        }
    }

    // Set logical file size
    file.set_len(size as u64)
        .map_err(|e| Error::io("set file length", e))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn fallocate(file: &File, size: i64) -> Result<()> {
    // Fallback: just set the file length
    file.set_len(size as u64)
        .map_err(|e| Error::io("set file length", e))
}

/// Punches a hole in a file (creates sparse region).
/// Returns the number of bytes actually reclaimed after alignment.
#[cfg(target_os = "linux")]
pub fn punch_hole(file: &File, offset: i64, length: i64) -> Result<i64> {
    use std::os::unix::io::AsRawFd;

    let (aligned_offset, aligned_length, can_punch) = align_for_hole_punch(offset, length);
    if !can_punch {
        return Ok(0);
    }

    // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
    let mode = 0x01 | 0x02;
    let result = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            mode,
            aligned_offset,
            aligned_length,
        )
    };

    if result == -1 {
        return Err(Error::io("punch hole", io::Error::last_os_error()));
    }
    Ok(aligned_length)
}

#[cfg(target_os = "macos")]
pub fn punch_hole(file: &File, offset: i64, length: i64) -> Result<i64> {
    use std::os::unix::io::AsRawFd;

    let (aligned_offset, aligned_length, can_punch) = align_for_hole_punch(offset, length);
    if !can_punch {
        return Ok(0);
    }

    #[repr(C)]
    struct FPunchHole {
        fp_flags: u32,
        fp_reserved: u32,
        fp_offset: i64,
        fp_length: i64,
    }

    let ph = FPunchHole {
        fp_flags: 0,
        fp_reserved: 0,
        fp_offset: aligned_offset,
        fp_length: aligned_length,
    };

    // F_PUNCHHOLE = 99 on Darwin
    let result = unsafe { libc::fcntl(file.as_raw_fd(), 99, &ph as *const FPunchHole) };

    if result == -1 {
        return Err(Error::io("punch hole", io::Error::last_os_error()));
    }
    Ok(aligned_length)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn punch_hole(_file: &File, _offset: i64, _length: i64) -> Result<i64> {
    // Hole punching not supported
    Ok(0)
}

/// Syncs file data to disk (fdatasync on Linux, F_FULLFSYNC on Darwin).
pub fn fdatasync(file: &File) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    #[cfg(target_os = "linux")]
    {
        let result = unsafe { libc::fdatasync(file.as_raw_fd()) };
        if result == -1 {
            return Err(Error::io("fdatasync", io::Error::last_os_error()));
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    {
        // F_FULLFSYNC ensures data reaches physical disk
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if result == -1 {
            return Err(Error::io("F_FULLFSYNC", io::Error::last_os_error()));
        }
        Ok(())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        file.sync_data().map_err(|e| Error::io("sync_data", e))
    }
}

/// Hints to the kernel about expected access pattern.
///
/// This is advisory - errors are logged but not fatal.
#[cfg(target_os = "linux")]
pub fn fadvise(file: &File, offset: i64, length: i64) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    // POSIX_FADV_SEQUENTIAL = 2
    let result = unsafe {
        libc::posix_fadvise(file.as_raw_fd(), offset, length, libc::POSIX_FADV_SEQUENTIAL)
    };

    if result != 0 {
        return Err(Error::io("fadvise", io::Error::from_raw_os_error(result)));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn fadvise(_file: &File, _offset: i64, _length: i64) -> Result<()> {
    // fadvise not available on this platform
    Ok(())
}

/// Syncs file based on flags.
pub fn sync_file(file: &File, flags: OpenFlags) -> Result<()> {
    if !REQUIRES_EXPLICIT_SYNC {
        return Ok(());
    }
    if flags.sync {
        file.sync_all().map_err(|e| Error::io("sync_all", e))
    } else if flags.dsync {
        fdatasync(file)
    } else {
        Ok(())
    }
}

/// Writes aligned data to a file with optional sync.
pub fn write_aligned(file: &mut File, buf: &[u8], flags: OpenFlags) -> Result<usize> {
    if flags.direct_io && REQUIRES_ALIGNMENT && !is_aligned(buf) {
        return Err(Error::Alignment {
            address: buf.as_ptr() as usize,
            alignment: BLOCK_SIZE,
        });
    }
    let written = file.write(buf).map_err(|e| Error::io("write", e))?;
    sync_file(file, flags)?;
    Ok(written)
}

/// Reads from a file at the given offset.
pub fn read_at(file: &mut File, buf: &mut [u8], offset: u64) -> Result<usize> {
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| Error::io("seek", e))?;
    file.read(buf).map_err(|e| Error::io("read", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_page_align() {
        assert_eq!(page_align(0), 0);
        assert_eq!(page_align(1), BLOCK_SIZE);
        assert_eq!(page_align(BLOCK_SIZE), BLOCK_SIZE);
        assert_eq!(page_align(BLOCK_SIZE + 1), BLOCK_SIZE * 2);
    }

    #[test]
    fn test_aligned_buffer() {
        let mut buf = alloc_aligned(1000).unwrap();
        assert!(is_aligned(buf.spare_capacity_mut()));
        assert_eq!(buf.capacity(), BLOCK_SIZE); // Rounded up

        buf.extend_from_slice(b"hello");
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf[..5], b"hello");
    }

    #[test]
    fn test_align_for_hole_punch() {
        // Too small
        let (_, _, can) = align_for_hole_punch(0, 100);
        assert!(!can);

        // Exactly one block
        let (off, len, can) = align_for_hole_punch(0, BLOCK_SIZE as i64);
        assert!(can);
        assert_eq!(off, 0);
        assert_eq!(len, BLOCK_SIZE as i64);

        // Partial offset
        let (off, len, can) = align_for_hole_punch(100, BLOCK_SIZE as i64 * 2);
        assert!(can);
        assert_eq!(off, BLOCK_SIZE as i64);
        assert_eq!(len, BLOCK_SIZE as i64);
    }

    #[test]
    fn test_create_and_write_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let mut file = create_file(&path, OpenFlags::buffered()).unwrap();
        let data = b"hello world";
        file.write_all(data).unwrap();
        drop(file);

        let mut file = open_file(&path).unwrap();
        let mut buf = vec![0u8; data.len()];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, data);
    }

}
