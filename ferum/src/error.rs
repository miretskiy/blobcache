//! Error types for Ferum.
//!
//! Follows the Go codebase's error handling philosophy:
//! - NEVER ignore I/O errors
//! - Preserve error context with wrapping
//! - Distinguish between benign and critical errors

use thiserror::Error;

/// Result type alias for Ferum operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Ferum error types.
#[derive(Error, Debug)]
pub enum Error {
    // =========================================================================
    // Record Format Errors
    // =========================================================================
    /// Buffer provided is too small for the operation.
    #[error("buffer too small: need {needed} bytes, have {have}")]
    BufferTooSmall { needed: usize, have: usize },

    /// Invalid magic number in record header.
    #[error("invalid magic number: expected 0x{expected:08X}, got 0x{got:08X}")]
    InvalidMagic { expected: u32, got: u32 },

    /// Detected a hole (punched or padding) in the record stream.
    #[error("hole detected at offset {offset}")]
    Hole { offset: u64 },

    /// Header CRC mismatch - corrupt header, MUST NOT trust PhysicalSize.
    #[error("header CRC mismatch: expected 0x{expected:08X}, computed 0x{computed:08X}")]
    HeaderCrcMismatch { expected: u32, computed: u32 },

    /// Payload CRC mismatch - data corruption detected.
    #[error("payload CRC mismatch: expected 0x{expected:08X}, computed 0x{computed:08X}")]
    PayloadCrcMismatch { expected: u32, computed: u32 },

    /// Key mismatch (possible hash collision).
    #[error("key mismatch: possible hash collision")]
    KeyMismatch,

    /// Record length exceeds bounds.
    #[error("record length {length} exceeds maximum {max}")]
    BoundsExceeded { length: usize, max: usize },

    // =========================================================================
    // I/O Errors
    // =========================================================================
    /// Generic I/O error with context.
    #[error("I/O error during {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },

    /// Alignment error for O_DIRECT operations.
    #[error("data buffer not aligned for O_DIRECT: address 0x{address:X} not {alignment}-byte aligned")]
    Alignment { address: usize, alignment: usize },

    // =========================================================================
    // Compression Errors
    // =========================================================================
    /// Compression operation failed.
    #[error("compression failed ({codec}): {message}")]
    Compression { codec: &'static str, message: String },

    /// Decompression operation failed.
    #[error("decompression failed ({codec}): {message}")]
    Decompression { codec: &'static str, message: String },

    // =========================================================================
    // Configuration Errors
    // =========================================================================
    /// Invalid configuration parameter.
    #[error("invalid configuration: {message}")]
    InvalidConfig { message: String },

    // =========================================================================
    // Cache Errors
    // =========================================================================
    /// Key not found in cache.
    #[error("key not found")]
    NotFound,

    /// Cache is closed.
    #[error("cache is closed")]
    Closed,

    /// Cache is in degraded mode due to previous I/O errors.
    #[error("cache in degraded mode: {reason}")]
    Degraded { reason: String },

    /// Data corruption detected.
    #[error("data corruption: {message}")]
    Corruption { message: String },

    /// Resource exhaustion / backpressure (e.g., pool depleted, queue full).
    #[error("backpressure: {message}")]
    Backpressure { message: String },
}

impl Error {
    /// Creates an I/O error with operation context.
    pub fn io(operation: &'static str, source: std::io::Error) -> Self {
        Error::Io { operation, source }
    }

    /// Returns true if this error is transient and the operation might succeed on retry.
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Io { source, .. } => {
                use std::io::ErrorKind;
                matches!(
                    source.kind(),
                    ErrorKind::Interrupted
                        | ErrorKind::WouldBlock
                        | ErrorKind::TimedOut
                        | ErrorKind::OutOfMemory
                )
            }
            _ => false,
        }
    }

    /// Returns true if this error indicates data corruption.
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Error::HeaderCrcMismatch { .. }
                | Error::PayloadCrcMismatch { .. }
                | Error::InvalidMagic { .. }
                | Error::Corruption { .. }
        )
    }
}

/// BlobErrno represents error conditions stored in record flags.
/// Stored in bits 34-38 of the Flags field (5-bit, 32 values).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum BlobErrno {
    #[default]
    None = 0,
    Decompression = 1,
    ChecksumMismatch = 2,
    IoRead = 3,
    IoWrite = 4,
    Truncated = 5,
    InvalidFormat = 6,
    Unknown = 7,
}

impl BlobErrno {
    /// Creates a BlobErrno from a raw value.
    pub fn from_raw(value: u8) -> Self {
        match value {
            0 => BlobErrno::None,
            1 => BlobErrno::Decompression,
            2 => BlobErrno::ChecksumMismatch,
            3 => BlobErrno::IoRead,
            4 => BlobErrno::IoWrite,
            5 => BlobErrno::Truncated,
            6 => BlobErrno::InvalidFormat,
            _ => BlobErrno::Unknown,
        }
    }

    /// Returns true if there is no error.
    pub fn is_ok(self) -> bool {
        self == BlobErrno::None
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io { source, .. } => source,
            Error::NotFound => std::io::Error::new(std::io::ErrorKind::NotFound, err),
            Error::Alignment { .. } => {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
            }
            _ => std::io::Error::other(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_errno_roundtrip() {
        for i in 0..8 {
            let errno = BlobErrno::from_raw(i);
            assert_eq!(errno as u8, i);
        }
    }

    #[test]
    fn test_error_is_transient() {
        let transient = Error::Io {
            operation: "test",
            source: std::io::Error::new(std::io::ErrorKind::Interrupted, "interrupted"),
        };
        assert!(transient.is_transient());

        let permanent = Error::NotFound;
        assert!(!permanent.is_transient());
    }

    #[test]
    fn test_error_is_corruption() {
        let corruption = Error::HeaderCrcMismatch {
            expected: 1,
            computed: 2,
        };
        assert!(corruption.is_corruption());

        let not_corruption = Error::NotFound;
        assert!(!not_corruption.is_corruption());
    }
}
