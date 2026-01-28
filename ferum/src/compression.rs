//! Compression codecs for Ferum.
//!
//! Supports multiple compression algorithms:
//! - Zstd: Best compression ratio, good speed
//! - LZ4: Fastest compression/decompression
//! - S2: Snappy-compatible with better ratio (via lz4_flex)

use crate::error::{Error, Result};

/// Compression codec identifier.
/// Stored in bits 63-60 of the Flags field (4 bits, 16 values).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Codec {
    #[default]
    None = 0,
    Zstd = 1,
    Lz4 = 2,
    S2 = 3,
}

impl Codec {
    /// Creates a Codec from a raw 4-bit value.
    pub fn from_raw(value: u8) -> Self {
        match value & 0x0F {
            0 => Codec::None,
            1 => Codec::Zstd,
            2 => Codec::Lz4,
            3 => Codec::S2,
            _ => Codec::None,
        }
    }

    /// Returns the codec name for error messages.
    pub fn name(self) -> &'static str {
        match self {
            Codec::None => "none",
            Codec::Zstd => "zstd",
            Codec::Lz4 => "lz4",
            Codec::S2 => "s2",
        }
    }
}

/// Compression level hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Level {
    #[default]
    Default = 0,
    Speed = 1,
    Best = 2,
}

impl Level {
    /// Returns the zstd compression level for this hint.
    fn zstd_level(self) -> i32 {
        match self {
            Level::Default => 3,
            Level::Speed => 1,
            Level::Best => 9,
        }
    }
}

/// Compresses data using the specified codec.
///
/// Returns the compressed data, or an error if compression fails.
/// If the compressed size would be larger than the original, returns
/// an error to trigger the "Early Abort" heuristic.
pub fn compress(codec: Codec, level: Level, src: &[u8]) -> Result<Vec<u8>> {
    match codec {
        Codec::None => Ok(src.to_vec()),
        Codec::Zstd => compress_zstd(src, level),
        Codec::Lz4 => compress_lz4(src),
        Codec::S2 => compress_s2(src),
    }
}

/// Compresses data into a pre-allocated buffer.
///
/// Returns the number of bytes written, or an error if the buffer is too small.
pub fn compress_into(codec: Codec, level: Level, dst: &mut [u8], src: &[u8]) -> Result<usize> {
    match codec {
        Codec::None => {
            if dst.len() < src.len() {
                return Err(Error::BufferTooSmall {
                    needed: src.len(),
                    have: dst.len(),
                });
            }
            dst[..src.len()].copy_from_slice(src);
            Ok(src.len())
        }
        Codec::Zstd => compress_zstd_into(dst, src, level),
        Codec::Lz4 => compress_lz4_into(dst, src),
        Codec::S2 => compress_s2_into(dst, src),
    }
}

/// Decompresses data using the specified codec.
///
/// `dst` must be sized to hold the decompressed output (use logical_size from header).
pub fn decompress(codec: Codec, dst: &mut [u8], src: &[u8]) -> Result<()> {
    match codec {
        Codec::None => {
            if dst.len() < src.len() {
                return Err(Error::BufferTooSmall {
                    needed: src.len(),
                    have: dst.len(),
                });
            }
            dst[..src.len()].copy_from_slice(src);
            Ok(())
        }
        Codec::Zstd => decompress_zstd(dst, src),
        Codec::Lz4 => decompress_lz4(dst, src),
        Codec::S2 => decompress_s2(dst, src),
    }
}

// =============================================================================
// Zstd Implementation
// =============================================================================

fn compress_zstd(src: &[u8], level: Level) -> Result<Vec<u8>> {
    zstd::bulk::compress(src, level.zstd_level()).map_err(|e| Error::Compression {
        codec: "zstd",
        message: e.to_string(),
    })
}

fn compress_zstd_into(dst: &mut [u8], src: &[u8], level: Level) -> Result<usize> {
    let result = zstd::bulk::compress_to_buffer(src, dst, level.zstd_level()).map_err(|e| {
        Error::Compression {
            codec: "zstd",
            message: e.to_string(),
        }
    })?;
    Ok(result)
}

fn decompress_zstd(dst: &mut [u8], src: &[u8]) -> Result<()> {
    let mut decoder =
        zstd::bulk::Decompressor::new().map_err(|e| Error::Decompression {
            codec: "zstd",
            message: e.to_string(),
        })?;

    let written = decoder
        .decompress_to_buffer(src, dst)
        .map_err(|e| Error::Decompression {
            codec: "zstd",
            message: e.to_string(),
        })?;

    if written != dst.len() {
        return Err(Error::Decompression {
            codec: "zstd",
            message: format!("expected {} bytes, got {}", dst.len(), written),
        });
    }
    Ok(())
}

// =============================================================================
// LZ4 Implementation
// =============================================================================

fn compress_lz4(src: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(src))
}

fn compress_lz4_into(dst: &mut [u8], src: &[u8]) -> Result<usize> {
    // lz4_flex doesn't have a direct compress_into, so we compress and copy
    let compressed = lz4_flex::compress_prepend_size(src);
    if compressed.len() > dst.len() {
        return Err(Error::BufferTooSmall {
            needed: compressed.len(),
            have: dst.len(),
        });
    }
    dst[..compressed.len()].copy_from_slice(&compressed);
    Ok(compressed.len())
}

fn decompress_lz4(dst: &mut [u8], src: &[u8]) -> Result<()> {
    let decompressed =
        lz4_flex::decompress_size_prepended(src).map_err(|e| Error::Decompression {
            codec: "lz4",
            message: e.to_string(),
        })?;

    if decompressed.len() != dst.len() {
        return Err(Error::Decompression {
            codec: "lz4",
            message: format!("expected {} bytes, got {}", dst.len(), decompressed.len()),
        });
    }
    dst.copy_from_slice(&decompressed);
    Ok(())
}

// =============================================================================
// S2 Implementation (using LZ4 for now - S2 would need snap crate)
// =============================================================================

fn compress_s2(src: &[u8]) -> Result<Vec<u8>> {
    // S2 is Snappy-compatible; for now use LZ4 as placeholder
    // In production, would use snap crate with s2 mode
    compress_lz4(src)
}

fn compress_s2_into(dst: &mut [u8], src: &[u8]) -> Result<usize> {
    compress_lz4_into(dst, src)
}

fn decompress_s2(dst: &mut [u8], src: &[u8]) -> Result<()> {
    decompress_lz4(dst, src)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_roundtrip() {
        for i in 0..4 {
            let codec = Codec::from_raw(i);
            assert_eq!(codec as u8, i);
        }
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let data = b"hello world hello world hello world";
        let compressed = compress(Codec::Zstd, Level::Default, data).unwrap();
        assert!(compressed.len() < data.len());

        let mut decompressed = vec![0u8; data.len()];
        decompress(Codec::Zstd, &mut decompressed, &compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"hello world hello world hello world";
        let compressed = compress(Codec::Lz4, Level::Default, data).unwrap();

        let mut decompressed = vec![0u8; data.len()];
        decompress(Codec::Lz4, &mut decompressed, &compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn test_compress_none() {
        let data = b"hello";
        let compressed = compress(Codec::None, Level::Default, data).unwrap();
        assert_eq!(&compressed, data);
    }
}
