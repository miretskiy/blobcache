//! 128-bit XXH3 key implementation.
//!
//! Keys in Ferum are 128-bit XXH3 hashes of the original key bytes.
//! This provides:
//! - Fixed-size keys for efficient index storage
//! - Excellent distribution for hash tables
//! - Fast computation (XXH3 is one of the fastest non-crypto hashes)

use std::fmt;

use xxhash_rust::xxh3::xxh3_128;

/// A 128-bit key derived from XXH3 hash of the original key bytes.
///
/// The key is stored as two 64-bit halves (high, low) for efficient
/// comparison and storage in the sharded index.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Key {
    /// High 64 bits of the XXH3-128 hash.
    pub high: u64,
    /// Low 64 bits of the XXH3-128 hash.
    pub low: u64,
}

impl Key {
    /// Creates a new key from high and low 64-bit parts.
    pub const fn new(high: u64, low: u64) -> Self {
        Key { high, low }
    }

    /// Creates a key from hi and lo parts (for decoding from Go format).
    pub const fn from_parts(hi: u64, lo: u64) -> Self {
        Key { high: hi, low: lo }
    }

    /// Creates a key by hashing the provided bytes with XXH3-128.
    pub fn from_bytes(data: &[u8]) -> Self {
        let hash = xxh3_128(data);
        Key {
            high: (hash >> 64) as u64,
            low: hash as u64,
        }
    }

    /// Returns the shard index for this key (0-255).
    ///
    /// Uses the high byte of the high 64-bit part for even distribution.
    #[inline]
    pub fn shard(&self) -> u8 {
        (self.high >> 56) as u8
    }

    /// Returns the key as a 128-bit integer.
    pub fn as_u128(&self) -> u128 {
        ((self.high as u128) << 64) | (self.low as u128)
    }

    /// Creates a key from a 128-bit integer.
    pub fn from_u128(value: u128) -> Self {
        Key {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }

    /// Encodes the key into a 16-byte buffer.
    pub fn encode(&self, buf: &mut [u8; 16]) {
        buf[0..8].copy_from_slice(&self.high.to_le_bytes());
        buf[8..16].copy_from_slice(&self.low.to_le_bytes());
    }

    /// Returns the key as a 16-byte array.
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        self.encode(&mut buf);
        buf
    }

    /// Decodes a key from a 16-byte buffer.
    pub fn decode(buf: &[u8; 16]) -> Self {
        Key {
            high: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            low: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        }
    }

    /// Returns true if the key is all zeros (invalid/uninitialized).
    pub fn is_zero(&self) -> bool {
        self.high == 0 && self.low == 0
    }

    /// Creates a zero key.
    pub const fn zero() -> Self {
        Key { high: 0, low: 0 }
    }

    /// Returns the high 64 bits.
    #[inline]
    pub fn hi(&self) -> u64 {
        self.high
    }

    /// Returns the low 64 bits.
    #[inline]
    pub fn lo(&self) -> u64 {
        self.low
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({:016x}{:016x})", self.high, self.low)
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}{:016x}", self.high, self.low)
    }
}

impl From<&[u8]> for Key {
    fn from(data: &[u8]) -> Self {
        Key::from_bytes(data)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Key::from_bytes(s.as_bytes())
    }
}

impl From<u128> for Key {
    fn from(value: u128) -> Self {
        Key::from_u128(value)
    }
}

impl From<Key> for u128 {
    fn from(key: Key) -> Self {
        key.as_u128()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_from_bytes() {
        let key1 = Key::from_bytes(b"hello");
        let key2 = Key::from_bytes(b"hello");
        let key3 = Key::from_bytes(b"world");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_key_encode_decode() {
        let original = Key::from_bytes(b"test key");
        let mut buf = [0u8; 16];
        original.encode(&mut buf);
        let decoded = Key::decode(&buf);
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_key_shard_distribution() {
        // Test that different keys map to different shards
        let mut shards = std::collections::HashSet::new();
        for i in 0..1000 {
            let key = Key::from_bytes(format!("key-{}", i).as_bytes());
            shards.insert(key.shard());
        }
        // With 1000 random keys, we should hit most of the 256 shards
        assert!(shards.len() > 200);
    }

    #[test]
    fn test_key_u128_roundtrip() {
        let original = Key::from_bytes(b"roundtrip test");
        let as_u128 = original.as_u128();
        let back = Key::from_u128(as_u128);
        assert_eq!(original, back);
    }

    #[test]
    fn test_key_display() {
        let key = Key::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let display = format!("{}", key);
        assert_eq!(display, "0123456789abcdeffedcba9876543210");
    }

    #[test]
    fn test_key_is_zero() {
        let zero = Key::default();
        assert!(zero.is_zero());

        let non_zero = Key::from_bytes(b"not zero");
        assert!(!non_zero.is_zero());
    }
}
