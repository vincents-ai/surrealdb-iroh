#![allow(dead_code)]
//! Compression for sync messages using zstd.
//!
//! This module provides optional compression to reduce bandwidth
//! when syncing changes between peers.

use bytes::{Bytes, BytesMut};

/// Compression configuration.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Enable compression
    pub enabled: bool,
    /// Compression level (1-22, default 3)
    pub level: i32,
    /// Minimum size to compress (bytes)
    pub min_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            level: 3,
            min_size: 64, // Don't compress small messages
        }
    }
}

impl CompressionConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable compression.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the compression level (1-22).
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level.clamp(1, 22);
        self
    }

    /// Set the minimum size to compress.
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_size = size;
        self
    }
}

/// Compression result with metadata.
#[derive(Debug)]
pub struct CompressedData {
    /// The compressed data
    pub data: Bytes,
    /// Original size before compression
    pub original_size: usize,
    /// Whether compression was applied
    pub compressed: bool,
}

/// Compress data using zstd.
#[cfg(feature = "compression")]
pub fn compress(data: &[u8], config: &CompressionConfig) -> CompressedData {
    use zstd::compress;

    let original_size = data.len();

    // Don't compress small data
    if !config.enabled || original_size < config.min_size {
        return CompressedData {
            data: Bytes::copy_from_slice(data),
            original_size,
            compressed: false,
        };
    }

    // Compress
    let compressed = compress(data, config.level);

    // Use compressed data if it's smaller
    if compressed.len() < original_size {
        CompressedData {
            data: Bytes::from(compressed),
            original_size,
            compressed: true,
        }
    } else {
        CompressedData {
            data: Bytes::copy_from_slice(data),
            original_size,
            compressed: false,
        }
    }
}

/// Decompress zstd-compressed data.
#[cfg(feature = "compression")]
pub fn decompress(data: &[u8], expected_size: Option<usize>) -> Result<Bytes, CompressionError> {
    use zstd::decompress;

    let output_size = expected_size.unwrap_or(10 * 1024 * 1024); // Default 10MB max
    let mut output = Vec::with_capacity(output_size.min(data.len() * 10));

    decompress(data, output_size, &mut output)
        .map(|size| {
            output.truncate(size);
            Bytes::from(output)
        })
        .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
}

/// Encode a message with optional compression.
#[cfg(feature = "compression")]
pub fn encode_compressed(data: &[u8], config: &CompressionConfig) -> BytesMut {
    let compressed = compress(data, config);

    let mut result = BytesMut::new();

    // Write header byte: bit 0 = compressed flag
    let header = if compressed.compressed { 1u8 } else { 0u8 };
    result.push(header);

    // Write original size if compressed
    if compressed.compressed {
        result.extend_from_slice(&(compressed.original_size as u32).to_be_bytes());
    }

    // Write data
    result.extend_from_slice(&compressed.data);

    result
}

/// Decode a message with optional decompression.
#[cfg(feature = "compression")]
pub fn decode_compressed(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty data".to_string()));
    }

    let header = data[0];
    let compressed = (header & 1) == 1;

    if compressed {
        if data.len() < 5 {
            return Err(CompressionError::InvalidData(
                "truncated compressed data".to_string(),
            ));
        }

        let original_size = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let compressed_data = &data[5..];

        decompress(compressed_data, Some(original_size))
    } else {
        Ok(Bytes::copy_from_slice(&data[1..]))
    }
}

// No-op implementations when compression is disabled

#[cfg(not(feature = "compression"))]
pub fn compress(_data: &[u8], _config: &CompressionConfig) -> CompressedData {
    CompressedData {
        data: Bytes::copy_from_slice(_data),
        original_size: _data.len(),
        compressed: false,
    }
}

#[cfg(not(feature = "compression"))]
pub fn decompress(_data: &[u8], _expected_size: Option<usize>) -> Result<Bytes, CompressionError> {
    Ok(Bytes::copy_from_slice(_data))
}

#[cfg(not(feature = "compression"))]
pub fn encode_compressed(data: &[u8], _config: &CompressionConfig) -> BytesMut {
    let mut result = BytesMut::new();
    result.extend_from_slice(data);
    result
}

#[cfg(not(feature = "compression"))]
pub fn decode_compressed(data: &[u8]) -> Result<Bytes, CompressionError> {
    Ok(Bytes::copy_from_slice(data))
}

/// Compression error types.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    /// Decompression failed
    #[error("decompression failed: {0}")]
    DecompressionFailed(String),

    /// Invalid data format
    #[error("invalid data: {0}")]
    InvalidData(String),

    /// Output buffer too small
    #[error("buffer too small: needed {0}, got {1}")]
    BufferTooSmall(usize, usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.level, 3);
        assert_eq!(config.min_size, 64);
    }

    #[test]
    fn test_compression_disabled() {
        let config = CompressionConfig::new().with_enabled(false);
        let data = b"hello world this is some data to compress";
        let result = compress(data, &config);
        assert!(!result.compressed);
        assert_eq!(result.data.as_ref(), data);
    }

    #[test]
    fn test_compression_small_data() {
        let config = CompressionConfig::new().with_min_size(1024);
        let data = b"short";
        let result = compress(data, &config);
        assert!(!result.compressed);
    }

    #[test]
    fn test_compression_roundtrip() {
        #[cfg(feature = "compression")]
        {
            let config = CompressionConfig::default();
            let data = b"This is a much longer piece of data that should compress well because it has repetition and patternssss";

            let encoded = encode_compressed(data, &config);
            let decoded = decode_compressed(&encoded).unwrap();

            assert_eq!(decoded.as_ref(), data);
        }
    }
}
