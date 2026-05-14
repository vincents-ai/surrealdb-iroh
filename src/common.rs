//! Common utilities for Iroh Sync replication.
//!
//! This module provides shared functionality used across multiple modules,
//! including encoding utilities, timestamp generation, and other common operations.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Base64 URL-Safe Encoding
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Encode bytes to base64 URL-safe without padding.
///
/// Used for ticket encoding and peer ID serialization.
pub fn base64_url_encode(bytes: &[u8]) -> String {
	const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	let mut result = String::new();
	for chunk in bytes.chunks(3) {
		let mut n: u32 = 0;
		for (i, &byte) in chunk.iter().enumerate() {
			n |= (byte as u32) << (16 - i * 8);
		}
		// Determine number of output characters based on input length
		let chars = match chunk.len() {
			3 => 4,
			2 => 3,
			1 => 2, // 1 byte -> 2 chars (12 bits, using 8 bits of data)
			_ => 0, // Should never happen with chunks of 3
		};
		for i in 0..chars {
			let idx = ((n >> (18 - i * 6)) & 0x3F) as usize;
			result.push(ALPHABET[idx] as char);
		}
	}
	result
}

/// Decode base64 URL-safe without padding.
///
/// Returns the decoded bytes or an error message if decoding fails.
pub fn base64_url_decode(input: &str) -> Result<Vec<u8>, &'static str> {
	const DECODE_TABLE: [i8; 256] = {
		let mut table = [-1i8; 256];
		let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
		let mut i = 0;
		while i < alphabet.len() {
			table[alphabet[i] as usize] = i as i8;
			i += 1;
		}
		table
	};

	let input = input.as_bytes();
	let mut result = Vec::with_capacity(input.len() * 3 / 4);
	let mut buffer: u32 = 0;
	let mut bits = 0;

	for &byte in input {
		let value = DECODE_TABLE[byte as usize];
		if value < 0 {
			return Err("invalid base64 character");
		}
		buffer = (buffer << 6) | (value as u32);
		bits += 6;
		if bits >= 8 {
			bits -= 8;
			result.push((buffer >> bits) as u8);
		}
	}

	Ok(result)
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Timestamp Utilities
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Get current timestamp in nanoseconds since UNIX epoch.
///
/// Returns 0 if the system time is before UNIX epoch (should never happen).
pub fn current_timestamp_nanos() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// Get current timestamp in seconds since UNIX epoch.
///
/// Returns 0 if the system time is before UNIX epoch (should never happen).
pub fn current_timestamp_secs() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Convert a Duration to milliseconds as u64.
pub fn duration_as_ms(d: Duration) -> u64 {
	d.as_millis() as u64
}

/// Convert a Duration to microseconds as u64.
pub fn duration_as_micros(d: Duration) -> u64 {
	d.as_micros() as u64
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_base64_url_roundtrip() {
		let original = b"Hello, World! This is a test.";
		let encoded = base64_url_encode(original);
		let decoded = base64_url_decode(&encoded).unwrap();
		assert_eq!(original.as_slice(), decoded.as_slice());
	}

	#[test]
	fn test_base64_url_no_padding() {
		let data = b"test";
		let encoded = base64_url_encode(data);
		assert!(!encoded.contains('='));
	}

	#[test]
	fn test_base64_url_decode_invalid_char() {
		let result = base64_url_decode("invalid!!!");
		assert!(result.is_err());
	}

	#[test]
	fn test_base64_url_decode_empty() {
		let result = base64_url_decode("").unwrap();
		assert!(result.is_empty());
	}

	#[test]
	fn test_current_timestamp_nanos() {
		let ts = current_timestamp_nanos();
		assert!(ts > 0, "timestamp should be positive");
	}

	#[test]
	fn test_current_timestamp_secs() {
		let ts = current_timestamp_secs();
		assert!(ts > 0, "timestamp should be positive");
	}
}
