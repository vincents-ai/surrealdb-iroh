//! Change encoding and decoding for sync operations.
//!
//! This module handles the serialization format for database changes that
//! are synchronized between peers.

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::current_timestamp_nanos;

/// Represents a single change to the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
	/// Unique identifier for this change
	pub id: Uuid,
	/// Timestamp when the change was made
	pub timestamp: u64,
	/// The namespace affected
	pub namespace: String,
	/// The database affected
	pub database: String,
	/// The table affected (if applicable)
	pub table: Option<String>,
	/// The type of operation
	pub operation: ChangeOperation,
	/// The key affected (for low-level key-value operations)
	pub key: Option<Bytes>,
	/// The serialized value (for set operations)
	pub value: Option<Bytes>,
	/// Additional metadata
	#[serde(default)]
	pub metadata: HashMap<String, String>,
}

/// Type of change operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOperation {
	/// A query was executed
	Query,
	/// A key-value pair was set
	Set,
	/// A key was deleted
	Delete,
	/// A transaction was committed
	Commit,
	/// Schema change (DEFINE statement)
	Schema,
	/// Authentication change
	Auth,
	/// Live query notification
	LiveNotify,
}

impl Change {
	/// Create a new change for a query execution.
	pub fn query(
		namespace: impl Into<String>,
		database: impl Into<String>,
		table: Option<String>,
	) -> Self {
		Self {
			id: Uuid::new_v4(),
			timestamp: current_timestamp_nanos(),
			namespace: namespace.into(),
			database: database.into(),
			table,
			operation: ChangeOperation::Query,
			key: None,
			value: None,
			metadata: HashMap::new(),
		}
	}

	/// Create a new key-value set change.
	pub fn set(
		namespace: impl Into<String>,
		database: impl Into<String>,
		key: Bytes,
		value: Bytes,
	) -> Self {
		Self {
			id: Uuid::new_v4(),
			timestamp: current_timestamp_nanos(),
			namespace: namespace.into(),
			database: database.into(),
			table: None,
			operation: ChangeOperation::Set,
			key: Some(key),
			value: Some(value),
			metadata: HashMap::new(),
		}
	}

	/// Create a new delete change.
	pub fn delete(namespace: impl Into<String>, database: impl Into<String>, key: Bytes) -> Self {
		Self {
			id: Uuid::new_v4(),
			timestamp: current_timestamp_nanos(),
			namespace: namespace.into(),
			database: database.into(),
			table: None,
			operation: ChangeOperation::Delete,
			key: Some(key),
			value: None,
			metadata: HashMap::new(),
		}
	}
}

/// A batch of changes for efficient sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeBatch {
	/// Offset in the change log (for resumable sync)
	pub offset: u64,
	/// The changes in this batch
	pub changes: Vec<Change>,
	/// Whether this is the final batch
	#[serde(rename = "is_final")]
	pub is_final: bool,
}

#[allow(dead_code)]
impl ChangeBatch {
	/// Create a new change batch.
	pub fn new(offset: u64, changes: Vec<Change>) -> Self {
		Self {
			offset,
			changes,
			is_final: false,
		}
	}

	/// Mark this batch as final.
	pub fn with_final(mut self) -> Self {
		self.is_final = true;
		self
	}

	/// Get the total number of changes.
	pub fn len(&self) -> usize {
		self.changes.len()
	}

	/// Check if the batch is empty.
	pub fn is_empty(&self) -> bool {
		self.changes.is_empty()
	}
}

/// Message types for the sync protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum SyncMessage {
	/// Request changes since a given offset
	Request {
		offset: u64,
		limit: Option<usize>,
	},
	/// Response containing changes
	Response(ChangeBatch),
	/// Acknowledgment of received changes
	Ack {
		offset: u64,
	},
	/// Ping for keepalive
	Ping,
	/// Pong response
	Pong,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Serialization
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Encode a sync message to bytes using JSON.
pub fn encode_message(msg: &SyncMessage) -> Result<Bytes, bincode::Error> {
	// Use JSON for encoding due to bincode's limitation with externally tagged enums
	let json = serde_json::to_string(msg)
		.map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e.to_string())))?;
	Ok(Bytes::from(json.into_bytes()))
}

/// Decode a sync message from bytes using JSON.
pub fn decode_message(data: &[u8]) -> Result<SyncMessage, bincode::Error> {
	let json_str = String::from_utf8(data.to_vec())
		.map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e.to_string())))?;
	serde_json::from_str(&json_str)
		.map_err(|e| bincode::Error::new(bincode::ErrorKind::Custom(e.to_string())))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_change_encoding() {
		// Test encoding/decoding of sync messages
		let msg = SyncMessage::Request {
			offset: 0,
			limit: None,
		};

		let encoded = encode_message(&msg);
		assert!(encoded.is_ok(), "encoding failed: {:?}", encoded.err());

		let decoded = decode_message(&encoded.unwrap());
		assert!(decoded.is_ok(), "decoding failed: {:?}", decoded.err());

		match decoded.unwrap() {
			SyncMessage::Request {
				offset,
				..
			} => {
				assert_eq!(offset, 0);
			}
			_ => panic!("expected Request variant"),
		}
	}

	#[test]
	fn test_change_batch() {
		let batch = ChangeBatch::new(
			0,
			vec![
				Change::set("ns", "db", Bytes::from("key1"), Bytes::from("value1")),
				Change::set("ns", "db", Bytes::from("key2"), Bytes::from("value2")),
			],
		);
		assert_eq!(batch.len(), 2);
	}
}
