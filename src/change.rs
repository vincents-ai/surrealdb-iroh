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
    Request { offset: u64, limit: Option<usize> },
    /// Response containing changes
    Response(ChangeBatch),
    /// Acknowledgment of received changes
    Ack { offset: u64 },
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
    use pretty_assertions::assert_eq;

    #[test]
    fn test_change_encoding() {
        let msg = SyncMessage::Request {
            offset: 0,
            limit: None,
        };

        let encoded = encode_message(&msg);
        assert!(encoded.is_ok(), "encoding failed: {:?}", encoded.err());

        let decoded = decode_message(&encoded.unwrap());
        assert!(decoded.is_ok(), "decoding failed: {:?}", decoded.err());

        match decoded.unwrap() {
            SyncMessage::Request { offset, .. } => {
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

/// Property-based tests using proptest
#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    // Implement Arbitrary for ChangeOperation (simple enum)
    impl Arbitrary for ChangeOperation {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(ChangeOperation::Query),
                Just(ChangeOperation::Set),
                Just(ChangeOperation::Delete),
                Just(ChangeOperation::Commit),
                Just(ChangeOperation::Schema),
                Just(ChangeOperation::Auth),
                Just(ChangeOperation::LiveNotify),
            ]
            .boxed()
        }
    }

    // Implement Arbitrary for SyncMessage
    impl Arbitrary for SyncMessage {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(SyncMessage::Ping),
                Just(SyncMessage::Pong),
                (any::<u64>()).prop_map(|n| SyncMessage::Pong),
                ((any::<u64>(), any::<Option<usize>>()))
                    .prop_map(|(offset, limit)| { SyncMessage::Request { offset, limit } }),
                (any::<u64>()).prop_map(|offset| SyncMessage::Ack { offset }),
            ]
            .boxed()
        }
    }

    /// Test that encode/decode roundtrip works for all message types.
    #[test]
    fn test_sync_message_encode_decode_roundtrip() {
        let config = ProptestConfig::with_cases(16);
        let mut runner = TestRunner::new(config);

        // Test Request variant
        runner
            .run(
                &((0u64..10000).prop_map(|offset| SyncMessage::Request {
                    offset,
                    limit: None,
                })),
                |msg| {
                    let encoded = encode_message(&msg).unwrap();
                    let decoded = decode_message(&encoded).unwrap();
                    assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
                    Ok(())
                },
            )
            .expect("Request roundtrip should pass");

        // Test Ack variant
        runner
            .run(
                &((0u64..10000).prop_map(|offset| SyncMessage::Ack { offset })),
                |msg| {
                    let encoded = encode_message(&msg).unwrap();
                    let decoded = decode_message(&encoded).unwrap();
                    assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
                    Ok(())
                },
            )
            .expect("Ack roundtrip should pass");

        // Test Ping/Pong
        for msg in [SyncMessage::Ping, SyncMessage::Pong] {
            let encoded = encode_message(&msg).unwrap();
            let decoded = decode_message(&encoded).unwrap();
            assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
        }
    }

    /// Test that SyncMessage serializes to valid JSON with type field.
    #[test]
    fn test_sync_message_serializes_to_valid_json() {
        let config = ProptestConfig::with_cases(16);
        let mut runner = TestRunner::new(config);

        runner
            .run(&any::<SyncMessage>(), |msg| {
                let json = serde_json::to_string(&msg).unwrap();
                let parsed: serde_json::Value = json.parse().unwrap();
                assert!(
                    parsed.get("type").is_some(),
                    "SyncMessage should have type field"
                );
                Ok(())
            })
            .expect("JSON serialization should pass");
    }

    /// Test that ChangeOperation serializes and deserializes correctly.
    #[test]
    fn test_change_operation_serialization() {
        let config = ProptestConfig::with_cases(16);
        let mut runner = TestRunner::new(config);

        runner
            .run(&any::<ChangeOperation>(), |op| {
                let json = serde_json::to_string(&op).unwrap();
                let back = serde_json::from_str::<ChangeOperation>(&json).unwrap();
                assert_eq!(op, back);
                Ok(())
            })
            .expect("operation serialization should pass");
    }

    /// Test batch length consistency.
    #[test]
    fn test_batch_len_consistency() {
        // Test empty batch
        let batch = ChangeBatch::new(0, vec![]);
        assert_eq!(batch.is_empty(), batch.len() == 0);
        assert_eq!(!batch.is_empty(), batch.len() > 0);
    }

    /// Test batch clone preserves data.
    #[test]
    fn test_batch_clone_preserves_data() {
        let config = ProptestConfig::with_cases(16);
        let mut runner = TestRunner::new(config);

        runner
            .run(
                &((any::<u64>(), any::<bool>(), any::<usize>())).prop_map(
                    |(offset, is_final, len)| {
                        let changes: Vec<Change> = (0..len.min(100))
                            .map(|_| Change::query("namespace", "database", None))
                            .collect();
                        let mut batch = ChangeBatch::new(offset, changes);
                        batch.is_final = is_final;
                        batch
                    },
                ),
                |batch| {
                    let cloned = batch.clone();
                    assert_eq!(cloned.offset, batch.offset);
                    assert_eq!(cloned.is_final, batch.is_final);
                    assert_eq!(cloned.changes.len(), batch.changes.len());
                    Ok(())
                },
            )
            .expect("clone should preserve data");
    }

    /// Test batch offset is preserved.
    #[test]
    fn test_batch_offset_preserved() {
        let config = ProptestConfig::with_cases(16);
        let mut runner = TestRunner::new(config);

        runner
            .run(&any::<u64>(), |offset| {
                let batch = ChangeBatch::new(offset, vec![]);
                assert_eq!(batch.offset, offset);
                Ok(())
            })
            .expect("offset should be preserved");
    }

    /// Test timestamp monotonicity.
    #[test]
    fn test_timestamp_monotonic() {
        // Note: This test verifies that multiple calls to current_timestamp_nanos
        // are monotonically non-decreasing (within the same test run)
        let mut prev = 0u64;
        for _ in 0..100 {
            let ts = current_timestamp_nanos();
            assert!(ts >= prev, "timestamps should be monotonic");
            prev = ts;
        }
    }

    /// Test change IDs are unique within a single thread.
    #[test]
    fn test_change_ids_are_unique() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..100 {
            let c = Change::query("ns", "db", None);
            assert!(ids.insert(c.id), "Change IDs should be unique");
        }
    }

    /// Test all SyncMessage variants roundtrip.
    #[test]
    fn test_all_sync_message_variants() {
        // Test Ping
        let encoded = encode_message(&SyncMessage::Ping).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        assert!(matches!(decoded, SyncMessage::Ping));

        // Test Pong
        let encoded = encode_message(&SyncMessage::Pong).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        assert!(matches!(decoded, SyncMessage::Pong));

        // Test Request with limit
        let msg = SyncMessage::Request {
            offset: 42,
            limit: Some(100),
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        match decoded {
            SyncMessage::Request { offset, limit } => {
                assert_eq!(offset, 42);
                assert_eq!(limit, Some(100));
            }
            _ => panic!("expected Request"),
        }

        // Test Response
        let batch = ChangeBatch::new(
            10,
            vec![Change::set("ns", "db", Bytes::from("k"), Bytes::from("v"))],
        );
        let msg = SyncMessage::Response(batch);
        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        match decoded {
            SyncMessage::Response(batch) => {
                assert_eq!(batch.offset, 10);
                assert_eq!(batch.changes.len(), 1);
            }
            _ => panic!("expected Response"),
        }
    }

    /// Test large batch handling.
    #[test]
    fn test_large_batch_handling() {
        let changes: Vec<Change> = (0..1000).map(|_| Change::query("ns", "db", None)).collect();
        let batch = ChangeBatch::new(0, changes);
        assert_eq!(batch.len(), 1000);
        assert!(!batch.is_empty());

        let encoded = encode_message(&SyncMessage::Response(batch.clone())).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        match decoded {
            SyncMessage::Response(batch) => {
                assert_eq!(batch.changes.len(), 1000);
            }
            _ => panic!("expected Response"),
        }
    }

    /// Test empty batch.
    #[test]
    fn test_empty_batch() {
        let batch = ChangeBatch::new(0, vec![]);
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        let encoded = encode_message(&SyncMessage::Response(batch)).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        match decoded {
            SyncMessage::Response(batch) => {
                assert!(batch.is_empty());
                assert_eq!(batch.changes.len(), 0);
            }
            _ => panic!("expected Response"),
        }
    }

    /// Test batch with_final.
    #[test]
    fn test_batch_with_final() {
        let batch = ChangeBatch::new(0, vec![]).with_final();
        assert!(batch.is_final);

        let encoded = encode_message(&SyncMessage::Response(batch)).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        match decoded {
            SyncMessage::Response(batch) => {
                assert!(batch.is_final);
            }
            _ => panic!("expected Response"),
        }
    }

    /// Test that encode/decode preserves message semantics.
    #[test]
    fn test_encode_decode_preserves_semantics() {
        let config = ProptestConfig::with_cases(32);
        let mut runner = TestRunner::new(config);

        runner
            .run(&any::<SyncMessage>(), |msg| {
                let encoded = encode_message(&msg).unwrap();
                let decoded = decode_message(&encoded).unwrap();

                // Verify the decoded message matches the original
                match (&msg, &decoded) {
                    (
                        SyncMessage::Request {
                            offset: o1,
                            limit: l1,
                        },
                        SyncMessage::Request {
                            offset: o2,
                            limit: l2,
                        },
                    ) => {
                        assert_eq!(o1, o2);
                        assert_eq!(l1, l2);
                    }
                    (SyncMessage::Ack { offset: o1 }, SyncMessage::Ack { offset: o2 }) => {
                        assert_eq!(o1, o2);
                    }
                    _ => assert_eq!(format!("{:?}", msg), format!("{:?}", decoded)),
                }
                Ok(())
            })
            .expect("semantics should be preserved");
    }
}
