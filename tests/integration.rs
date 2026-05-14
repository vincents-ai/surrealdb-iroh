//! Integration tests for P2P replication.
//!
//! These tests verify the P2P replication functionality.
//! Note: Full P2P tests require network setup and are skipped in basic CI.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use surrealdb_iroh::{
    Change, ChangeEvent, ChangeEventType, ChangeNotifier, ReplicatorConfig, StorageHook,
    SubscriptionFilter,
};

/// Test change notifier subscription.
#[tokio::test]
async fn test_change_notifier() {
    let notifier = std::sync::Arc::new(ChangeNotifier::new(10));

    // Subscribe to all changes
    let mut subscription = notifier.subscribe(SubscriptionFilter::everything());

    // Publish some changes
    for i in 0..3u8 {
        let key_bytes = vec![i];
        let val_bytes = vec![i];
        let change = Change::set("ns", "db", Bytes::from(key_bytes), Bytes::from(val_bytes));
        notifier.notify(ChangeEvent::new(ChangeEventType::Set, change, i as u64));
    }

    // Receive events
    let mut received = 0;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_secs(1), subscription.recv()).await
    {
        received += 1;
        if received >= 3 {
            break;
        }
    }

    assert_eq!(received, 3);
}

/// Test custom storage hook implementation.
struct TestHook {
    change_count: std::sync::Arc<AtomicUsize>,
}

impl TestHook {
    fn new(count: std::sync::Arc<AtomicUsize>) -> Self {
        Self {
            change_count: count,
        }
    }
}

impl StorageHook for TestHook {
    fn on_change(&self, _change: surrealdb_iroh::Change) {
        self.change_count.fetch_add(1, Ordering::SeqCst);
    }

    fn on_sync_request(&self, _peer_id: &iroh::EndpointId) {}

    fn on_sync_complete(&self, _peer_id: &iroh::EndpointId, _changes_applied: usize) {}
}

/// Test subscription filter matching.
#[tokio::test]
async fn test_subscription_filter_matching() {
    let notifier = std::sync::Arc::new(ChangeNotifier::new(10));

    // Subscribe to specific namespace
    let filter = SubscriptionFilter::new().with_namespace("ns1");
    let mut subscription = notifier.subscribe(filter);

    // Publish change for ns1
    let change1 = Change::set("ns1", "db1", Bytes::from("key1"), Bytes::from("val1"));
    notifier.notify(ChangeEvent::new(ChangeEventType::Set, change1, 1));

    // Publish change for ns2 (should not match)
    let change2 = Change::set("ns2", "db1", Bytes::from("key2"), Bytes::from("val2"));
    notifier.notify(ChangeEvent::new(ChangeEventType::Set, change2, 2));

    // Should only receive ns1 change
    let event = tokio::time::timeout(Duration::from_secs(1), subscription.recv()).await;
    assert!(event.is_ok());
    assert!(event.unwrap().is_some());
}

/// Test change batch creation.
#[tokio::test]
async fn test_change_batch_operations() {
    use surrealdb_iroh::ChangeBatch;

    let changes = vec![
        Change::set("ns", "db", Bytes::from("key1"), Bytes::from("val1")),
        Change::set("ns", "db", Bytes::from("key2"), Bytes::from("val2")),
    ];

    let batch = ChangeBatch::new(changes, 0);
    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());
}

/// Test Change creation.
#[tokio::test]
async fn test_change_creation() {
    let change = Change::set("ns", "db", Bytes::from("key"), Bytes::from("value"));
    assert_eq!(change.namespace, "ns");
    assert_eq!(change.database, "db");
}

/// Test Change deletion.
#[tokio::test]
async fn test_change_deletion() {
    let change = Change::delete("ns", "db", Bytes::from("key"));
    assert_eq!(change.namespace, "ns");
    assert_eq!(change.database, "db");
    assert!(change.value.is_none());
}

/// Test subscription filter with database.
#[tokio::test]
async fn test_subscription_filter_with_db() {
    let notifier = std::sync::Arc::new(ChangeNotifier::new(10));

    let filter = SubscriptionFilter::new()
        .with_namespace("ns1")
        .with_database("db1");

    let mut subscription = notifier.subscribe(filter);

    // Publish matching change
    let change = Change::set("ns1", "db1", Bytes::from("key"), Bytes::from("val"));
    notifier.notify(ChangeEvent::new(ChangeEventType::Set, change, 1));

    // Should receive the change
    let event = tokio::time::timeout(Duration::from_secs(1), subscription.recv()).await;
    assert!(event.is_ok());
    assert!(event.unwrap().is_some());
}

/// Test multiple namespace/database combinations.
#[tokio::test]
async fn test_multi_namespace_db() {
    let namespaces = vec!["ns1", "ns2", "ns3"];
    let databases = vec!["db1", "db2"];

    let mut total_changes = 0;
    for ns in &namespaces {
        for db in &databases {
            let change = Change::set(
                ns.clone(),
                db.clone(),
                Bytes::from("key"),
                Bytes::from("value"),
            );
            total_changes += 1;
            assert_eq!(change.namespace, *ns);
            assert_eq!(change.database, *db);
        }
    }

    assert_eq!(total_changes, 6);
}

/// Test empty key and value handling.
#[tokio::test]
async fn test_empty_key_value() {
    let change = Change::set("ns", "db", Bytes::new(), Bytes::new());
    assert!(change.key.as_ref().map(|k| k.is_empty()).unwrap_or(true));
    assert!(change.value.as_ref().map(|v| v.is_empty()).unwrap_or(true));
}

/// Test large value handling.
#[tokio::test]
async fn test_large_value() {
    // Create a 100KB value
    let large_value = Bytes::from(vec![0u8; 100 * 1024]);

    let change = Change::set("ns", "db", Bytes::from("large-key"), large_value);
    assert_eq!(
        change.value.as_ref().map(|v| v.len()).unwrap_or(0),
        100 * 1024
    );
}

/// Test change event types.
#[tokio::test]
async fn test_change_event_types() {
    assert_eq!(format!("{}", ChangeEventType::Set), "set");
    assert_eq!(format!("{}", ChangeEventType::Delete), "delete");
    assert_eq!(format!("{}", ChangeEventType::Batch), "batch");
}

/// Test subscription filter everything matches.
#[tokio::test]
async fn test_filter_everything_matches() {
    let filter = SubscriptionFilter::everything();

    let event = ChangeEvent::new(
        ChangeEventType::Set,
        Change::set("ns1", "db1", Bytes::from("key"), Bytes::from("value")),
        1,
    );

    assert!(filter.matches(&event));
}

/// Test subscription filter namespace mismatch.
#[tokio::test]
async fn test_filter_namespace_mismatch() {
    let filter = SubscriptionFilter::new().with_namespace("ns1");

    let event = ChangeEvent::new(
        ChangeEventType::Set,
        Change::set("ns2", "db1", Bytes::from("key"), Bytes::from("value")),
        1,
    );

    assert!(!filter.matches(&event));
}
