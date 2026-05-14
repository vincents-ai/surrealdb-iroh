//! Watch/notify system for event-based change notifications.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::debug;

use crate::change::Change;
use crate::common::current_timestamp_secs;

/// Type of change event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChangeEventType {
	/// A value was set
	Set,
	/// A value was deleted
	Delete,
	/// Multiple changes occurred
	Batch,
}

impl std::fmt::Display for ChangeEventType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ChangeEventType::Set => write!(f, "set"),
			ChangeEventType::Delete => write!(f, "delete"),
			ChangeEventType::Batch => write!(f, "batch"),
		}
	}
}

/// A change event with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
	/// Type of change
	pub event_type: ChangeEventType,
	/// The change data
	pub change: Change,
	/// Source peer ID (if from sync)
	pub source_peer: Option<iroh::EndpointId>,
	/// Sequence number for ordering
	pub sequence: u64,
	/// Timestamp (epoch seconds)
	pub timestamp_secs: u64,
}

impl ChangeEvent {
	/// Create a new change event.
	pub fn new(event_type: ChangeEventType, change: Change, sequence: u64) -> Self {
		Self {
			event_type,
			change,
			source_peer: None,
			sequence,
			timestamp_secs: current_timestamp_secs(),
		}
	}

	/// Create from a sync source.
	pub fn from_sync(change: Change, sequence: u64, peer: iroh::EndpointId) -> Self {
		let event_type = match &change.operation {
			crate::change::ChangeOperation::Set => ChangeEventType::Set,
			crate::change::ChangeOperation::Delete => ChangeEventType::Delete,
			_ => ChangeEventType::Set,
		};

		Self {
			event_type,
			change,
			source_peer: Some(peer),
			sequence,
			timestamp_secs: current_timestamp_secs(),
		}
	}
}

/// Subscription filter for change events.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionFilter {
	/// Match specific namespaces
	pub namespaces: Option<Vec<String>>,
	/// Match specific databases
	pub databases: Option<Vec<String>>,
}

impl SubscriptionFilter {
	/// Create a new filter that matches everything.
	pub fn new() -> Self {
		Self::default()
	}

	/// Create a new filter that matches everything.
	pub fn everything() -> Self {
		Self::default()
	}

	/// Filter by namespace.
	pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
		let ns = ns.into();
		self.namespaces = Some(match self.namespaces {
			Some(mut v) => {
				v.push(ns);
				v
			}
			None => vec![ns],
		});
		self
	}

	/// Filter by database.
	pub fn with_database(mut self, db: impl Into<String>) -> Self {
		let db = db.into();
		self.databases = Some(match self.databases {
			Some(mut v) => {
				v.push(db);
				v
			}
			None => vec![db],
		});
		self
	}

	/// Check if an event matches this filter.
	pub fn matches(&self, event: &ChangeEvent) -> bool {
		if let Some(ref namespaces) = self.namespaces {
			if !namespaces.contains(&event.change.namespace) {
				return false;
			}
		}

		if let Some(ref databases) = self.databases {
			if !databases.contains(&event.change.database) {
				return false;
			}
		}

		true
	}
}

/// Subscription handle for receiving change events.
pub struct Subscription {
	/// Unique subscription ID
	pub id: u64,
	/// Filter for this subscription
	pub filter: SubscriptionFilter,
	rx: broadcast::Receiver<Arc<ChangeEvent>>,
}

impl Subscription {
	/// Receive the next event.
	pub async fn recv(&mut self) -> Option<Arc<ChangeEvent>> {
		self.rx.recv().await.ok()
	}

	/// Try to receive without waiting.
	pub fn try_recv(&mut self) -> Option<Arc<ChangeEvent>> {
		self.rx.try_recv().ok()
	}
}

/// Subscriber entry.
struct SubscriberEntry {
	filter: SubscriptionFilter,
	sender: broadcast::Sender<Arc<ChangeEvent>>,
}

/// Event bus for distributing change notifications.
pub struct ChangeNotifier {
	/// Subscribers
	subscribers: RwLock<HashMap<u64, SubscriberEntry>>,
	/// Event sequence counter
	sequence: RwLock<u64>,
	/// Configuration
	buffer_size: usize,
}

impl ChangeNotifier {
	/// Create a new change notifier.
	pub fn new(buffer_size: usize) -> Self {
		Self {
			subscribers: RwLock::new(HashMap::new()),
			sequence: RwLock::new(0),
			buffer_size,
		}
	}

	/// Subscribe to changes with a filter.
	pub fn subscribe(&self, filter: SubscriptionFilter) -> Subscription {
		let (tx, rx) = broadcast::channel(self.buffer_size);
		let id = {
			let mut subscribers = self.subscribers.write();
			let id = subscribers.len() as u64 + 1;
			subscribers.insert(
				id,
				SubscriberEntry {
					filter: filter.clone(),
					sender: tx,
				},
			);
			id
		};

		debug!(subscriber_id = id, "subscription created");

		Subscription {
			id,
			filter,
			rx,
		}
	}

	/// Unsubscribe from changes.
	pub fn unsubscribe(&self, id: u64) {
		let mut subscribers = self.subscribers.write();
		if subscribers.remove(&id).is_some() {
			debug!(subscriber_id = id, "subscription removed");
		}
	}

	/// Publish a change event.
	pub fn notify(&self, event: ChangeEvent) {
		let event = Arc::new(event);
		let subscribers = self.subscribers.read();
		let mut count = 0;

		for (_, entry) in subscribers.iter() {
			if entry.sender.send(Arc::clone(&event)).is_ok() {
				count += 1;
			}
		}

		debug!(subscribers = count, "change event published");
	}

	/// Publish a change from a sync operation.
	pub fn notify_sync(&self, change: Change, peer: iroh::EndpointId) {
		let event = Arc::new(ChangeEvent::from_sync(change, 0, peer));
		let subscribers = self.subscribers.read();

		for (_, entry) in subscribers.iter() {
			let _ = entry.sender.send(Arc::clone(&event));
		}
	}

	/// Get the number of subscribers.
	pub fn subscriber_count(&self) -> usize {
		self.subscribers.read().len()
	}

	/// Get the current sequence number.
	pub fn sequence(&self) -> u64 {
		*self.sequence.read()
	}

	/// Increment and return the sequence number atomically.
	pub fn inc_sequence(&self) -> u64 {
		let mut seq = self.sequence.write();
		let current = *seq;
		*seq = current + 1;
		current + 1
	}
}

impl Default for ChangeNotifier {
	fn default() -> Self {
		Self::new(100)
	}
}

/// A change observer that can be integrated with the sync system.
pub struct ChangeObserver {
	notifier: Arc<ChangeNotifier>,
}

impl ChangeObserver {
	/// Create a new change observer.
	pub fn new() -> Self {
		Self {
			notifier: Arc::new(ChangeNotifier::new(100)),
		}
	}

	/// Subscribe to changes.
	pub fn subscribe(&self, filter: SubscriptionFilter) -> Subscription {
		self.notifier.subscribe(filter)
	}

	/// Unsubscribe.
	pub fn unsubscribe(&self, id: u64) {
		self.notifier.unsubscribe(id);
	}

	/// Notify of a local change.
	pub fn notify_local(&self, change: Change) {
		let seq = self.notifier.inc_sequence();

		let event_type = match change.operation {
			crate::change::ChangeOperation::Set => ChangeEventType::Set,
			crate::change::ChangeOperation::Delete => ChangeEventType::Delete,
			_ => ChangeEventType::Set,
		};

		let event = ChangeEvent {
			event_type,
			change,
			source_peer: None,
			sequence: seq,
			timestamp_secs: current_timestamp_secs(),
		};

		self.notifier.notify(event);
	}

	/// Notify of a synced change.
	pub fn notify_sync(&self, change: Change, peer: iroh::EndpointId) {
		self.notifier.notify_sync(change, peer);
	}

	/// Get subscriber count.
	pub fn subscriber_count(&self) -> usize {
		self.notifier.subscriber_count()
	}
}

impl Default for ChangeObserver {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::Bytes;

	#[test]
	fn test_subscription_filter() {
		let filter = SubscriptionFilter::everything();
		let event = ChangeEvent::new(
			ChangeEventType::Set,
			Change::set("ns1", "db1", Bytes::from("key1"), Bytes::from("value")),
			1,
		);

		assert!(filter.matches(&event));
	}

	#[test]
	fn test_subscription_filter_namespace() {
		let filter = SubscriptionFilter::new().with_namespace("ns1");

		let event1 = ChangeEvent::new(
			ChangeEventType::Set,
			Change::set("ns1", "db1", Bytes::from("key1"), Bytes::from("value")),
			1,
		);

		let event2 = ChangeEvent::new(
			ChangeEventType::Set,
			Change::set("ns2", "db1", Bytes::from("key1"), Bytes::from("value")),
			2,
		);

		assert!(filter.matches(&event1));
		assert!(!filter.matches(&event2));
	}

	#[tokio::test]
	async fn test_change_notifier() {
		let notifier = ChangeNotifier::new(10);
		let mut sub = notifier.subscribe(SubscriptionFilter::everything());

		let change = Change::set("ns", "db", Bytes::from("key"), Bytes::from("value"));
		notifier.notify(ChangeEvent::new(ChangeEventType::Set, change, 1));

		let event = sub.recv().await;
		assert!(event.is_some());

		assert_eq!(notifier.subscriber_count(), 1);
	}
}
