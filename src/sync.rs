//! Synchronization state machine and management.
//!
//! This module implements the state machine for P2P sync operations,
//! tracking changes and managing sync state between peers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use crate::change::Change;
use crate::common::base64_url_decode;
use crate::error::{Error, Result};

#[cfg(feature = "metrics")]
use crate::telemetry::set_pending_changes;

/// Sync state between two peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncState {
	/// Not connected to this peer
	Disconnected,
	/// Actively connecting to peer
	Connecting,
	/// Sync in progress
	Syncing,
	/// Fully synchronized with peer
	Synchronized,
	/// Error occurred with this peer
	Error(String),
}

/// A peer's sync state.
#[derive(Debug, Clone)]
pub struct PeerSyncState {
	/// Peer identifier
	pub peer_id: iroh::EndpointId,
	/// Current sync state
	pub state: SyncState,
	/// Last offset we've seen from this peer
	pub last_offset: u64,
	/// Last time we synced with this peer
	pub last_sync: Option<std::time::Instant>,
}

impl PeerSyncState {
	/// Create a new peer sync state.
	pub fn new(peer_id: iroh::EndpointId) -> Self {
		Self {
			peer_id,
			state: SyncState::Disconnected,
			last_offset: 0,
			last_sync: None,
		}
	}
}

/// Snapshot of sync state for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSnapshot {
	/// Current change offset
	pub offset: u64,
	/// Persisted changes (for resumable sync)
	pub changes: Vec<Change>,
	/// Peer states
	pub peers: Vec<(String, u64)>, // (peer_id_base64, last_offset)
}

/// Manages synchronization state and change tracking.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SyncManager {
	/// Current offset in the change log
	offset: Arc<AtomicU64>,
	/// In-memory change log (using VecDeque for O(1) front removal)
	changes: Arc<RwLock<VecDeque<Change>>>,
	/// Maximum number of changes to keep in memory
	max_changes: usize,
	/// Peer sync states
	peer_states: Arc<RwLock<std::collections::HashMap<String, PeerSyncState>>>,
	/// Channel for notifying of new changes
	change_tx: mpsc::Sender<Change>,
	/// Receiver for change notifications
	change_rx: RwLock<Option<mpsc::Receiver<Change>>>,
}

impl SyncManager {
	/// Create a new sync manager.
	pub fn new(max_changes: usize) -> Self {
		let (change_tx, change_rx) = mpsc::channel(1000);
		Self {
			offset: Arc::new(AtomicU64::new(0)),
			changes: Arc::new(RwLock::new(VecDeque::with_capacity(max_changes))),
			max_changes,
			peer_states: Arc::new(RwLock::new(std::collections::HashMap::new())),
			change_tx,
			change_rx: RwLock::new(Some(change_rx)),
		}
	}

	/// Get the current offset.
	pub fn offset(&self) -> u64 {
		self.offset.load(Ordering::SeqCst)
	}

	/// Get a receiver for change notifications.
	pub fn change_receiver(&self) -> Option<mpsc::Receiver<Change>> {
		self.change_rx.write().take()
	}

	/// Record a new change and return its offset.
	#[instrument(level = "debug", skip(self, change))]
	pub fn record_change(&self, change: Change) -> u64 {
		let offset = self.offset.fetch_add(1, Ordering::SeqCst);

		// Record the change
		let mut changes = self.changes.write();
		changes.push_back(change);

		// O(1) removal from front using VecDeque instead of O(N) Vec::drain
		if changes.len() > self.max_changes {
			changes.pop_front();
		}

		debug!(offset, "recorded change");

		#[cfg(feature = "metrics")]
		{
			set_pending_changes(changes.len());
		}

		offset
	}

	/// Get all changes since a given offset.
	#[instrument(level = "debug", skip(self))]
	pub fn get_changes_since(&self, offset: u64) -> Vec<Change> {
		let changes = self.changes.read();
		let current_offset = self.offset.load(Ordering::SeqCst);

		if offset >= current_offset {
			return Vec::new();
		}

		// Calculate the starting index relative to the end of the deque
		// Since VecDeque may have dropped older changes, we can't use absolute offsets
		let count = (current_offset - offset) as usize;
		let skip_count = changes.len().saturating_sub(count);

		changes.iter().skip(skip_count).cloned().collect()
	}

	/// Apply remote changes to the local state.
	#[instrument(level = "debug", skip(self, changes))]
	pub fn apply_changes(&self, changes: Vec<Change>) -> Result<u64> {
		let mut current_offset = self.offset.load(Ordering::SeqCst);

		for change in changes {
			// In a real implementation, we would:
			// 1. Apply the change to local store
			// 2. Record it in our change log with the remote's offset
			current_offset += 1;

			// Update our offset to be at least as high as any applied change
			let change_seq = change.id.to_u128_le();
			self.offset.fetch_max(change_seq as u64, Ordering::SeqCst);
		}

		Ok(current_offset)
	}

	/// Update the state of a peer.
	pub fn update_peer_state(&self, peer_id: &iroh::EndpointId, state: SyncState) {
		let key = crate::common::base64_url_encode(peer_id.as_bytes());
		let mut states = self.peer_states.write();

		states.entry(key).or_insert_with(|| PeerSyncState::new(*peer_id)).state = state;
	}

	/// Get the state of all known peers.
	pub fn get_peer_states(&self) -> Vec<PeerSyncState> {
		let states = self.peer_states.read();
		states.values().cloned().collect()
	}

	/// Get a state snapshot for persistence.
	pub fn snapshot(&self) -> SyncSnapshot {
		let changes: Vec<Change> = self.changes.read().iter().cloned().collect();
		let offset = self.offset.load(Ordering::SeqCst);

		let peers: Vec<_> =
			self.peer_states.read().iter().map(|(k, v)| (k.clone(), v.last_offset)).collect();

		SyncSnapshot {
			offset,
			changes,
			peers,
		}
	}

	/// Restore from a snapshot.
	pub fn restore(&self, snapshot: SyncSnapshot) {
		let mut changes = self.changes.write();
		changes.clear();
		for change in snapshot.changes {
			changes.push_back(change);
		}
		drop(changes);

		self.offset.store(snapshot.offset, Ordering::SeqCst);

		// Restore peer states with explicit error logging
		let mut states = self.peer_states.write();
		let mut failed_peers = 0;
		for (key, offset) in snapshot.peers {
			match Self::decode_peer_id(&key) {
				Ok(peer_id) => {
					states.insert(
						key,
						PeerSyncState {
							peer_id,
							state: SyncState::Disconnected,
							last_offset: offset,
							last_sync: None,
						},
					);
				}
				Err(e) => {
					warn!(
						peer_key = %key,
						offset = offset,
						err = %e,
						"failed to restore peer state, skipping"
					);
					failed_peers += 1;
				}
			}
		}
		drop(states);

		if failed_peers > 0 {
			warn!(
				failed_count = failed_peers,
				restored_offset = snapshot.offset,
				"some peer states could not be restored from snapshot"
			);
		}
	}

	fn decode_peer_id(encoded: &str) -> Result<iroh::EndpointId> {
		let bytes = base64_url_decode(encoded)
			.map_err(|e| Error::Encoding(format!("base64 decode failed: {}", e)))?;

		// Create endpoint ID from bytes
		let key_bytes: [u8; 32] = bytes[..32].try_into().map_err(|_| {
			Error::Encoding(format!("invalid peer ID length: expected 32, got {}", bytes.len()))
		})?;

		iroh::EndpointId::from_bytes(&key_bytes)
			.map_err(|e| Error::Encoding(format!("failed to create endpoint ID: {}", e)))
	}
}

impl Default for SyncManager {
	fn default() -> Self {
		Self::new(10000)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::Bytes;

	/// Generate a valid EndpointId for testing.
	#[cfg(test)]
	fn generate_test_endpoint_id() -> iroh::EndpointId {
		iroh::SecretKey::generate().public()
	}

	#[test]
	fn test_change_tracking() {
		let manager = SyncManager::new(100);

		let change1 = Change::set("ns", "db", Bytes::from("key1"), Bytes::from("val1"));
		let change2 = Change::set("ns", "db", Bytes::from("key2"), Bytes::from("val2"));

		let offset1 = manager.record_change(change1);
		let offset2 = manager.record_change(change2);

		assert_eq!(offset1, 0);
		assert_eq!(offset2, 1);
		assert_eq!(manager.offset(), 2);

		// Get changes since offset 0
		let changes = manager.get_changes_since(0);
		assert_eq!(changes.len(), 2);

		// Get changes since offset 1
		let changes = manager.get_changes_since(1);
		assert_eq!(changes.len(), 1);

		// Get changes since offset 2 (no changes)
		let changes = manager.get_changes_since(2);
		assert!(changes.is_empty());
	}

	#[test]
	fn test_change_trimming() {
		let manager = SyncManager::new(3);

		for i in 0..5 {
			let change =
				Change::set("ns", "db", Bytes::from(format!("key{i}")), Bytes::from("val"));
			manager.record_change(change);
		}

		// Should only have the last 3 changes
		let changes = manager.get_changes_since(0);
		assert_eq!(changes.len(), 3);
	}

	#[test]
	fn test_restore_with_valid_snapshot() {
		let manager = SyncManager::new(100);

		// Add some changes
		for i in 0..5 {
			let change =
				Change::set("ns", "db", Bytes::from(format!("key{i}")), Bytes::from("val"));
			manager.record_change(change);
		}

		// Create snapshot
		let snapshot = manager.snapshot();
		assert_eq!(snapshot.offset, 5);

		// Clear and restore
		let manager2 = SyncManager::new(100);
		manager2.restore(snapshot.clone());

		// Verify restoration
		let changes = manager2.get_changes_since(0);
		assert_eq!(changes.len(), 5);
	}

	#[test]
	fn test_restore_with_empty_snapshot() {
		let manager = SyncManager::new(100);

		let snapshot = manager.snapshot();
		assert_eq!(snapshot.offset, 0);
		assert!(snapshot.changes.is_empty());
	}

	#[test]
	fn test_snapshot_and_restore_preserves_peer_state() {
		let manager = SyncManager::new(100);

		// Add some changes
		for i in 0..3 {
			let change =
				Change::set("ns", "db", Bytes::from(format!("key{i}")), Bytes::from("val"));
			manager.record_change(change);
		}

		// Get a peer state and encode its ID
		let peer_id = generate_test_endpoint_id();
		manager.update_peer_state(&peer_id, SyncState::Syncing);

		// Snapshot
		let snapshot = manager.snapshot();

		// Restore to new manager
		let manager2 = SyncManager::new(100);
		manager2.restore(snapshot);

		// Check peer state was restored
		let states = manager2.get_peer_states();
		assert_eq!(states.len(), 1);
		assert_eq!(states[0].peer_id, peer_id);
	}

	#[test]
	fn test_peer_state_encoding_roundtrip() {
		// Test that we can roundtrip peer IDs through base64
		let peer_id = generate_test_endpoint_id();

		// Encode like we do in update_peer_state
		let encoded = crate::common::base64_url_encode(peer_id.as_bytes());

		// Verify we can decode it back using the common module
		let decoded = crate::common::base64_url_decode(&encoded);
		assert!(decoded.is_ok());
		let decoded_bytes = decoded.unwrap();
		assert_eq!(decoded_bytes.as_slice(), peer_id.as_bytes());
	}

	#[test]
	fn test_invalid_base64_in_restore() {
		let manager = SyncManager::new(100);

		let snapshot = SyncSnapshot {
			offset: 5,
			changes: vec![],
			peers: vec![("not-valid-base64!!!".to_string(), 0)],
		};

		// Should log warning but not panic
		manager.restore(snapshot);

		// Manager should still be functional
		let change = Change::set("ns", "db", Bytes::from("key"), Bytes::from("val"));
		let _ = manager.record_change(change);
	}
}
