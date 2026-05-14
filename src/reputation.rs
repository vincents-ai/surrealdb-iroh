//! Peer reputation and scoring system.
//!
//! This module provides a system for tracking peer reliability
//! and using reputation scores to prioritize connections.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Configuration for peer reputation.
#[derive(Debug, Clone)]
pub struct ReputationConfig {
	/// Initial reputation score (0-100)
	pub initial_score: f64,
	/// Score bonus for successful sync
	pub sync_success_bonus: f64,
	/// Score penalty for failed sync
	pub sync_failure_penalty: f64,
	/// Score bonus for staying connected
	pub connection_stability_bonus: f64,
	/// Score penalty for disconnect
	pub disconnection_penalty: f64,
	/// Decay rate per hour (percentage)
	pub decay_rate: f64,
	/// Minimum reputation score
	pub min_score: f64,
	/// Maximum reputation score
	pub max_score: f64,
}

impl Default for ReputationConfig {
	fn default() -> Self {
		Self {
			initial_score: 50.0,
			sync_success_bonus: 5.0,
			sync_failure_penalty: 10.0,
			connection_stability_bonus: 0.5,
			disconnection_penalty: 5.0,
			decay_rate: 1.0,
			min_score: 0.0,
			max_score: 100.0,
		}
	}
}

impl ReputationConfig {
	/// Create a new config with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the initial score.
	pub fn with_initial_score(mut self, score: f64) -> Self {
		self.initial_score = score;
		self
	}

	/// Set sync bonuses and penalties.
	pub fn with_sync_results(mut self, success_bonus: f64, failure_penalty: f64) -> Self {
		self.sync_success_bonus = success_bonus;
		self.sync_failure_penalty = failure_penalty;
		self
	}

	/// Set the decay rate.
	pub fn with_decay_rate(mut self, rate: f64) -> Self {
		self.decay_rate = rate;
		self
	}
}

/// Reputation score for a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReputation {
	/// Peer endpoint ID
	pub peer_id: iroh::EndpointId,
	/// Current reputation score
	pub score: f64,
	/// Total successful syncs
	pub sync_successes: u64,
	/// Total failed syncs
	pub sync_failures: u64,
	/// Total connection time in seconds
	pub connection_time_secs: u64,
	/// Number of disconnections
	pub disconnections: u64,
	/// When the peer was first seen (epoch seconds)
	first_seen_secs: u64,
	/// Last time the reputation was updated (epoch seconds)
	last_updated_secs: u64,
}

impl PeerReputation {
	/// Create a new reputation entry.
	pub fn new(peer_id: iroh::EndpointId, initial_score: f64) -> Self {
		let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
		Self {
			peer_id,
			score: initial_score,
			sync_successes: 0,
			sync_failures: 0,
			connection_time_secs: 0,
			disconnections: 0,
			first_seen_secs: now,
			last_updated_secs: now,
		}
	}

	/// Get the age of the reputation entry.
	pub fn age(&self) -> Duration {
		let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
		Duration::from_secs(now.saturating_sub(self.last_updated_secs))
	}

	/// Get the first seen time.
	pub fn first_seen(&self) -> SystemTime {
		UNIX_EPOCH + Duration::from_secs(self.first_seen_secs)
	}

	/// Get the last updated time.
	pub fn last_updated(&self) -> SystemTime {
		UNIX_EPOCH + Duration::from_secs(self.last_updated_secs)
	}

	/// Check if the peer is considered reliable.
	pub fn is_reliable(&self, threshold: f64) -> bool {
		self.score >= threshold
	}

	/// Get a summary of the reputation.
	pub fn summary(&self) -> ReputationSummary {
		ReputationSummary {
			peer_id: self.peer_id,
			score: self.score,
			sync_success_rate: if self.sync_successes + self.sync_failures > 0 {
				self.sync_successes as f64 / (self.sync_successes + self.sync_failures) as f64
			} else {
				0.0
			},
			avg_connection_secs: if self.disconnections > 0 {
				self.connection_time_secs as f64 / self.disconnections as f64
			} else {
				0.0
			},
		}
	}
}

/// Summary of peer reputation.
#[derive(Debug, Clone)]
pub struct ReputationSummary {
	/// Peer endpoint ID
	pub peer_id: iroh::EndpointId,
	/// Current reputation score
	pub score: f64,
	/// Ratio of successful syncs to total syncs
	pub sync_success_rate: f64,
	/// Average connection duration in seconds
	pub avg_connection_secs: f64,
}

/// Peer reputation manager.
pub struct ReputationManager {
	/// Configuration
	config: ReputationConfig,
	/// Peer reputations
	reputations: RwLock<HashMap<String, PeerReputation>>,
	/// Connection start times (epoch seconds)
	connection_starts: RwLock<HashMap<String, u64>>,
}

impl ReputationManager {
	/// Create a new reputation manager.
	pub fn new(config: ReputationConfig) -> Self {
		Self {
			config,
			reputations: RwLock::new(HashMap::new()),
			connection_starts: RwLock::new(HashMap::new()),
		}
	}

	/// Get current epoch seconds.
	fn now_secs() -> u64 {
		SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
	}

	/// Record a successful sync.
	pub fn record_sync_success(&self, peer_id: &iroh::EndpointId) {
		let key = peer_id.to_string();
		let mut reputations = self.reputations.write();

		if let Some(rep) = reputations.get_mut(&key) {
			rep.sync_successes += 1;
			rep.score = (rep.score + self.config.sync_success_bonus).min(self.config.max_score);
			rep.last_updated_secs = Self::now_secs();

			debug!(peer = %peer_id, score = rep.score, "sync success recorded");
		}
	}

	/// Record a failed sync.
	pub fn record_sync_failure(&self, peer_id: &iroh::EndpointId) {
		let key = peer_id.to_string();
		let mut reputations = self.reputations.write();

		if let Some(rep) = reputations.get_mut(&key) {
			rep.sync_failures += 1;
			rep.score = (rep.score - self.config.sync_failure_penalty).max(self.config.min_score);
			rep.last_updated_secs = Self::now_secs();

			debug!(peer = %peer_id, score = rep.score, "sync failure recorded");
		}
	}

	/// Record a connection established.
	pub fn record_connection(&self, peer_id: &iroh::EndpointId) {
		let key = peer_id.to_string();

		// Record connection start time
		{
			let mut starts = self.connection_starts.write();
			starts.insert(key.clone(), Self::now_secs());
		}

		// Update reputation
		let mut reputations = self.reputations.write();
		if let Some(rep) = reputations.get_mut(&key) {
			rep.last_updated_secs = Self::now_secs();
			info!(peer = %peer_id, score = rep.score, "connection established");
		}
	}

	/// Record a disconnection.
	pub fn record_disconnection(&self, peer_id: &iroh::EndpointId) {
		let key = peer_id.to_string();

		// Calculate connection duration
		let duration = {
			let mut starts = self.connection_starts.write();
			starts.remove(&key).map(|start| Self::now_secs().saturating_sub(start)).unwrap_or(0)
		};

		// Update reputation
		let mut reputations = self.reputations.write();
		if let Some(rep) = reputations.get_mut(&key) {
			rep.connection_time_secs += duration;
			rep.disconnections += 1;
			rep.score = (rep.score - self.config.disconnection_penalty).max(self.config.min_score);
			rep.last_updated_secs = Self::now_secs();

			debug!(
				peer = %peer_id,
				score = rep.score,
				duration_secs = duration,
				"disconnection recorded"
			);
		}
	}

	/// Get reputation for a peer.
	pub fn get_reputation(&self, peer_id: &iroh::EndpointId) -> Option<PeerReputation> {
		let key = peer_id.to_string();
		let reputations = self.reputations.read();
		reputations.get(&key).cloned()
	}

	/// Get all reputations sorted by score.
	pub fn get_all_reputations(&self) -> Vec<PeerReputation> {
		let reputations = self.reputations.read();
		let mut reps: Vec<PeerReputation> = reputations.values().cloned().collect();
		reps.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
		reps
	}

	/// Get the best peers to connect to.
	pub fn get_best_peers(&self, count: usize) -> Vec<PeerReputation> {
		self.get_all_reputations()
			.into_iter()
			.take(count)
			.filter(|rep| rep.is_reliable(30.0))
			.collect()
	}

	/// Apply time-based decay to all reputations.
	pub fn apply_decay(&self) {
		let mut reputations = self.reputations.write();
		let now_secs = Self::now_secs();

		for rep in reputations.values_mut() {
			let hours = (now_secs.saturating_sub(rep.last_updated_secs)) as f64 / 3600.0;
			let decay = hours * self.config.decay_rate;
			rep.score = (rep.score - decay).max(self.config.min_score);
			rep.last_updated_secs = now_secs;
		}

		debug!("applied reputation decay");
	}

	/// Remove a peer's reputation.
	pub fn remove(&self, peer_id: &iroh::EndpointId) {
		let key = peer_id.to_string();
		let mut reputations = self.reputations.write();
		let mut starts = self.connection_starts.write();

		reputations.remove(&key);
		starts.remove(&key);

		debug!(peer = %peer_id, "reputation removed");
	}

	/// Get statistics.
	pub fn stats(&self) -> ReputationStats {
		let reputations = self.reputations.read();
		let count = reputations.len();

		if count == 0 {
			return ReputationStats::default();
		}

		let sum: f64 = reputations.values().map(|r| r.score).sum();
		let avg = sum / count as f64;

		let mut max_score = f64::MIN;
		let mut min_score = f64::MAX;

		for rep in reputations.values() {
			max_score = max_score.max(rep.score);
			min_score = min_score.min(rep.score);
		}

		ReputationStats {
			peer_count: count,
			avg_score: avg,
			max_score,
			min_score,
		}
	}
}

/// Statistics about peer reputations.
#[derive(Debug, Default, Clone)]
pub struct ReputationStats {
	/// Number of peers tracked
	pub peer_count: usize,
	/// Average reputation score
	pub avg_score: f64,
	/// Maximum reputation score
	pub max_score: f64,
	/// Minimum reputation score
	pub min_score: f64,
}

/// A filter for selecting peers based on reputation.
#[derive(Debug, Default, Clone)]
pub struct ReputationFilter {
	/// Minimum score threshold
	min_score: Option<f64>,
	/// Maximum score threshold
	max_score: Option<f64>,
	/// Minimum successful syncs
	min_sync_successes: Option<u64>,
	/// Maximum failures
	max_failures: Option<u64>,
}

impl ReputationFilter {
	/// Create a new filter.
	pub fn new() -> Self {
		Self {
			min_score: None,
			max_score: None,
			min_sync_successes: None,
			max_failures: None,
		}
	}

	/// Set minimum score threshold.
	pub fn with_min_score(mut self, score: f64) -> Self {
		self.min_score = Some(score);
		self
	}

	/// Set maximum score threshold.
	pub fn with_max_score(mut self, score: f64) -> Self {
		self.max_score = Some(score);
		self
	}

	/// Set minimum successful syncs.
	pub fn with_min_sync_successes(mut self, count: u64) -> Self {
		self.min_sync_successes = Some(count);
		self
	}

	/// Set maximum failures.
	pub fn with_max_failures(mut self, count: u64) -> Self {
		self.max_failures = Some(count);
		self
	}

	/// Check if a reputation passes the filter.
	pub fn passes(&self, rep: &PeerReputation) -> bool {
		if let Some(min) = self.min_score {
			if rep.score < min {
				return false;
			}
		}

		if let Some(max) = self.max_score {
			if rep.score > max {
				return false;
			}
		}

		if let Some(min) = self.min_sync_successes {
			if rep.sync_successes < min {
				return false;
			}
		}

		if let Some(max) = self.max_failures {
			if rep.sync_failures > max {
				return false;
			}
		}

		true
	}

	/// Apply filter to a list of reputations.
	pub fn apply(&self, reputations: &[PeerReputation]) -> Vec<PeerReputation> {
		reputations.iter().filter(|r| self.passes(r)).cloned().collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Generate a valid EndpointId for testing.
	#[cfg(test)]
	fn generate_test_endpoint_id() -> iroh::EndpointId {
		iroh::SecretKey::generate().public()
	}

	#[test]
	fn test_reputation_config_default() {
		let config = ReputationConfig::default();
		assert_eq!(config.initial_score, 50.0);
		assert_eq!(config.sync_success_bonus, 5.0);
	}

	#[test]
	fn test_peer_reputation() {
		let peer_id = generate_test_endpoint_id();
		let rep = PeerReputation::new(peer_id, 50.0);

		assert_eq!(rep.score, 50.0);
		assert_eq!(rep.sync_successes, 0);
	}

	#[test]
	fn test_reputation_manager() {
		let manager = ReputationManager::new(ReputationConfig::default());
		let peer_id = generate_test_endpoint_id();

		// Create a reputation entry first by accessing it directly
		{
			let mut reputations = manager.reputations.write();
			reputations.insert(peer_id.to_string(), PeerReputation::new(peer_id, 50.0));
		}

		// Now record sync success
		manager.record_sync_success(&peer_id);

		let rep = manager.get_reputation(&peer_id).unwrap();
		assert_eq!(rep.sync_successes, 1);
		assert!(rep.score > 50.0);
	}

	#[test]
	fn test_reputation_filter() {
		let filter = ReputationFilter::new().with_min_score(30.0).with_max_failures(5);

		let peer_id = generate_test_endpoint_id();
		let rep = PeerReputation::new(peer_id, 50.0);

		assert!(filter.passes(&rep));
	}
}
