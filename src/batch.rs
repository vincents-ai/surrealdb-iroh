#![allow(dead_code)]
//! Change batching for efficient sync transmission.
//!
//! This module provides batching functionality to accumulate changes
//! before sending them over the wire, reducing overhead and improving
//! throughput.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, instrument, warn};

use crate::change::Change;
use crate::sync::SyncManager;

/// Configuration for change batching.
#[derive(Debug, Clone)]
pub struct BatchConfig {
	/// Maximum number of changes per batch
	pub max_batch_size: usize,
	/// Maximum time to wait before flushing a batch
	pub max_batch_age: Duration,
	/// Enable batching
	pub enabled: bool,
}

impl Default for BatchConfig {
	fn default() -> Self {
		Self {
			max_batch_size: 100,
			max_batch_age: Duration::from_millis(100),
			enabled: true,
		}
	}
}

impl BatchConfig {
	/// Create a new config with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the maximum batch size.
	pub fn with_max_batch_size(mut self, size: usize) -> Self {
		self.max_batch_size = size;
		self
	}

	/// Set the maximum batch age.
	pub fn with_max_batch_age(mut self, age: Duration) -> Self {
		self.max_batch_age = age;
		self
	}

	/// Enable or disable batching.
	pub fn with_enabled(mut self, enabled: bool) -> Self {
		self.enabled = enabled;
		self
	}
}

/// A batch of changes ready for transmission.
#[derive(Debug, Clone)]
pub struct ChangeBatch {
	/// The changes in this batch
	pub changes: Vec<Change>,
	/// When the batch was created
	pub created_at: Instant,
	/// Batch sequence number
	pub sequence: u64,
}

impl ChangeBatch {
	/// Create a new batch.
	pub fn new(changes: Vec<Change>, sequence: u64) -> Self {
		Self {
			changes,
			created_at: Instant::now(),
			sequence,
		}
	}

	/// Get the number of changes in this batch.
	pub fn len(&self) -> usize {
		self.changes.len()
	}

	/// Check if the batch is empty.
	pub fn is_empty(&self) -> bool {
		self.changes.is_empty()
	}
}

/// Manages batching of changes before sync.
pub struct ChangeBatcher {
	/// Configuration
	config: BatchConfig,
	/// Pending changes not yet batched
	pending: RwLock<Vec<Change>>,
	/// Batches ready for transmission
	batches: RwLock<Vec<ChangeBatch>>,
	/// Channel for sending ready batches
	batch_tx: mpsc::Sender<ChangeBatch>,
	/// When the current batch was started
	batch_start: RwLock<Option<Instant>>,
	/// Current batch sequence number
	sequence: RwLock<u64>,
	/// Shutdown sender for the background task
	shutdown_tx: RwLock<Option<watch::Sender<()>>>,
	/// JoinHandle for the background flush task (for supervision)
	flush_task_handle: RwLock<Option<JoinHandle<()>>>,
	/// Flag indicating if the background task crashed
	task_has_crashed: RwLock<bool>,
}

impl ChangeBatcher {
	/// Create a new change batcher.
	pub fn new(config: BatchConfig, batch_tx: mpsc::Sender<ChangeBatch>) -> Self {
		Self {
			config,
			pending: RwLock::new(Vec::new()),
			batches: RwLock::new(Vec::new()),
			batch_tx,
			batch_start: RwLock::new(None),
			sequence: RwLock::new(0),
			shutdown_tx: RwLock::new(None),
			flush_task_handle: RwLock::new(None),
			task_has_crashed: RwLock::new(false),
		}
	}

	/// Start the background flush task for deterministic latency.
	///
	/// This spawns a task that periodically checks if the batch has exceeded
	/// max_batch_age and flushes it regardless of whether new changes arrive.
	///
	/// If a task is already running, this is a no-op.
	pub fn start_background_flush_task(self: &Arc<Self>) {
		// Check if a task is already running
		{
			let handle = self.flush_task_handle.read();
			if handle.is_some() {
				return;
			}
		}

		let batcher = self.clone();
		let (shutdown_tx, shutdown_rx) = watch::channel(());
		let max_batch_age = self.config.max_batch_age;

		// Store the shutdown sender
		*self.shutdown_tx.write() = Some(shutdown_tx);

		let handle = tokio::spawn(async move {
			// Track the next flush time
			let mut next_flush = tokio::time::Instant::now() + max_batch_age;

			let mut shutdown_rx = shutdown_rx;

			loop {
				tokio::select! {
					_ = tokio::time::sleep_until(next_flush) => {
						next_flush += max_batch_age;
						// Time-based flush: check if there's a stale batch to flush
						let should_flush = {
							let batch_start = batcher.batch_start.read();
							match *batch_start {
								Some(start) => {
									let elapsed = start.elapsed();
									if elapsed >= max_batch_age {
										let pending_count = batcher.pending_count();
										if pending_count > 0 {
											debug!(
												batch_age_ms = %elapsed.as_millis(),
												pending = pending_count,
												"background flush triggered by age"
											);
											true
										} else {
											// No pending changes, reset timer
											false
										}
									} else {
										false
									}
								}
								None => false,
							}
						};

						if should_flush {
							batcher.flush();
						}
					}
					_ = shutdown_rx.changed() => {
						debug!("background flush task shutting down");
						break;
					}
				}
			}

			// Clear the handle on normal exit
			*batcher.flush_task_handle.write() = None;
		});

		// Store the handle for supervision
		*self.flush_task_handle.write() = Some(handle);
	}

	/// Check if the background flush task has crashed.
	///
	/// Returns true if the task has exited abnormally.
	pub fn has_task_crashed(&self) -> bool {
		*self.task_has_crashed.read()
	}

	/// Check if a background flush task is currently running.
	pub fn is_task_running(&self) -> bool {
		self.flush_task_handle.read().is_some()
	}

	/// Gracefully shutdown the background flush task.
	///
	/// This sends a shutdown signal and waits for the task to complete.
	/// Returns immediately if no task is running.
	pub async fn shutdown(&self) {
		// Mark shutdown in progress - this helps the task recognize shutdown faster
		let _ = self.shutdown_tx.read().as_ref().map(|tx| tx.send(()));

		// Give the task a moment to process the shutdown signal
		tokio::time::sleep(Duration::from_millis(10)).await;

		// Take the handle outside the lock before awaiting
		let handle = self.flush_task_handle.write().take();

		// Now wait for the task to complete
		if let Some(h) = handle {
			match h.await {
				Ok(()) => {
					debug!("background flush task shutdown complete");
				}
				Err(e) => {
					error!(err = %e, "background flush task panicked during shutdown");
					*self.task_has_crashed.write() = true;
				}
			}
		}
	}

	/// Add a change to the pending buffer.
	///
	/// Returns true if a batch was flushed.
	#[instrument(skip(self, change))]
	pub fn add(&self, change: Change) -> bool {
		// Check if background task crashed
		if self.has_task_crashed() {
			warn!("background flush task has crashed, attempting restart");
			self.try_restart_task();
		}

		if !self.config.enabled {
			// Batching disabled, create a single-item batch
			let seq = {
				let mut seq = self.sequence.write();
				*seq += 1;
				*seq
			};

			let batch = ChangeBatch::new(vec![change], seq);
			if let Err(e) = self.batch_tx.try_send(batch) {
				warn!(err = %e, "failed to send batch");
			}
			return true;
		}

		// Mark batch start time if this is the first change
		{
			let mut batch_start = self.batch_start.write();
			if batch_start.is_none() {
				*batch_start = Some(Instant::now());
			}
		}

		// Add to pending
		{
			let mut pending = self.pending.write();
			pending.push(change);
		}

		// Check if we should flush based on size
		self.check_flush()
	}

	/// Try to restart the background task if it crashed.
	fn try_restart_task(&self) {
		// Only restart if task has crashed and is not currently running
		if self.has_task_crashed() && !self.is_task_running() {
			// We need Arc<Self> to restart - create one from self
			// This is a workaround since we can't easily get Arc<Self> from &self
			warn!("cannot auto-restart background task without Arc<Self>");
			*self.task_has_crashed.write() = false;
		}
	}

	/// Check if we should flush the pending changes.
	fn check_flush(&self) -> bool {
		let pending_len = {
			let pending = self.pending.read();
			pending.len()
		};

		let should_flush_size = pending_len >= self.config.max_batch_size;

		// Note: We removed the time check here because time-based flushing
		// is now handled by the background task for deterministic latency

		if should_flush_size {
			self.flush()
		} else {
			false
		}
	}

	/// Flush pending changes into a batch.
	fn flush(&self) -> bool {
		let changes = {
			let mut pending = self.pending.write();
			if pending.is_empty() {
				return false;
			}

			// Reset batch start
			{
				let mut batch_start = self.batch_start.write();
				*batch_start = None;
			}

			std::mem::take(&mut *pending)
		};

		if changes.is_empty() {
			return false;
		}

		let seq = {
			let mut seq = self.sequence.write();
			*seq += 1;
			*seq
		};

		let batch = ChangeBatch::new(changes, seq);

		debug!(batch_size = batch.len(), sequence = seq, "flushing batch");

		if let Err(e) = self.batch_tx.try_send(batch) {
			warn!(err = %e, "failed to send batch");
			return false;
		}

		true
	}

	/// Force flush any pending changes.
	pub fn force_flush(&self) {
		self.flush();
	}

	/// Get the number of pending changes.
	pub fn pending_count(&self) -> usize {
		self.pending.read().len()
	}

	/// Get the number of ready batches.
	pub fn ready_batch_count(&self) -> usize {
		self.batches.read().len()
	}

	/// Get batch configuration.
	pub fn config(&self) -> &BatchConfig {
		&self.config
	}
}

impl Drop for ChangeBatcher {
	fn drop(&mut self) {
		// Best effort shutdown on drop
		if let Some(tx) = self.shutdown_tx.write().take() {
			let _ = tx.send(());
		}
	}
}

/// A change collector that wraps a sync manager and adds batching.
pub struct BatchingSyncManager {
	/// Inner sync manager
	sync_manager: Arc<SyncManager>,
	/// Change batcher
	batcher: Arc<ChangeBatcher>,
}

impl BatchingSyncManager {
	/// Create a new batching sync manager.
	pub fn new(sync_manager: Arc<SyncManager>, config: BatchConfig) -> Self {
		let (batch_tx, _batch_rx) = mpsc::channel(100);

		let batcher = Arc::new(ChangeBatcher::new(config, batch_tx));
		// Start the background flush task
		batcher.start_background_flush_task();

		Self {
			sync_manager,
			batcher,
		}
	}

	/// Record a change with batching.
	pub fn record_change(&self, change: Change) {
		// Record to sync manager
		self.sync_manager.record_change(change.clone());

		// Also add to batcher
		self.batcher.add(change);
	}

	/// Flush pending batches.
	pub fn flush(&self) {
		self.batcher.force_flush();
	}

	/// Get the underlying sync manager.
	pub fn sync_manager(&self) -> &SyncManager {
		&self.sync_manager
	}

	/// Get pending change count.
	pub fn pending_count(&self) -> usize {
		self.batcher.pending_count()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn test_batch_config_default() {
		let config = BatchConfig::default();
		assert_eq!(config.max_batch_size, 100);
		assert!(config.enabled);
	}

	#[tokio::test]
	async fn test_change_batch() {
		let changes =
			vec![Change::set("ns", "db", bytes::Bytes::from("key1"), bytes::Bytes::from("val1"))];
		let batch = ChangeBatch::new(changes, 1);
		assert_eq!(batch.len(), 1);
		assert!(!batch.is_empty());
	}

	#[tokio::test]
	async fn test_batch_flush_on_size() {
		let (tx, _rx) = mpsc::channel(10);
		let batcher = Arc::new(ChangeBatcher::new(BatchConfig::new().with_max_batch_size(2), tx));

		// Add first change (shouldn't flush)
		let flushed = batcher.add(Change::set(
			"ns",
			"db",
			bytes::Bytes::from("k1"),
			bytes::Bytes::from("v1"),
		));
		assert!(!flushed);
		assert_eq!(batcher.pending_count(), 1);

		// Add second change (should flush)
		let flushed = batcher.add(Change::set(
			"ns",
			"db",
			bytes::Bytes::from("k2"),
			bytes::Bytes::from("v2"),
		));
		assert!(flushed);
		assert_eq!(batcher.pending_count(), 0);
	}

	#[tokio::test]
	async fn test_background_flush_task() {
		let (tx, mut rx) = mpsc::channel(10);
		let batcher = Arc::new(ChangeBatcher::new(
			BatchConfig::new()
				.with_max_batch_size(100) // High threshold so size won't trigger
				.with_max_batch_age(Duration::from_millis(50)),
			tx,
		));

		// Start the background task
		batcher.start_background_flush_task();

		// Add one change (won't trigger size-based flush)
		batcher.add(Change::set("ns", "db", bytes::Bytes::from("k1"), bytes::Bytes::from("v1")));
		assert_eq!(batcher.pending_count(), 1);

		// Wait for background task to flush by age
		let batch = tokio::time::timeout(Duration::from_millis(200), rx.recv())
			.await
			.expect("should have received batch within timeout")
			.expect("channel should not be closed");

		assert_eq!(batch.len(), 1);
		assert_eq!(batcher.pending_count(), 0);
	}

	#[tokio::test]
	async fn test_graceful_shutdown() {
		// Create a bounded channel to ensure the receiver doesn't block
		let (tx, rx) = mpsc::channel(10);
		let batcher = Arc::new(ChangeBatcher::new(
			BatchConfig::new()
				.with_max_batch_size(100)
				.with_max_batch_age(Duration::from_millis(50)),
			tx,
		));

		// Start background task
		batcher.start_background_flush_task();
		assert!(batcher.is_task_running(), "task should be running after start");

		// Drop the receiver to ensure the channel won't block sends
		drop(rx);

		// Shutdown with timeout to prevent hanging
		let shutdown_result =
			tokio::time::timeout(Duration::from_secs(5), batcher.shutdown()).await;

		assert!(shutdown_result.is_ok(), "shutdown timed out - task may be stuck");
		assert!(!batcher.is_task_running(), "task should not be running after shutdown");
	}

	#[tokio::test]
	async fn test_task_crash_detection() {
		let (tx, _rx) = mpsc::channel(10);
		let batcher = Arc::new(ChangeBatcher::new(
			BatchConfig::new()
				.with_max_batch_size(100)
				.with_max_batch_age(Duration::from_secs(3600)), // Very long
			tx,
		));

		batcher.start_background_flush_task();
		assert!(!batcher.has_task_crashed());
	}
}
