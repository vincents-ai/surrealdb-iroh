//! Integration traits for database replication.
//!
//! This module provides the integration points for connecting
//! surrealdb-iroh with a SurrealDB embedded instance.

use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::change::Change;
use crate::replicator::Replicator;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Integration Traits
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Trait for integration with database storage backends.
///
/// Implement this trait to receive notifications when database changes occur
/// and when sync operations complete.
pub trait StorageHook: Send + Sync {
    /// Called when a change occurs in the database.
    fn on_change(&self, change: Change);

    /// Called when a sync is requested.
    fn on_sync_request(&self, peer_id: &iroh::EndpointId);

    /// Called when sync completes.
    fn on_sync_complete(&self, peer_id: &iroh::EndpointId, changes_applied: usize);
}

/// Trait for integration with database query engine.
pub trait QueryHook: Send + Sync {
    /// Called before a query is executed.
    fn on_query_start(&self, query: &str);

    /// Called after a query completes.
    fn on_query_complete(&self, query: &str, duration_ms: u64);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Replicator Runner
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Wrapper for running the replicator alongside a database.
///
/// This struct manages the lifecycle of the Iroh-based P2P replicator
/// and provides methods for change recording and peer connection.
#[derive(Clone)]
pub struct ReplicatorRunner {
    replicator: Arc<Replicator>,
    hooks: Arc<RwLock<Vec<Box<dyn StorageHook>>>>,
    change_tx: mpsc::Sender<Change>,
}

impl ReplicatorRunner {
    /// Create a new replicator runner.
    pub async fn new(config: crate::ReplicatorConfig) -> crate::Result<Self> {
        let replicator = Replicator::new(config).await?;
        let (change_tx, mut change_rx) = mpsc::channel(1000);

        let runner = Self {
            replicator: Arc::new(replicator),
            hooks: Arc::new(RwLock::new(Vec::new())),
            change_tx,
        };

        // Spawn task to forward changes to replicator and notify hooks
        let hooks = runner.hooks.clone();
        let replicator = runner.replicator.clone();
        tokio::spawn(async move {
            while let Some(change) = change_rx.recv().await {
                // Forward to replicator
                replicator.record_change(change.clone());

                // Notify all hooks
                let hooks = hooks.read();
                for hook in hooks.iter() {
                    hook.on_change(change.clone());
                }
            }
        });

        Ok(runner)
    }

    /// Start the replicator.
    pub async fn start(&self) -> crate::Result<tokio::task::JoinHandle<()>> {
        self.replicator.start().await
    }

    /// Record a local change for sync and hook notification.
    pub fn record_change(&self, change: Change) {
        // This will be forwarded to replicator via channel
        let _ = self.change_tx.try_send(change);
    }

    /// Connect to a peer using a connection ticket.
    pub async fn connect_ticket(&self, ticket: &str) -> crate::Result<()> {
        self.replicator.connect_ticket(ticket).await
    }

    /// Generate a connection ticket for peer sharing.
    pub async fn generate_ticket(&self) -> crate::Result<String> {
        self.replicator.generate_ticket().await
    }

    /// Get our endpoint ID.
    pub fn endpoint_id(&self) -> Option<iroh::EndpointId> {
        self.replicator.endpoint_id()
    }

    /// Shutdown the replicator.
    pub async fn shutdown(&self) -> crate::Result<()> {
        self.replicator.shutdown().await
    }

    /// Register a storage hook.
    pub fn register_hook<H: StorageHook + 'static>(&self, hook: H) {
        self.hooks.write().push(Box::new(hook));
    }

    /// Get the inner replicator for advanced operations.
    pub fn replicator(&self) -> &Arc<Replicator> {
        &self.replicator
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Sync Context
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Integration context for connecting replication to a database.
///
/// This provides a higher-level interface for recording changes
/// from within SurrealDB's transaction processing.
pub struct SyncContext {
    replicator: Arc<Replicator>,
}

impl SyncContext {
    /// Create a new sync context.
    pub fn new(replicator: Arc<Replicator>) -> Self {
        Self { replicator }
    }

    /// Record a database change for replication.
    ///
    /// Call this from within the database's transaction commit path
    /// to record changes for P2P sync.
    pub fn record_db_change(
        &self,
        namespace: impl Into<String>,
        database: impl Into<String>,
        key: Bytes,
        value: Option<Bytes>,
    ) {
        let change = if let Some(v) = value {
            Change::set(namespace, database, key, v)
        } else {
            Change::delete(namespace, database, key)
        };
        self.replicator.record_change(change);
    }

    /// Get the sync manager.
    pub fn sync_manager(&self) -> &crate::SyncManager {
        self.replicator.sync_manager()
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Default Implementations
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A no-op storage hook that does nothing.
pub struct NoOpStorageHook;

impl StorageHook for NoOpStorageHook {
    fn on_change(&self, _change: Change) {}
    fn on_sync_request(&self, _peer_id: &iroh::EndpointId) {}
    fn on_sync_complete(&self, _peer_id: &iroh::EndpointId, _changes_applied: usize) {}
}

/// A logging storage hook that logs all events.
pub struct LoggingStorageHook {
    namespace: String,
    database: String,
}

impl LoggingStorageHook {
    pub fn new(namespace: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            database: database.into(),
        }
    }
}

impl StorageHook for LoggingStorageHook {
    fn on_change(&self, change: Change) {
        tracing::info!(
            ns = %change.namespace,
            db = %change.database,
            key = ?change.key,
            op = ?change.operation,
            "change recorded for sync"
        );
    }

    fn on_sync_request(&self, peer_id: &iroh::EndpointId) {
        tracing::info!(peer = %peer_id, "sync requested");
    }

    fn on_sync_complete(&self, peer_id: &iroh::EndpointId, changes_applied: usize) {
        tracing::info!(
            peer = %peer_id,
            changes = changes_applied,
            "sync completed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_hook_trait() {
        struct TestHook;

        impl StorageHook for TestHook {
            fn on_change(&self, _change: Change) {}
            fn on_sync_request(&self, _peer_id: &iroh::EndpointId) {}
            fn on_sync_complete(&self, _peer_id: &iroh::EndpointId, _changes_applied: usize) {}
        }

        let _hook = TestHook;
    }

    #[tokio::test]
    async fn test_sync_context() {
        let config = crate::ReplicatorConfig::default();
        let _runner = ReplicatorRunner::new(config).await.unwrap();
        // Context creation would fail without running replicator
    }
}
