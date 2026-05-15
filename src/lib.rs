//! P2P Replication Layer using Iroh
//!
//! This crate provides P2P (peer-to-peer) replication for databases using the
//! [Iroh](https://iroh.computer/) networking library.
//!
//! > **Note:** This crate is independently developed and is not affiliated with
//! > SurrealDB. It is designed to be compatible with SurrealDB's replication
//! > protocol but is maintained separately.
//!
//! # Features
//!
//! - **P2P Connectivity**: Establish direct connections between database instances
//! - **NAT Traversal**: Automatically handles NAT and firewall traversal
//! - **Change Sync**: Efficient synchronization of database changes
//! - **Observable**: Built on tracing for observability
//! - **Compression**: Optional zstd compression for bandwidth reduction
//! - **Batching**: Change batching for reduced overhead
//!
//! # Quick Start
//!
//! ```ignore
//! use surrealdb_iroh::{ReplicatorConfig, ReplicatorRunner, Change};
//!
//! async fn example() -> anyhow::Result<()> {
//!     let config = ReplicatorConfig::default();
//!     let runner = ReplicatorRunner::new(config).await?;
//!     
//!     // Generate ticket to share with peers
//!     let ticket = runner.generate_ticket().await?;
//!     
//!     // Record changes for sync
//!     runner.record_change(Change::set("ns", "db", key, value));
//!     
//!     runner.shutdown().await?;
//!     Ok(())
//! }
//! ```

#![allow(missing_docs)]
#![allow(dead_code)]

mod batch;
mod change;
mod common;
mod compress;
mod config;
mod discovery;
mod error;
mod hook;
mod notify;
mod persist;
mod pool;
mod protocol;
mod replicator;
mod reputation;
mod retry;
mod snapshot;
mod sync;

#[cfg(any(feature = "opentelemetry", feature = "metrics"))]
mod telemetry;

pub use batch::{BatchConfig, BatchingSyncManager, ChangeBatch};
pub use change::Change;
pub use compress::{CompressedData, CompressionConfig, CompressionError};
pub use config::ReplicatorConfig;
pub use discovery::DiscoveryManager;
pub use error::{Error, Result};
pub use hook::{
    LoggingStorageHook, NoOpStorageHook, QueryHook, ReplicatorRunner, StorageHook, SyncContext,
};
pub use notify::{
    ChangeEvent, ChangeEventType, ChangeNotifier, ChangeObserver, Subscription, SubscriptionFilter,
};
pub use persist::{ChangeStore, ChangeStoreManager, FileChangeStore, MemoryChangeStore};
pub use pool::{ConnectionPool, ConnectionPoolConfig, ConnectionPoolStats};
pub use protocol::SurrealDbProtocol;
pub use replicator::Replicator;
pub use reputation::{
    PeerReputation, ReputationConfig, ReputationFilter, ReputationManager, ReputationStats,
    ReputationSummary,
};
pub use retry::{RetryBudget, RetryConfig, RetryError, RetryOperation, RetryPolicy};
pub use snapshot::{
    DatabaseSnapshot, SnapshotChunk, SnapshotConfig, SnapshotExporter, SnapshotImporter,
};
pub use sync::{SyncManager, SyncSnapshot, SyncState};

// OpenTelemetry and metrics (optional)
#[cfg(any(feature = "opentelemetry", feature = "metrics"))]
pub use telemetry;

// Re-export iroh types for advanced users
pub use iroh::EndpointId;

/// ALPN protocol identifier for replication sync
pub const SYNC_ALPN: &[u8] = b"surrealdb/iroh-sync/1";

/// Current protocol version
pub const PROTOCOL_VERSION: u32 = 1;
