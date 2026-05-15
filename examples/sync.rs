//! P2P Replication Example
//!
//! This example demonstrates how to use the surrealdb-iroh crate
//! for peer-to-peer database replication.
//!
//! Run two instances of this example and share connection tickets
//! to sync data between them.
//!
//! # Example 1: Simple Replicator Setup
//!
//! ```ignore
//! use surrealdb_iroh::{ReplicatorConfig, ReplicatorRunner};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create replicator configuration
//!     let config = ReplicatorConfig::default();
//!
//!     // Start the replicator
//!     let runner = ReplicatorRunner::new(config).await?;
//!     let handle = runner.start().await?;
//!
//!     // Get our endpoint ID
//!     let endpoint_id = runner.endpoint_id();
//!     println!("Our endpoint: {:?}", endpoint_id);
//!
//!     // Generate a connection ticket to share
//!     let ticket = runner.generate_ticket().await?;
//!     println!("Share this ticket with peers: {}", ticket);
//!
//!     // ... use the replicator ...
//!
//!     // Shutdown gracefully
//!     runner.shutdown().await?;
//!     handle.await?;
//!     Ok(())
//! }
//! ```

use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use surrealdb_iroh::{
    Change, ChangeNotifier, ReplicatorConfig, ReplicatorRunner, StorageHook, SubscriptionFilter,
};

/// Example 1: Basic peer-to-peer sync
pub async fn run_basic_example() -> Result<()> {
    info!("Starting basic replication example");

    // Create configuration with custom settings
    let config = ReplicatorConfig::default();

    // Start the replicator
    let runner = ReplicatorRunner::new(config).await?;
    let shutdown_handle = runner.start().await?;

    // Print connection info
    let endpoint_id = runner
        .endpoint_id()
        .ok_or_else(|| anyhow!("No endpoint ID"))?;
    let ticket = runner.generate_ticket().await?;

    info!("Node ID: {:?}", endpoint_id);
    info!("Connection ticket: {}", ticket);
    info!("Share the ticket above with another peer to sync");

    // Record some changes
    info!("Recording sample changes...");
    runner.record_change(Change::set(
        "my_namespace",
        "my_database",
        Bytes::from("key1"),
        Bytes::from("value1"),
    ));
    runner.record_change(Change::set(
        "my_namespace",
        "my_database",
        Bytes::from("key2"),
        Bytes::from("value2"),
    ));

    // Wait for sync to happen
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shutdown
    info!("Shutting down...");
    runner.shutdown().await?;
    let _ = shutdown_handle.await;

    Ok(())
}

/// Example 2: Connecting to a peer with a ticket
pub async fn run_connect_example(ticket: &str) -> Result<()> {
    info!("Starting as a peer connecting to: {}", ticket);

    let config = ReplicatorConfig::default();

    let runner = ReplicatorRunner::new(config).await?;
    let shutdown_handle = runner.start().await?;

    info!("Connecting to peer...");
    let _ = runner.connect_ticket(ticket).await;

    // Wait for sync
    tokio::time::sleep(Duration::from_secs(10)).await;

    info!("Done, shutting down...");
    runner.shutdown().await?;
    let _ = shutdown_handle.await;

    Ok(())
}

/// Example 3: Observing sync events using ChangeNotifier
pub async fn run_observer_example() -> Result<()> {
    info!("Starting observer example");

    let config = ReplicatorConfig::default();

    let runner = ReplicatorRunner::new(config).await?;
    let _shutdown_handle = runner.start().await?;

    // Create a notifier and subscribe
    let notifier = std::sync::Arc::new(ChangeNotifier::new(100));
    let mut subscription = notifier.subscribe(SubscriptionFilter::everything());

    info!("Listening for changes...");

    // Process events for 30 seconds
    let timeout = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = subscription.recv().await {
            info!("Change event: {:?}", event);
        }
    })
    .await;

    match timeout {
        Ok(()) => info!("Stream ended"),
        Err(_) => info!("Timeout reached, shutting down"),
    }

    runner.shutdown().await?;
    Ok(())
}

/// Example 4: Custom observer implementation using StorageHook
pub struct MyStorageHook {
    change_count: std::sync::atomic::AtomicUsize,
}

impl MyStorageHook {
    fn new() -> Self {
        Self {
            change_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl StorageHook for MyStorageHook {
    fn on_change(&self, change: surrealdb_iroh::Change) {
        info!(
            ns = %change.namespace,
            db = %change.database,
            op = ?change.operation,
            "📤 Local change recorded"
        );
        self.change_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn on_sync_request(&self, peer_id: &iroh::EndpointId) {
        info!("🔗 Sync requested by peer: {:?}", peer_id);
    }

    fn on_sync_complete(&self, peer_id: &iroh::EndpointId, changes_applied: usize) {
        info!(
            "✅ Sync complete with peer {:?}, {} changes applied",
            peer_id, changes_applied
        );
    }
}

pub async fn run_custom_observer_example() -> Result<()> {
    info!("Starting custom observer example");

    let runner = ReplicatorRunner::new(ReplicatorConfig::default()).await?;
    let _shutdown_handle = runner.start().await?;

    // Register our custom storage hook
    let hook = MyStorageHook::new();
    runner.register_hook(hook);

    // Record changes and observe them
    runner.record_change(Change::set(
        "ns",
        "db",
        Bytes::from("test-key"),
        Bytes::from("test-value"),
    ));

    tokio::time::sleep(Duration::from_secs(5)).await;

    runner.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            "info,surrealdb_iroh=debug",
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse command line args
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("connect") => {
            let ticket = args
                .get(2)
                .ok_or_else(|| anyhow!("Usage: sync connect <ticket>"))?;
            run_connect_example(ticket).await?;
        }
        Some("observe") => {
            run_observer_example().await?;
        }
        Some("observer") => {
            run_custom_observer_example().await?;
        }
        _ => {
            println!("P2P Replication Example");
            println!("=========================");
            println!();
            println!("Usage:");
            println!("  sync                    - Start as a peer (generates ticket)");
            println!("  sync connect <ticket>   - Connect to a peer");
            println!("  sync observe            - Observe sync events");
            println!("  sync observer           - Use custom observer");
            println!();
            println!("Run two instances:");
            println!("  Terminal 1: ./sync");
            println!("  Terminal 2: ./sync connect <ticket from terminal 1>");
            println!();

            run_basic_example().await?;
        }
    }

    Ok(())
}
