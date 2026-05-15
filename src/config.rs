//! Configuration for the Iroh Sync replicator.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for the Iroh replicator.
///
/// This struct configures how the P2P replication layer operates,
/// including discovery mechanisms, sync intervals, and network settings.
///
/// # Example
///
/// ```ignore
/// use surrealdb_iroh::ReplicatorConfig;
///
/// let config = ReplicatorConfig::default()
///     .with_max_peers(5)
///     .with_sync_interval(Duration::from_secs(60));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatorConfig {
    /// Enable DHT-based peer discovery (requires `dht` feature)
    #[serde(default)]
    pub enable_dht: bool,

    /// Enable DNS-based discovery (default: true)
    #[serde(default = "default_true")]
    pub enable_dns: bool,

    /// Enable local network discovery via mDNS (default: true)
    #[serde(default = "default_true")]
    pub enable_mdns: bool,

    /// Publish our address via Pkarr for DNS discovery (default: true)
    #[serde(default = "default_true")]
    pub enable_pkarr_publish: bool,

    /// Interval between sync attempts with connected peers
    #[serde(default = "default_sync_interval")]
    pub sync_interval: Duration,

    /// Maximum number of peers to connect to simultaneously
    #[serde(default = "default_max_peers")]
    pub max_peers: usize,

    /// Bind address for the Iroh endpoint
    #[serde(default)]
    pub bind_addr: Option<std::net::SocketAddr>,

    /// External address hint for NAT traversal
    #[serde(default)]
    pub external_addr: Option<std::net::SocketAddr>,

    /// Relay servers to use (defaults to Iroh public relays)
    #[serde(default)]
    pub relay_servers: Vec<String>,

    /// Enable metrics collection
    #[serde(default)]
    pub enable_metrics: bool,

    /// Secret key for this node (if None, generates a new one)
    /// Note: In iroh 0.98, secret keys are handled via presets
    #[serde(skip)]
    pub secret_key_bytes: Option<Vec<u8>>,
}

fn default_true() -> bool {
    true
}

fn default_sync_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_max_peers() -> usize {
    10
}

impl Default for ReplicatorConfig {
    fn default() -> Self {
        Self {
            enable_dht: false,
            enable_dns: true,
            enable_mdns: true,
            enable_pkarr_publish: true,
            sync_interval: default_sync_interval(),
            max_peers: default_max_peers(),
            bind_addr: None,
            external_addr: None,
            relay_servers: Vec::new(),
            enable_metrics: false,
            secret_key_bytes: None,
        }
    }
}

impl ReplicatorConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable DHT-based discovery.
    pub fn with_dht(mut self, enabled: bool) -> Self {
        self.enable_dht = enabled;
        self
    }

    /// Enable or disable DNS-based discovery.
    pub fn with_dns(mut self, enabled: bool) -> Self {
        self.enable_dns = enabled;
        self
    }

    /// Enable or disable mDNS local discovery.
    pub fn with_mdns(mut self, enabled: bool) -> Self {
        self.enable_mdns = enabled;
        self
    }

    /// Enable or disable Pkarr address publishing.
    pub fn with_pkarr(mut self, enabled: bool) -> Self {
        self.enable_pkarr_publish = enabled;
        self
    }

    /// Set the sync interval.
    pub fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    /// Set the maximum number of peers.
    pub fn with_max_peers(mut self, max: usize) -> Self {
        self.max_peers = max;
        self
    }

    /// Set the bind address.
    pub fn with_bind_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Set external address hint.
    pub fn with_external_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.external_addr = Some(addr);
        self
    }

    /// Add a relay server URL.
    pub fn with_relay_server(mut self, url: impl Into<String>) -> Self {
        self.relay_servers.push(url.into());
        self
    }
}
