//! Peer discovery for Iroh Sync replication.
//!
//! This module provides multiple discovery mechanisms:
//! - DNS/Pkarr for internet-wide discovery
//! - mDNS for local network discovery
//! - Manual connection via tickets
//!
//! The DiscoveryManager integrates with Iroh's endpoint to provide
//! peer discovery and ticket-based connection sharing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::{Endpoint, EndpointAddr, EndpointId};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument};

use crate::common::{base64_url_decode, base64_url_encode};
use crate::config::ReplicatorConfig;
use crate::error::Error;
type Result<T> = std::result::Result<T, Error>;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Types
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Represents a discovered peer.
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    /// The peer's endpoint ID (public key)
    pub endpoint_id: EndpointId,
    /// Known addresses for the peer
    pub addresses: Vec<EndpointAddr>,
    /// How this peer was discovered
    pub discovery_method: DiscoveryMethod,
    /// When this peer was discovered
    pub discovered_at: Instant,
    /// Peer's display name (optional)
    pub name: Option<String>,
    /// Last successful connection time
    pub last_connected: Option<Instant>,
}

impl DiscoveredPeer {
    /// Check if this peer is still valid (not too old).
    pub fn is_valid(&self, max_age: Duration) -> bool {
        self.discovered_at.elapsed() < max_age
    }

    /// Mark this peer as successfully connected.
    pub fn mark_connected(&mut self) {
        self.last_connected = Some(Instant::now());
    }
}

/// How a peer was discovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiscoveryMethod {
    /// Discovered via DNS/Pkarr
    Dns,
    /// Discovered via local mDNS
    Mdns,
    /// Manually connected via ticket
    Manual,
    /// Discovered via relay server
    Relay,
}

impl DiscoveryMethod {
    /// Get a human-readable name for this method (lowercase).
    pub fn as_str(&self) -> &'static str {
        match self {
            DiscoveryMethod::Dns => "dns",
            DiscoveryMethod::Mdns => "mdns",
            DiscoveryMethod::Manual => "manual",
            DiscoveryMethod::Relay => "relay",
        }
    }

    /// Get an uppercase name for this method.
    pub fn as_upper(&self) -> &'static str {
        match self {
            DiscoveryMethod::Dns => "DNS",
            DiscoveryMethod::Mdns => "mDNS",
            DiscoveryMethod::Manual => "Manual",
            DiscoveryMethod::Relay => "Relay",
        }
    }
}

impl std::fmt::Display for DiscoveryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_upper())
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Discovery Manager
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Manages peer discovery across multiple mechanisms.
#[derive(Clone)]
pub struct DiscoveryManager {
    /// Recently discovered peers
    discovered_peers: Arc<RwLock<Vec<DiscoveredPeer>>>,
    /// Channel for sending peer discovery events
    peer_tx: mpsc::Sender<DiscoveredPeer>,
    /// Configuration
    config: ReplicatorConfig,
    /// Our endpoint ID
    self_endpoint_id: Option<EndpointId>,
}

impl DiscoveryManager {
    /// Create a new discovery manager with the given configuration.
    #[instrument(skip_all)]
    pub async fn new(config: &ReplicatorConfig) -> Result<Self> {
        let (peer_tx, _peer_rx) = mpsc::channel(100);

        Ok(Self {
            discovered_peers: Arc::new(RwLock::new(Vec::new())),
            peer_tx,
            config: config.clone(),
            self_endpoint_id: None,
        })
    }

    /// Set our own endpoint ID (called after endpoint is created).
    pub fn set_self_endpoint_id(&mut self, id: EndpointId) {
        self.self_endpoint_id = Some(id);
    }

    /// Get a channel for receiving discovered peers.
    pub fn peer_receiver(&self) -> mpsc::Sender<DiscoveredPeer> {
        self.peer_tx.clone()
    }

    /// Record a discovered peer.
    pub fn record_peer(&self, peer: DiscoveredPeer) {
        debug!(
            endpoint_id = %peer.endpoint_id,
            method = %peer.discovery_method,
            "peer discovered"
        );

        let mut peers = self.discovered_peers.write();

        // Update existing or add new
        if let Some(existing) = peers.iter_mut().find(|p| p.endpoint_id == peer.endpoint_id) {
            *existing = peer;
        } else {
            peers.push(peer);
        }

        // Trim old entries
        peers.retain(|p| p.is_valid(Duration::from_secs(300)));
    }

    /// Get all known peers.
    pub fn known_peers(&self) -> Vec<DiscoveredPeer> {
        self.discovered_peers.read().clone()
    }

    /// Get valid peers (not expired).
    pub fn valid_peers(&self) -> Vec<DiscoveredPeer> {
        let peers = self.discovered_peers.read();
        peers
            .iter()
            .filter(|p| p.is_valid(Duration::from_secs(300)))
            .cloned()
            .collect()
    }

    /// Get the count of known peers.
    pub fn peer_count(&self) -> usize {
        self.discovered_peers.read().len()
    }

    /// Parse a ticket string into connection info.
    pub fn parse_ticket(ticket: &str) -> Result<(EndpointId, Vec<EndpointAddr>)> {
        // Tickets are base64-encoded JSON
        let bytes = base64_url_decode(ticket).map_err(|e| Error::InvalidTicket(e.to_string()))?;

        let ticket_data: TicketData =
            serde_json::from_slice(&bytes).map_err(|e| Error::InvalidTicket(e.to_string()))?;

        Ok((ticket_data.endpoint_id, ticket_data.addresses))
    }

    /// Generate a ticket string for sharing our address.
    #[instrument(skip_all, fields(endpoint_id = %endpoint_id, addr_count = addrs.len()))]
    pub async fn generate_ticket(
        &self,
        endpoint_id: EndpointId,
        addrs: Vec<EndpointAddr>,
    ) -> Result<String> {
        let ticket_data = TicketData {
            endpoint_id,
            addresses: addrs,
        };

        let bytes = serde_json::to_vec(&ticket_data).map_err(|e| Error::Encoding(e.to_string()))?;

        Ok(base64_url_encode(&bytes))
    }

    /// Connect to a peer using their endpoint ID.
    ///
    /// This method attempts to connect using Iroh's built-in resolution,
    /// which handles DNS, relay, and direct connections automatically.
    #[instrument(skip_all, fields(peer_id = %endpoint_id))]
    pub async fn connect_to_peer(
        &self,
        endpoint: &Endpoint,
        endpoint_id: EndpointId,
    ) -> Result<()> {
        // Check if we're trying to connect to ourselves
        if let Some(self_id) = self.self_endpoint_id {
            if self_id == endpoint_id {
                return Err(Error::Other(anyhow::anyhow!("cannot connect to self")));
            }
        }

        info!(endpoint_id = %endpoint_id, "initiating connection to peer");

        // Iroh's endpoint handles discovery automatically when connecting by EndpointId
        // This will attempt DNS resolution, relay connections, etc.
        let addr = EndpointAddr::from(endpoint_id);

        endpoint
            .connect(addr, crate::SYNC_ALPN)
            .await
            .map_err(|e| Error::Connection(format!("failed to connect to peer: {}", e)))?;

        // Record successful connection
        let mut peers = self.discovered_peers.write();
        if let Some(peer) = peers.iter_mut().find(|p| p.endpoint_id == endpoint_id) {
            peer.mark_connected();
        }

        Ok(())
    }

    /// Discover addresses for a peer by their endpoint ID.
    ///
    /// Note: Iroh handles most discovery automatically. This method
    /// provides a way to check if we can reach a peer.
    pub async fn discover(&self, _endpoint_id: EndpointId) -> Result<Vec<EndpointAddr>> {
        // In modern Iroh, discovery is handled automatically by the endpoint
        // when connecting. We return empty and let the connection attempt
        // handle actual discovery.
        debug!("discovery delegated to Iroh endpoint");
        Ok(Vec::new())
    }

    /// Parse and validate a connection ticket.
    pub fn validate_ticket(&self, ticket: &str) -> Result<DiscoveredPeer> {
        let (endpoint_id, addrs) = Self::parse_ticket(ticket)?;

        Ok(DiscoveredPeer {
            endpoint_id,
            addresses: addrs,
            discovery_method: DiscoveryMethod::Manual,
            discovered_at: Instant::now(),
            name: None,
            last_connected: None,
        })
    }

    /// Clear all discovered peers.
    pub fn clear_peers(&self) {
        self.discovered_peers.write().clear();
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Ticket Data
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Data encoded in a connection ticket.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TicketData {
    endpoint_id: EndpointId,
    addresses: Vec<EndpointAddr>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test Helpers
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
use iroh::SecretKey;

/// Generate a valid EndpointId for testing.
#[cfg(test)]
fn generate_test_endpoint_id() -> EndpointId {
    // Use SecretKey to generate a valid public key
    SecretKey::generate().public()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discovery_manager_creation() {
        let config = ReplicatorConfig::default();
        let discovery = DiscoveryManager::new(&config).await;
        assert!(discovery.is_ok());
    }

    #[tokio::test]
    async fn test_ticket_roundtrip() {
        // Test the base64 encoding/decoding works
        use crate::common::{base64_url_decode, base64_url_encode};

        // Create a simple JSON string
        let json = r#"{"endpoint_id":"test","addresses":[]}"#;
        let bytes = json.as_bytes();

        // Encode
        let encoded = base64_url_encode(bytes);
        println!("Encoded: {} chars", encoded.len());

        // Decode
        let decoded = base64_url_decode(&encoded).unwrap();
        let decoded_str = String::from_utf8(decoded).unwrap();
        println!("Decoded: {}", decoded_str);

        assert_eq!(json, decoded_str);
    }

    #[tokio::test]
    async fn test_record_peer() {
        let discovery = DiscoveryManager::new(&ReplicatorConfig::default())
            .await
            .unwrap();

        let peer = DiscoveredPeer {
            endpoint_id: generate_test_endpoint_id(),
            addresses: Vec::new(),
            discovery_method: DiscoveryMethod::Manual,
            discovered_at: Instant::now(),
            name: Some("test-peer".to_string()),
            last_connected: None,
        };

        discovery.record_peer(peer.clone());

        let peers = discovery.known_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].endpoint_id, peer.endpoint_id);
    }

    #[test]
    fn test_discovery_method_display() {
        assert_eq!(format!("{}", DiscoveryMethod::Dns), "DNS");
        assert_eq!(format!("{}", DiscoveryMethod::Mdns), "mDNS");
        assert_eq!(format!("{}", DiscoveryMethod::Manual), "Manual");
        assert_eq!(format!("{}", DiscoveryMethod::Relay), "Relay");
    }

    #[test]
    fn test_parse_invalid_ticket() {
        // Test parsing of invalid base64
        let result = DiscoveryManager::parse_ticket("invalid-base64!!!");
        assert!(result.is_err());

        // Test parsing of valid base64 but invalid JSON
        let result = DiscoveryManager::parse_ticket("bm90LWpzb24="); // "not-json" base64
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ticket_with_addresses() {
        // This tests the full ticket serialization/deserialization
        let discovery = std::sync::Arc::new(
            DiscoveryManager::new(&ReplicatorConfig::default())
                .await
                .unwrap(),
        );

        let endpoint_id = generate_test_endpoint_id();

        let ticket = discovery
            .generate_ticket(endpoint_id, vec![])
            .await
            .unwrap();

        // Verify we can parse it back
        let (decoded_id, _) = DiscoveryManager::parse_ticket(&ticket).unwrap();
        assert_eq!(decoded_id, endpoint_id);
    }
}
