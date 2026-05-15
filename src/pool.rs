//! Connection pool for reusing QUIC connections between peers.
//!
//! This module provides connection pooling to avoid the overhead of
//! establishing new QUIC connections for each sync operation.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use iroh::{endpoint::Connection, Endpoint, EndpointAddr};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

#[cfg(feature = "metrics")]
use crate::telemetry::set_pool_size;

/// A pooled connection to a peer.
#[derive(Debug)]
pub struct PooledConnection {
    /// The actual connection
    connection: Connection,
    /// When the connection was established
    created_at: Instant,
    /// Last time the connection was used
    last_used: Instant,
    /// Number of times this connection has been used
    use_count: u64,
    /// Whether the connection is being used
    in_use: bool,
    /// Whether the connection has been marked as closed
    marked_closed: bool,
}

#[allow(dead_code)]
impl PooledConnection {
    /// Create a new pooled connection.
    pub fn new(connection: Connection) -> Self {
        let now = Instant::now();
        Self {
            connection,
            created_at: now,
            last_used: now,
            use_count: 0,
            in_use: false,
            marked_closed: false,
        }
    }

    /// Mark the connection as being used.
    pub fn mark_used(&mut self) {
        self.last_used = Instant::now();
        self.use_count += 1;
        self.in_use = true;
    }

    /// Mark the connection as released (returned to pool).
    pub fn mark_released(&mut self) {
        self.in_use = false;
    }

    /// Mark the connection as closed.
    pub fn mark_closed(&mut self) {
        self.marked_closed = true;
    }

    /// Check if the connection is marked as closed.
    pub fn is_closed(&self) -> bool {
        self.marked_closed
    }

    /// Get a reference to the underlying connection.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Get the age of the connection.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Check if connection is currently in use.
    pub fn is_in_use(&self) -> bool {
        self.in_use
    }
}

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    /// Maximum age of a connection before it's closed
    pub max_age: Duration,
    /// Maximum idle time before a connection is closed
    pub max_idle: Duration,
    /// Maximum uses per connection (0 = unlimited)
    pub max_uses: u64,
    /// Interval between health checks
    pub health_check_interval: Duration,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            max_age: Duration::from_secs(3600), // 1 hour
            max_idle: Duration::from_secs(300), // 5 minutes
            max_uses: 100,                      // 100 sync operations
            health_check_interval: Duration::from_secs(60),
        }
    }
}

impl ConnectionPoolConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of connections.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set the maximum age of a connection.
    pub fn with_max_age(mut self, age: Duration) -> Self {
        self.max_age = age;
        self
    }

    /// Set the maximum idle time.
    pub fn with_max_idle(mut self, idle: Duration) -> Self {
        self.max_idle = idle;
        self
    }

    /// Set the maximum uses per connection.
    pub fn with_max_uses(mut self, uses: u64) -> Self {
        self.max_uses = uses;
        self
    }
}

/// A connection pool for managing peer connections.
pub struct ConnectionPool {
    /// Pool configuration
    config: ConnectionPoolConfig,
    /// Active connections keyed by peer endpoint ID
    connections: RwLock<HashMap<String, PooledConnection>>,
    /// Channel for cleanup notifications
    cleanup_tx: mpsc::Sender<String>,
}

#[allow(dead_code)]
impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(config: ConnectionPoolConfig) -> Self {
        let (cleanup_tx, _cleanup_rx) = mpsc::channel(100);

        Self {
            config,
            connections: RwLock::new(HashMap::new()),
            cleanup_tx,
        }
    }

    /// Get or create a connection to a peer.
    ///
    /// FIRST checks the pool for an existing healthy connection.
    /// Only establishes a new connection if no healthy one exists.
    #[instrument(skip_all)]
    pub async fn get_connection(
        &self,
        endpoint: &Endpoint,
        addr: EndpointAddr,
        alpn: &[u8],
    ) -> anyhow::Result<(Connection, iroh::EndpointId)> {
        let peer_key = format!("{}", addr.id);

        // OPTIMISTIC CHECK: Look for existing healthy connection FIRST
        {
            let mut connections = self.connections.write();

            if let Some(pooled) = connections.get_mut(&peer_key) {
                // Check if connection is healthy and available
                if !pooled.marked_closed
                    && !pooled.is_in_use()
                    && self.is_connection_healthy(pooled)
                {
                    pooled.mark_used();
                    let conn = pooled.connection.clone();
                    let peer_id = conn.remote_id();

                    debug!(peer = %peer_key, "reusing pooled connection");
                    return Ok((conn, peer_id));
                }
            }
        }

        // CACHE MISS: No healthy pooled connection, create new one
        info!(peer = %peer_key, "establishing new connection");
        let conn = endpoint.connect(addr, alpn).await?;
        let peer_id = conn.remote_id();

        // Add new connection to pool
        {
            let mut connections = self.connections.write();

            // Evict oldest if at capacity
            if connections.len() >= self.config.max_connections {
                if let Some(oldest_key) = self.find_oldest_unused_connection(&connections) {
                    debug!(peer = %oldest_key, "evicting oldest unused connection");
                    connections.remove(&oldest_key);
                }
            }

            let mut pooled = PooledConnection::new(conn.clone());
            pooled.mark_used();
            connections.insert(peer_key, pooled);

            #[cfg(feature = "metrics")]
            {
                set_pool_size(connections.len());
            }
        }

        Ok((conn, peer_id))
    }

    /// Release a connection back to the pool.
    pub fn release(&self, peer_id: &iroh::EndpointId) {
        let key = peer_id.to_string();
        let mut connections = self.connections.write();

        if let Some(pooled) = connections.get_mut(&key) {
            pooled.mark_released();
            pooled.last_used = Instant::now();
            debug!(peer = %key, "connection released to pool");
        }
    }

    /// Remove a connection from the pool.
    pub fn remove(&self, peer_id: &iroh::EndpointId) {
        let key = peer_id.to_string();
        let mut connections = self.connections.write();
        connections.remove(&key);
        debug!(peer = %key, "connection removed from pool");

        #[cfg(feature = "metrics")]
        {
            set_pool_size(connections.len());
        }
    }

    /// Check if a connection is healthy.
    fn is_connection_healthy(&self, pooled: &PooledConnection) -> bool {
        // Don't return closed connections
        if pooled.marked_closed {
            return false;
        }

        // Check age
        if pooled.age() > self.config.max_age {
            return false;
        }

        // Check idle time
        if pooled.last_used.elapsed() > self.config.max_idle {
            return false;
        }

        // Check use count
        if self.config.max_uses > 0 && pooled.use_count >= self.config.max_uses {
            return false;
        }

        true
    }

    /// Find the oldest unused connection in the pool.
    fn find_oldest_unused_connection(
        &self,
        connections: &HashMap<String, PooledConnection>,
    ) -> Option<String> {
        connections
            .iter()
            .filter(|(_, pooled)| !pooled.is_in_use())
            .min_by_key(|(_, pooled)| pooled.created_at)
            .map(|(key, _)| key.clone())
    }

    /// Clean up stale connections.
    pub fn cleanup(&self) {
        let mut connections = self.connections.write();

        connections.retain(|peer_id, pooled| {
            // Keep if healthy
            if self.is_connection_healthy(pooled) && !pooled.is_in_use() {
                return true;
            }

            // Mark as closed and remove
            pooled.mark_closed();
            warn!(peer = %peer_id, "removing stale connection from pool");
            false
        });

        #[cfg(feature = "metrics")]
        {
            set_pool_size(connections.len());
        }
    }

    /// Get the number of active connections.
    pub fn len(&self) -> usize {
        self.connections.read().len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.connections.read().is_empty()
    }

    /// Get connection stats.
    pub fn stats(&self) -> ConnectionPoolStats {
        let connections = self.connections.read();

        let total_uses: u64 = connections.values().map(|p| p.use_count).sum();
        let oldest_age = connections
            .values()
            .map(|p| p.age())
            .max()
            .unwrap_or_default();

        let in_use_count = connections.values().filter(|p| p.is_in_use()).count();

        ConnectionPoolStats {
            active_connections: connections.len(),
            in_use_connections: in_use_count,
            max_connections: self.config.max_connections,
            total_uses,
            oldest_connection_age: oldest_age,
        }
    }
}

/// Statistics about the connection pool.
#[derive(Debug, Clone)]
pub struct ConnectionPoolStats {
    /// Number of active connections
    pub active_connections: usize,
    /// Number of connections currently in use
    pub in_use_connections: usize,
    /// Maximum connections allowed
    pub max_connections: usize,
    /// Total uses across all connections
    pub total_uses: u64,
    /// Age of the oldest connection
    pub oldest_connection_age: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = ConnectionPoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.max_uses, 100);
    }

    #[test]
    fn test_pooled_connection() {
        // PooledConnection requires a real connection, so we can't test this directly
        // without mocking or integration tests
    }

    #[test]
    fn test_pool_stats() {
        let stats = ConnectionPoolStats {
            active_connections: 5,
            in_use_connections: 2,
            max_connections: 10,
            total_uses: 100,
            oldest_connection_age: Duration::from_secs(60),
        };

        assert_eq!(stats.active_connections, 5);
        assert_eq!(stats.in_use_connections, 2);
        assert_eq!(stats.max_connections, 10);
    }
}
