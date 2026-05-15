//! Protocol handler for Iroh Sync sync.
//!
//! This module implements the custom SurrealDB sync protocol over Iroh QUIC connections.

use std::sync::Arc;
use std::time::Instant;

use iroh::endpoint::Connection;
use iroh::Endpoint;
use tracing::{debug, error, info, instrument};

#[cfg(any(feature = "opentelemetry", feature = "metrics"))]
use crate::telemetry;

use crate::change::{decode_message, encode_message, SyncMessage};
use crate::config::ReplicatorConfig;
use crate::error::{Error, Result};
use crate::sync::SyncManager;

/// Protocol ALPN identifier
pub const ALPN: &[u8] = b"surrealdb/iroh-sync/1";

/// Maximum message size to prevent allocation attacks (16MB).
/// A malicious peer sending a larger length prefix would cause OOM.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Handler for the SurrealDB sync protocol.
///
/// This struct manages the sync protocol over a single QUIC connection.
/// It handles change synchronization by exchanging messages with the peer.
#[derive(Clone)]
#[allow(dead_code)]
pub struct SurrealDbProtocol {
    /// Iroh endpoint
    endpoint: Endpoint,
    /// Sync manager for tracking changes
    sync_manager: Arc<SyncManager>,
    /// Configuration
    config: ReplicatorConfig,
}

impl SurrealDbProtocol {
    /// Create a new protocol handler.
    pub fn new(
        endpoint: Endpoint,
        sync_manager: Arc<SyncManager>,
        config: ReplicatorConfig,
    ) -> Self {
        Self {
            endpoint,
            sync_manager,
            config,
        }
    }
}

impl std::fmt::Debug for SurrealDbProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurrealDbProtocol")
            .field("endpoint_id", &self.endpoint.secret_key().public())
            .finish()
    }
}

impl SurrealDbProtocol {
    /// Get our endpoint ID.
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint.secret_key().public()
    }

    /// Handle an incoming connection from a peer.
    ///
    /// This is called when a remote peer initiates a sync connection.
    /// We accept the bi-directional stream and process sync messages.
    #[instrument(skip_all, fields(peer_id = %conn.remote_id()))]
    pub async fn handle_connection(&self, conn: Connection) -> Result<()> {
        let peer_id = conn.remote_id();
        let start = Instant::now();

        info!(peer = %peer_id, "incoming sync connection");

        #[cfg(feature = "metrics")]
        {
            telemetry::record_connection_incoming();

            // Check if connection is via relay
            if let Some(selected) = conn.to_info().selected_path() {
                if selected.is_relay() {
                    telemetry::record_relay_used();
                }
            }
        }

        // Accept a bi-directional stream
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        // Update peer state
        self.sync_manager
            .update_peer_state(&peer_id, crate::sync::SyncState::Syncing);

        // Process messages in a loop
        let local_offset = self.sync_manager.offset();

        loop {
            // Read message length (4 bytes)
            let mut len_buf = [0u8; 4];
            match recv.read_exact(&mut len_buf).await {
                Ok(()) => {}
                Err(e) => {
                    // Check if connection closed gracefully
                    let closed = format!("{}", e);
                    if closed.contains("finished") || closed.contains("closed") {
                        debug!(peer = %peer_id, "connection closed by peer");
                    } else {
                        error!(peer = %peer_id, err = %e, "failed to read message length");
                    }
                    break;
                }
            }

            // Check for end-of-stream marker (zero length)
            let len = u32::from_be_bytes(len_buf);
            if len == 0 {
                debug!(peer = %peer_id, "end of stream marker received");
                break;
            }

            // SECURITY: Enforce maximum message size to prevent OOM attacks
            if len as usize > MAX_MESSAGE_SIZE {
                error!(
                    peer = %peer_id,
                    requested_size = len,
                    max_size = MAX_MESSAGE_SIZE,
                    "message too large, potential attack, dropping connection"
                );
                return Err(Error::Connection(
                    "message size exceeds maximum allowed".to_string(),
                ));
            }

            // Read message
            let mut msg = vec![0u8; len as usize];
            match recv.read_exact(&mut msg).await {
                Ok(()) => {}
                Err(e) => {
                    error!(peer = %peer_id, err = %e, "failed to read message");
                    break;
                }
            }

            #[cfg(feature = "metrics")]
            {
                telemetry::record_bytes_received(len as u64);
            }

            // Decode message
            let request: SyncMessage = match decode_message(&msg) {
                Ok(m) => m,
                Err(e) => {
                    error!(peer = %peer_id, err = %e, "failed to decode message");
                    break;
                }
            };

            debug!(peer = %peer_id, ?request, "received sync message");

            // Process message
            let response = self.process_message(&request, local_offset).await?;

            // Send response
            let encoded = encode_message(&response).map_err(|e| Error::Encoding(e.to_string()))?;

            let resp_len = (encoded.len() as u32).to_be_bytes();
            send.write_all(&resp_len)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            send.write_all(&encoded)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;

            #[cfg(feature = "metrics")]
            {
                telemetry::record_bytes_sent(resp_len.len() as u64 + encoded.len() as u64);
            }

            debug!(peer = %peer_id, "sent response");
        }

        // Signal end of data
        send.finish()
            .map_err(|e| Error::ConnectionClosed(format!("{}", e)))?;

        // Wait for connection to close
        conn.closed().await;

        self.sync_manager
            .update_peer_state(&peer_id, crate::sync::SyncState::Synchronized);

        let duration = start.elapsed();

        #[cfg(feature = "metrics")]
        {
            telemetry::record_sync_success(duration);
        }

        info!(peer = %peer_id, duration_ms = %duration.as_millis(), "sync connection closed");
        Ok(())
    }

    /// Process a sync message and generate a response.
    #[instrument(skip_all, fields(offset = %local_offset))]
    async fn process_message(&self, msg: &SyncMessage, local_offset: u64) -> Result<SyncMessage> {
        match msg {
            SyncMessage::Request { offset, limit } => {
                // Gather changes since the given offset
                let mut changes = self.sync_manager.get_changes_since(*offset);

                // Apply limit if specified
                if let Some(limit) = limit {
                    changes.truncate(*limit);
                }

                let new_offset = *offset + changes.len() as u64;

                // Check if this is final
                let is_final = changes.is_empty() || self.sync_manager.offset() <= new_offset;

                debug!(
                    offset = %offset,
                    count = changes.len(),
                    is_final = is_final,
                    "processing request"
                );

                Ok(SyncMessage::Response(crate::change::ChangeBatch {
                    offset: new_offset,
                    is_final,
                    changes,
                }))
            }

            SyncMessage::Response(batch) => {
                // Apply received changes
                for change in &batch.changes {
                    self.sync_manager.record_change(change.clone());
                }

                debug!(
                    count = batch.changes.len(),
                    new_offset = %batch.offset,
                    "applied batch"
                );

                Ok(SyncMessage::Ack {
                    offset: batch.offset,
                })
            }

            SyncMessage::Ack { .. } => {
                // Acknowledgment received
                Ok(SyncMessage::Ack {
                    offset: local_offset,
                })
            }

            SyncMessage::Ping => Ok(SyncMessage::Pong),

            SyncMessage::Pong => Ok(SyncMessage::Ping),
        }
    }

    /// Connect to a peer and perform synchronization.
    pub async fn connect_and_sync(&self, addr: iroh::EndpointAddr) -> Result<()> {
        let connect_start = Instant::now();

        info!("connecting to peer");

        #[cfg(feature = "metrics")]
        {
            telemetry::record_connection_outgoing();
        }

        // Connect to the peer
        let conn = self
            .endpoint
            .connect(addr.clone(), ALPN)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let peer_id = conn.remote_id();
        let connect_duration = connect_start.elapsed();

        #[cfg(feature = "metrics")]
        {
            telemetry::record_connection_latency(connect_duration);

            // Check if connection is via relay
            if let Some(selected) = conn.to_info().selected_path() {
                if selected.is_relay() {
                    telemetry::record_relay_used();
                }
            }
        }

        info!(peer = %peer_id, connect_ms = %connect_duration.as_millis(), "connected to peer");

        // Open a bidirectional stream
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        // Update peer state
        self.sync_manager
            .update_peer_state(&peer_id, crate::sync::SyncState::Syncing);

        // Start with our current offset
        let local_offset = self.sync_manager.offset();

        // Send initial request
        let request = SyncMessage::Request {
            offset: local_offset,
            limit: Some(1000),
        };

        let encoded = encode_message(&request).map_err(|e| Error::Encoding(e.to_string()))?;

        let req_len = (encoded.len() as u32).to_be_bytes();
        send.write_all(&req_len)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        send.write_all(&encoded)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        debug!(peer = %peer_id, offset = %local_offset, "sent initial request");

        // Receive responses in a loop
        loop {
            // Read message length
            let mut len_buf = [0u8; 4];
            match recv.read_exact(&mut len_buf).await {
                Ok(()) => {}
                Err(e) => {
                    debug!(peer = %peer_id, err = %e, "connection closed");
                    break;
                }
            }

            let len = u32::from_be_bytes(len_buf);
            if len == 0 {
                debug!(peer = %peer_id, "end of stream");
                break;
            }

            // SECURITY: Enforce maximum message size to prevent OOM attacks
            if len as usize > MAX_MESSAGE_SIZE {
                error!(
                    peer = %peer_id,
                    requested_size = len,
                    max_size = MAX_MESSAGE_SIZE,
                    "message too large, potential attack, dropping connection"
                );
                return Err(Error::Connection(
                    "message size exceeds maximum allowed".to_string(),
                ));
            }

            // Read message
            let mut msg = vec![0u8; len as usize];
            recv.read_exact(&mut msg)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;

            #[cfg(feature = "metrics")]
            {
                telemetry::record_bytes_received(len as u64);
            }

            // Decode message
            let response: SyncMessage =
                decode_message(&msg).map_err(|e| Error::Encoding(e.to_string()))?;

            match response {
                SyncMessage::Response(batch) => {
                    self.sync_manager.apply_changes(batch.changes.clone())?;
                    debug!(count = batch.changes.len(), "applied batch");
                }
                SyncMessage::Ack { .. } => {
                    debug!("sync complete");
                    break;
                }
                _ => {}
            }
        }

        // Send empty message to signal end
        send.write_all(&[0u8; 4])
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        send.finish()
            .map_err(|e| Error::ConnectionClosed(format!("{}", e)))?;

        // Explicitly close the connection to release resources
        conn.close(0u32.into(), b"done");

        self.sync_manager
            .update_peer_state(&peer_id, crate::sync::SyncState::Synchronized);

        #[cfg(feature = "metrics")]
        {
            telemetry::record_sync_success(connect_start.elapsed());
        }

        info!(peer = %peer_id, "sync completed");
        Ok(())
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_message_size_constant() {
        // Verify the constant is reasonable
        assert_eq!(MAX_MESSAGE_SIZE, 16 * 1024 * 1024);
        assert!(MAX_MESSAGE_SIZE < usize::MAX);
    }

    #[test]
    fn test_message_size_limit_enforcement_logic() {
        // Test that we correctly identify oversized messages
        let oversized = 17 * 1024 * 1024; // 17MB
        let within_limit = 16 * 1024 * 1024; // 16MB

        assert!(oversized > MAX_MESSAGE_SIZE);
        assert!(within_limit <= MAX_MESSAGE_SIZE);
    }

    #[test]
    fn test_end_of_stream_marker() {
        // Verify we use zero-length as end marker
        let end_marker = u32::from_be_bytes([0, 0, 0, 0]);
        assert_eq!(end_marker, 0);
    }

    #[test]
    fn test_alpn_value() {
        // Verify ALPN is correct
        assert_eq!(ALPN, b"surrealdb/iroh-sync/1");
    }
}
