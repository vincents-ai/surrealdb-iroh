//! Error types for iroh-sync replication.

use thiserror::Error;

/// Errors that can occur during Iroh replication operations.
#[derive(Error, Debug)]
pub enum Error {
	/// Failed to create or configure Iroh endpoint
	#[error("failed to create endpoint: {0}")]
	Endpoint(String),

	/// Discovery operation failed
	#[error("discovery failed: {0}")]
	Discovery(String),

	/// Failed to establish connection to peer
	#[error("connection failed: {0}")]
	Connection(String),

	/// Connection was closed or reset
	#[error("connection closed: {0}")]
	ConnectionClosed(String),

	/// Invalid ticket format
	#[error("invalid ticket: {0}")]
	InvalidTicket(String),

	/// Sync protocol error
	#[error("sync error: {0}")]
	Sync(String),

	/// Change encoding/decoding error
	#[error("encoding error: {0}")]
	Encoding(String),

	/// State machine error
	#[error("state error: {0}")]
	State(String),

	/// Shutdown was requested
	#[error("shutdown requested")]
	Shutdown,

	/// Channel send failed (receiver dropped)
	#[error("channel closed")]
	ChannelClosed,

	/// Peer not found or not connected
	#[error("peer not found: {0}")]
	PeerNotFound(String),

	/// Maximum peers reached
	#[error("maximum peers ({0}) reached")]
	MaxPeersReached(usize),

	/// Timeout during operation
	#[error("operation timed out after {0:?}")]
	Timeout(std::time::Duration),

	/// Any other error
	#[error("error: {0}")]
	Other(#[from] anyhow::Error),
}

/// Result type alias for Iroh replication operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
	fn from(err: std::io::Error) -> Self {
		Error::Connection(err.to_string())
	}
}

impl From<iroh::endpoint::InvalidSocketAddr> for Error {
	fn from(err: iroh::endpoint::InvalidSocketAddr) -> Self {
		Error::Endpoint(err.to_string())
	}
}

impl From<bincode::Error> for Error {
	fn from(err: bincode::Error) -> Self {
		Error::Encoding(err.to_string())
	}
}

impl From<serde_json::Error> for Error {
	fn from(err: serde_json::Error) -> Self {
		Error::Encoding(err.to_string())
	}
}

impl From<iroh::endpoint::ConnectionError> for Error {
	fn from(err: iroh::endpoint::ConnectionError) -> Self {
		Error::Connection(err.to_string())
	}
}
