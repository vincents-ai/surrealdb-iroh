//! OpenTelemetry observability integration for iroh sync.
//!
//! This module provides tracing and metrics support for monitoring
//! P2P sync operations, following the OTel Semantic Conventions.
//!
//! ## Semantic Conventions
//!
//! This module follows the OTel semantic conventions for:
//! - **RPC spans**: `rpc.system.name = "iroh"`, `rpc.method`
//! - **Messaging spans**: `messaging.system = "iroh-p2p"`, `messaging.operation.type`
//! - **Database spans**: `db.system.name = "surrealdb"`, `db.operation.name`
//!
//! Custom attributes for iroh-sync metrics (custom namespace)
//! that don't have standard semconv coverage.

use std::time::Duration;

#[cfg(feature = "opentelemetry")]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "metrics")]
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Semantic Convention Constants (OTel spec)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// RPC system name for Iroh P2P operations.
pub const ATTR_RPC_SYSTEM: &str = "rpc.system";
/// RPC method name (e.g., "sync", "connect", "ticket.resolve").
pub const ATTR_RPC_METHOD: &str = "rpc.method";
/// RPC response status code.
pub const ATTR_RPC_STATUS_CODE: &str = "rpc.response.status_code";

/// Server address (peer address).
pub const ATTR_SERVER_ADDRESS: &str = "server.address";
/// Server port.
pub const ATTR_SERVER_PORT: &str = "server.port";

/// Database system name.
pub const ATTR_DB_SYSTEM: &str = "db.system";
/// Database operation name.
pub const ATTR_DB_OPERATION: &str = "db.operation.name";
/// Database namespace.
pub const ATTR_DB_NAMESPACE: &str = "db.namespace";
/// Database name.
pub const ATTR_DB_NAME: &str = "db.name";
/// Database query text (sanitized).
pub const ATTR_DB_STATEMENT: &str = "db.statement";

/// Network transport (e.g., "udp", "quic").
pub const ATTR_NET_TRANSPORT: &str = "net.transport";
/// Network peer address.
pub const ATTR_NET_PEER_ADDRESS: &str = "net.peer.address";
/// Network peer port.
pub const ATTR_NET_PEER_PORT: &str = "net.peer.port";

/// Error type for failed operations.
pub const ATTR_ERROR_TYPE: &str = "error.type";
/// Error message.
pub const ATTR_ERROR_MESSAGE: &str = "error.message";

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Custom Attributes (iroh.sync.* namespace — no standard semconv)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Custom attribute namespace for SurrealDB Iroh P2P.
pub const ATTR_IROH_NAMESPACE: &str = "iroh.sync";

/// Whether a relay server was used for the connection.
pub const ATTR_IROH_RELAY_USED: &str = "iroh.sync.relay.used";
/// Number of currently connected peers.
pub const ATTR_IROH_PEER_COUNT: &str = "iroh.sync.peer.count";
/// Whether connection was direct (P2P) or via relay.
pub const ATTR_IROH_CONNECTION_TYPE: &str = "iroh.sync.connection.type";
/// Sync direction: "push", "pull", or "bidirectional".
pub const ATTR_IROH_SYNC_DIRECTION: &str = "iroh.sync.sync.direction";
/// Number of changes in a sync operation.
pub const ATTR_IROH_SYNC_CHANGES: &str = "iroh.sync.sync.changes";
/// Whether the sync was a full snapshot or incremental.
pub const ATTR_IROH_SYNC_TYPE: &str = "iroh.sync.sync.type";
/// Number of bytes transferred.
pub const ATTR_IROH_BYTES_TRANSFERRED: &str = "iroh.sync.bytes.transferred";
/// Connection ticket string (hash only, no secrets).
pub const ATTR_IROH_TICKET_HASH: &str = "iroh.sync.ticket.hash";

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Span Names (OTel RPC/Messaging semconv: `{operation} {resource}`)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Span for sync operations.
pub const SPAN_SYNC: &str = "sync";
/// Span for connection establishment.
pub const SPAN_CONNECT: &str = "connect";
/// Span for ticket resolution.
pub const SPAN_TICKET_RESOLVE: &str = "ticket.resolve";
/// Span for change batch sending.
pub const SPAN_SEND_BATCH: &str = "send.batch";
/// Span for change batch receiving.
pub const SPAN_RECEIVE_BATCH: &str = "receive.batch";

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Histogram Bucket Boundaries (from OTel spec patterns)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Bucket boundaries for sync duration histogram (seconds).
/// Covers: 10ms to ~80s for typical sync operations.
pub const SYNC_DURATION_BOUNDARIES: &[f64] =
	&[0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12, 10.24, 20.48, 40.96, 81.92];

/// Bucket boundaries for connection latency histogram (seconds).
/// Covers: 1ms to ~5s for connection establishment.
pub const CONNECTION_LATENCY_BOUNDARIES: &[f64] =
	&[0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

/// Bucket boundaries for bytes transferred histogram.
/// Covers: 1KB to 100MB.
pub const BYTES_TRANSFERRED_BOUNDARIES: &[f64] =
	&[1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Metric Names (OTel: use dot notation, lowercase)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Counter — total incoming connections.
pub const METRIC_CONNECTIONS_INCOMING: &str = "iroh.sync.connections.incoming";
/// Counter — total outgoing connections.
pub const METRIC_CONNECTIONS_OUTGOING: &str = "iroh.sync.connections.outgoing";
/// Counter — total connections by type.
pub const METRIC_CONNECTIONS_TOTAL: &str = "iroh.sync.connections.total";
/// Histogram — connection establishment latency.
pub const METRIC_CONNECTION_LATENCY: &str = "iroh.sync.connection.latency";

/// Counter — successful sync operations.
pub const METRIC_SYNC_SUCCESS: &str = "iroh.sync.sync.success";
/// Counter — failed sync operations.
pub const METRIC_SYNC_FAILURE: &str = "iroh.sync.sync.failure";
/// Counter — total sync operations.
pub const METRIC_SYNC_TOTAL: &str = "iroh.sync.sync.total";
/// Histogram — sync operation duration.
pub const METRIC_SYNC_DURATION: &str = "iroh.sync.sync.duration";
/// Counter — sync operations by direction.
pub const METRIC_SYNC_DIRECTION: &str = "iroh.sync.sync.direction";
/// Counter — sync type (full/incremental).
pub const METRIC_SYNC_TYPE: &str = "iroh.sync.sync.type";

/// Counter — bytes sent.
pub const METRIC_BYTES_SENT: &str = "iroh.sync.bytes.sent";
/// Counter — bytes received.
pub const METRIC_BYTES_RECEIVED: &str = "iroh.sync.bytes.received";
/// Counter — total bytes.
pub const METRIC_BYTES_TOTAL: &str = "iroh.sync.bytes.total";

/// Gauge — current active peers.
pub const METRIC_PEERS_ACTIVE: &str = "iroh.sync.peers.active";
/// Gauge — changes pending sync.
pub const METRIC_CHANGES_PENDING: &str = "iroh.sync.changes.pending";
/// Gauge — connection pool size.
pub const METRIC_POOL_SIZE: &str = "iroh.sync.pool.size";

/// Counter — relay connections used.
pub const METRIC_RELAY_USED: &str = "iroh.sync.relay.used";

/// Counter — ticket generations.
pub const METRIC_TICKETS_GENERATED: &str = "iroh.sync.tickets.generated";
/// Counter — ticket resolutions.
pub const METRIC_TICKETS_RESOLVED: &str = "iroh.sync.tickets.resolved";

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Initialization
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(feature = "opentelemetry")]
pub fn init_tracing(endpoint: &str, service_name: &str) -> anyhow::Result<()> {
	// Create OTLP exporter
	let exporter = opentelemetry_otlp::new_exporter().tonic().with_endpoint(endpoint);

	// Create trace provider with resource attributes
	let trace_provider = exporter
		.build::<opentelemetry_otlp::SpanExporter>()?
		.with_trace_config(opentelemetry_sdk::trace::Config::default().with_resource(
			opentelemetry_sdk::Resource::new(vec![
				opentelemetry_sdk::ResourceAttribute::new("service.name", service_name),
				opentelemetry_sdk::ResourceAttribute::new("rpc.system", "iroh"),
			]),
		))
		.start();

	// Create subscriber with tracing layer
	let subscriber = tracing_subscriber::registry()
		.with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
		.with(tracing_opentelemetry::layer().with_tracer(trace_provider.tracer(service_name)));

	subscriber.init();

	// Set global default tracer
	opentelemetry::global::set_tracer_provider(trace_provider);

	Ok(())
}

#[cfg(feature = "metrics")]
pub fn init_metrics(addr: &str) -> anyhow::Result<()> {
	// Describe counters
	describe_counter!(METRIC_CONNECTIONS_INCOMING, "Total incoming connections");
	describe_counter!(METRIC_CONNECTIONS_OUTGOING, "Total outgoing connections");
	describe_counter!(METRIC_SYNC_SUCCESS, "Successful sync operations");
	describe_counter!(METRIC_SYNC_FAILURE, "Failed sync operations");
	describe_counter!(METRIC_BYTES_SENT, "Bytes sent");
	describe_counter!(METRIC_BYTES_RECEIVED, "Bytes received");
	describe_counter!(METRIC_RELAY_USED, "Relay connections used");

	// Describe gauges
	describe_gauge!(METRIC_PEERS_ACTIVE, "Currently connected peers");
	describe_gauge!(METRIC_CHANGES_PENDING, "Changes pending sync");
	describe_gauge!(METRIC_POOL_SIZE, "Connection pool size");

	// Describe histograms with explicit boundaries
	describe_histogram!(
		METRIC_SYNC_DURATION,
		"Sync operation duration in seconds",
		&SYNC_DURATION_BOUNDARIES
	);
	describe_histogram!(
		METRIC_CONNECTION_LATENCY,
		"Connection establishment latency in seconds",
		&CONNECTION_LATENCY_BOUNDARIES
	);
	describe_histogram!(
		"iroh.sync.bytes.transferred",
		"Bytes transferred per operation",
		&BYTES_TRANSFERRED_BOUNDARIES
	);

	// Create Prometheus exporter
	let exporter = metrics_exporter_prometheus::PrometheusBuilder::new()
		.with_address(addr.parse()?)
		.build()?;

	metrics::global::set_metrics_exporter(exporter).map_err(|e| anyhow::anyhow!("{}", e))?;

	Ok(())
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Metric Recording Functions
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(feature = "metrics")]
pub fn record_connection_incoming() {
	counter!(METRIC_CONNECTIONS_INCOMING).increment(1);
	counter!(METRIC_CONNECTIONS_TOTAL).increment(1);
}

#[cfg(feature = "metrics")]
pub fn record_connection_outgoing() {
	counter!(METRIC_CONNECTIONS_OUTGOING).increment(1);
	counter!(METRIC_CONNECTIONS_TOTAL).increment(1);
}

#[cfg(feature = "metrics")]
pub fn record_sync_success(duration: Duration) {
	counter!(METRIC_SYNC_SUCCESS).increment(1);
	counter!(METRIC_SYNC_TOTAL).increment(1);
	histogram!(METRIC_SYNC_DURATION).record(duration.as_secs_f64());
}

#[cfg(feature = "metrics")]
pub fn record_sync_failure(duration: Duration) {
	counter!(METRIC_SYNC_FAILURE).increment(1);
	counter!(METRIC_SYNC_TOTAL).increment(1);
	histogram!(METRIC_SYNC_DURATION).record(duration.as_secs_f64());
}

#[cfg(feature = "metrics")]
pub fn record_bytes_sent(bytes: u64) {
	counter!(METRIC_BYTES_SENT).increment(bytes);
	counter!(METRIC_BYTES_TOTAL).increment(bytes);
}

#[cfg(feature = "metrics")]
pub fn record_bytes_received(bytes: u64) {
	counter!(METRIC_BYTES_RECEIVED).increment(bytes);
	counter!(METRIC_BYTES_TOTAL).increment(bytes);
}

#[cfg(feature = "metrics")]
pub fn record_relay_used() {
	counter!(METRIC_RELAY_USED).increment(1);
}

#[cfg(feature = "metrics")]
pub fn set_active_peers(count: usize) {
	gauge!(METRIC_PEERS_ACTIVE).set(count as f64);
}

#[cfg(feature = "metrics")]
pub fn set_pending_changes(count: usize) {
	gauge!(METRIC_CHANGES_PENDING).set(count as f64);
}

#[cfg(feature = "metrics")]
pub fn set_pool_size(size: usize) {
	gauge!(METRIC_POOL_SIZE).set(size as f64);
}

#[cfg(feature = "metrics")]
pub fn record_connection_latency(duration: Duration) {
	histogram!(METRIC_CONNECTION_LATENCY).record(duration.as_secs_f64());
}

#[cfg(feature = "metrics")]
pub fn record_ticket_generated() {
	counter!(METRIC_TICKETS_GENERATED).increment(1);
}

#[cfg(feature = "metrics")]
pub fn record_ticket_resolved() {
	counter!(METRIC_TICKETS_RESOLVED).increment(1);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// No-op implementations when features are disabled
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(not(feature = "opentelemetry"))]
pub fn init_tracing(_endpoint: &str, _service_name: &str) -> anyhow::Result<()> {
	anyhow::bail!("opentelemetry feature not enabled")
}

#[cfg(not(feature = "metrics"))]
pub fn init_metrics(_addr: &str) -> anyhow::Result<()> {
	anyhow::bail!("metrics feature not enabled")
}

#[cfg(not(feature = "opentelemetry"))]
pub fn shutdown() {}

#[cfg(not(feature = "metrics"))]
pub fn record_connection_incoming() {}

#[cfg(not(feature = "metrics"))]
pub fn record_connection_outgoing() {}

#[cfg(not(feature = "metrics"))]
pub fn record_sync_success(_duration: Duration, _direction: &str, _sync_type: &str) {}

#[cfg(not(feature = "metrics"))]
pub fn record_sync_failure(_duration: Duration) {}

#[cfg(not(feature = "metrics"))]
pub fn record_bytes_sent(_bytes: u64) {}

#[cfg(not(feature = "metrics"))]
pub fn record_bytes_received(_bytes: u64) {}

#[cfg(not(feature = "metrics"))]
pub fn record_relay_used() {}

#[cfg(not(feature = "metrics"))]
pub fn set_active_peers(_count: usize) {}

#[cfg(not(feature = "metrics"))]
pub fn set_pending_changes(_count: usize) {}

#[cfg(not(feature = "metrics"))]
pub fn set_pool_size(_size: usize) {}

#[cfg(not(feature = "metrics"))]
pub fn record_connection_latency(_duration: Duration) {}

#[cfg(not(feature = "metrics"))]
pub fn record_ticket_generated() {}

#[cfg(not(feature = "metrics"))]
pub fn record_ticket_resolved() {}

#[cfg(feature = "opentelemetry")]
pub fn shutdown() {
	opentelemetry::global::shutdown_tracer_provider();
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Helper Functions
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Build a span name following OTel semconv: `{operation} {resource}`
///
/// # Examples
/// ```
/// assert_eq!(span_name("sync", "peer123"), "sync peer123");
/// assert_eq!(span_name("connect", "relay"), "connect relay");
/// ```
#[inline]
pub fn span_name(operation: &str, resource: &str) -> String {
	format!("{operation} {resource}")
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Well-known values
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Well-known `iroh.sync.connection.type` values.
pub mod connection_type {
	/// Direct peer-to-peer connection (no relay).
	pub const DIRECT: &str = "direct";
	/// Connection via relay server.
	pub const RELAY: &str = "relay";
	/// Hole-punched connection (initially relay, then direct).
	pub const HOLE_PUNCHED: &str = "hole_punched";
}

/// Well-known `iroh.sync.sync.direction` values.
pub mod sync_direction {
	/// Push changes to peer.
	pub const PUSH: &str = "push";
	/// Pull changes from peer.
	pub const PULL: &str = "pull";
	/// Bidirectional sync (push and pull).
	pub const BIDIRECTIONAL: &str = "bidirectional";
}

/// Well-known `iroh.sync.sync.type` values.
pub mod sync_type {
	/// Full state snapshot sync.
	pub const SNAPSHOT: &str = "snapshot";
	/// Incremental change sync.
	pub const INCREMENTAL: &str = "incremental";
}

/// Normalize connection type to canonical value.
///
/// Maps common aliases to spec well-known values. Unknown values pass through.
///
/// # Examples
/// ```
/// assert_eq!(normalize_connection_type("p2p"), "direct");
/// assert_eq!(normalize_connection_type("relay"), "relay");
/// ```
#[inline]
pub fn normalize_connection_type(value: &str) -> String {
	match value.to_lowercase().as_str() {
		"p2p" | "peer" | "direct" => connection_type::DIRECT.to_string(),
		"relay" | "server" => connection_type::RELAY.to_string(),
		"holepunch" | "hole-punch" | "hole_punched" => connection_type::HOLE_PUNCHED.to_string(),
		_ => value.to_string(),
	}
}

/// Normalize sync direction to canonical value.
///
/// # Examples
/// ```
/// assert_eq!(normalize_sync_direction("send"), "push");
/// assert_eq!(normalize_sync_direction("receive"), "pull");
/// ```
#[inline]
pub fn normalize_sync_direction(value: &str) -> String {
	match value.to_lowercase().as_str() {
		"push" | "send" | "upload" => sync_direction::PUSH.to_string(),
		"pull" | "receive" | "download" => sync_direction::PULL.to_string(),
		"bidirectional" | "bidir" | "both" | "sync" => sync_direction::BIDIRECTIONAL.to_string(),
		_ => value.to_string(),
	}
}

/// Normalize sync type to canonical value.
#[inline]
pub fn normalize_sync_type(value: &str) -> String {
	match value.to_lowercase().as_str() {
		"full" | "snapshot" | "initial" => sync_type::SNAPSHOT.to_string(),
		"delta" | "incremental" | "diff" => sync_type::INCREMENTAL.to_string(),
		_ => value.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn span_name_format() {
		assert_eq!(span_name("sync", "peer1"), "sync peer1");
		assert_eq!(span_name("connect", ""), "connect ");
	}

	#[test]
	fn normalize_connection_type_spec_values() {
		assert_eq!(normalize_connection_type("direct"), "direct");
		assert_eq!(normalize_connection_type("relay"), "relay");
		assert_eq!(normalize_connection_type("hole_punched"), "hole_punched");
	}

	#[test]
	fn normalize_connection_type_aliases() {
		assert_eq!(normalize_connection_type("p2p"), "direct");
		assert_eq!(normalize_connection_type("peer"), "direct");
		assert_eq!(normalize_connection_type("holepunch"), "hole_punched");
		assert_eq!(normalize_connection_type("hole-punch"), "hole_punched");
	}

	#[test]
	fn normalize_sync_direction_spec_values() {
		assert_eq!(normalize_sync_direction("push"), "push");
		assert_eq!(normalize_sync_direction("pull"), "pull");
		assert_eq!(normalize_sync_direction("bidirectional"), "bidirectional");
	}

	#[test]
	fn normalize_sync_direction_aliases() {
		assert_eq!(normalize_sync_direction("send"), "push");
		assert_eq!(normalize_sync_direction("receive"), "pull");
		assert_eq!(normalize_sync_direction("bidir"), "bidirectional");
	}

	#[test]
	fn normalize_sync_type_spec_values() {
		assert_eq!(normalize_sync_type("snapshot"), "snapshot");
		assert_eq!(normalize_sync_type("incremental"), "incremental");
	}

	#[test]
	fn normalize_sync_type_aliases() {
		assert_eq!(normalize_sync_type("full"), "snapshot");
		assert_eq!(normalize_sync_type("delta"), "incremental");
		assert_eq!(normalize_sync_type("diff"), "incremental");
	}

	#[test]
	fn metric_names_follow_otel_conventions() {
		assert!(METRIC_SYNC_TOTAL.contains('.'));
		assert!(METRIC_PEERS_ACTIVE.contains('.'));
	}

	#[test]
	fn attribute_keys_match_otel_semconv() {
		assert_eq!(ATTR_RPC_SYSTEM, "rpc.system");
		assert_eq!(ATTR_RPC_METHOD, "rpc.method");
		assert_eq!(ATTR_SERVER_ADDRESS, "server.address");
		assert_eq!(ATTR_ERROR_TYPE, "error.type");
	}
}
