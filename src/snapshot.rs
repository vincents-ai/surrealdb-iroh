#![allow(dead_code)]
//! Snapshot sync for full state transfer.

use crate::common::current_timestamp_secs;

use std::collections::HashMap;
use std::io;

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::change::Change;

/// Configuration for snapshot sync.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
	pub compress: bool,
	pub compression_level: i32,
	pub max_size: usize,
	pub chunk_size: usize,
}

impl Default for SnapshotConfig {
	fn default() -> Self {
		Self {
			compress: true,
			compression_level: 3,
			max_size: 100 * 1024 * 1024,
			chunk_size: 64 * 1024,
		}
	}
}

impl SnapshotConfig {
	pub fn new() -> Self {
		Self::default()
	}
	pub fn with_compress(mut self, compress: bool) -> Self {
		self.compress = compress;
		self
	}
	pub fn with_max_size(mut self, size: usize) -> Self {
		self.max_size = size;
		self
	}
	pub fn with_chunk_size(mut self, size: usize) -> Self {
		self.chunk_size = size;
		self
	}
}

/// A complete database snapshot for initial sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSnapshot {
	pub version: u32,
	pub timestamp_secs: u64,
	pub namespaces: Vec<NamespaceSnapshot>,
	pub checksum: u64,
}

impl DatabaseSnapshot {
	pub fn new(namespaces: Vec<NamespaceSnapshot>) -> Self {
		let checksum = Self::calculate_checksum(&namespaces);

		Self {
			version: 1,
			timestamp_secs: current_timestamp_secs(),
			namespaces,
			checksum,
		}
	}

	fn calculate_checksum(ns: &[NamespaceSnapshot]) -> u64 {
		use std::collections::hash_map::DefaultHasher;
		use std::hash::Hasher;

		let mut hasher = DefaultHasher::new();
		for n in ns {
			hasher.write(n.name.as_bytes());
			for db in &n.databases {
				hasher.write(db.name.as_bytes());
				hasher.write_usize(db.entries.len());
			}
		}
		hasher.finish()
	}

	pub fn total_entries(&self) -> usize {
		self.namespaces.iter().map(|ns| ns.total_entries()).sum()
	}

	pub fn validate(&self) -> bool {
		self.checksum == Self::calculate_checksum(&self.namespaces)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceSnapshot {
	pub name: String,
	pub databases: Vec<DbSnapshot>,
}

impl NamespaceSnapshot {
	pub fn total_entries(&self) -> usize {
		self.databases.iter().map(|db| db.total_entries()).sum()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSnapshot {
	pub name: String,
	pub entries: Vec<KeyValueEntry>,
	pub schemas: Vec<SchemaEntry>,
}

impl DbSnapshot {
	pub fn total_entries(&self) -> usize {
		self.entries.len()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueEntry {
	pub key: Option<Bytes>,
	pub value: Option<Bytes>,
	pub timestamp_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
	pub schema_type: String,
	pub definition: Bytes,
}

/// Snapshot serializer for converting changes to snapshots.
pub struct SnapshotSerializer {
	config: SnapshotConfig,
	pending_changes: RwLock<Vec<Change>>,
}

impl SnapshotSerializer {
	pub fn new(config: SnapshotConfig) -> Self {
		Self {
			config,
			pending_changes: RwLock::new(Vec::new()),
		}
	}

	pub fn add_change(&self, change: Change) {
		self.pending_changes.write().push(change);
	}

	pub fn pending_count(&self) -> usize {
		self.pending_changes.read().len()
	}

	pub fn build_snapshot(&self) -> DatabaseSnapshot {
		let changes = std::mem::take(&mut *self.pending_changes.write());
		let mut ns_map: HashMap<String, HashMap<String, Vec<KeyValueEntry>>> = HashMap::new();

		for change in changes {
			let db_map = ns_map.entry(change.namespace.clone()).or_default();
			let entries = db_map.entry(change.database.clone()).or_default();

			let entry = KeyValueEntry {
				key: change.key,
				value: change.value,
				timestamp_secs: current_timestamp_secs(),
			};
			entries.push(entry);
		}

		let namespaces: Vec<NamespaceSnapshot> = ns_map
			.into_iter()
			.map(|(name, databases)| {
				let dbs: Vec<DbSnapshot> = databases
					.into_iter()
					.map(|(db_name, entries)| DbSnapshot {
						name: db_name,
						entries,
						schemas: Vec::new(),
					})
					.collect();
				NamespaceSnapshot {
					name,
					databases: dbs,
				}
			})
			.collect();

		DatabaseSnapshot::new(namespaces)
	}

	pub fn serialize(&self, snapshot: &DatabaseSnapshot) -> io::Result<Bytes> {
		let encoded = bincode::serialize(snapshot)
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
		Ok(Bytes::from(encoded))
	}

	pub fn deserialize(&self, data: &[u8]) -> io::Result<DatabaseSnapshot> {
		bincode::deserialize(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
	}
}

/// A chunk of a snapshot for streaming.
#[derive(Debug, Clone)]
pub struct SnapshotChunk {
	pub sequence: u64,
	pub total: u64,
	pub data: Bytes,
	pub is_last: bool,
}

impl SnapshotChunk {
	pub fn new(sequence: u64, total: u64, data: Bytes, is_last: bool) -> Self {
		Self {
			sequence,
			total,
			data,
			is_last,
		}
	}
}

/// Snapshot exporter for streaming large snapshots.
pub struct SnapshotExporter {
	serializer: SnapshotSerializer,
	config: SnapshotConfig,
}

impl SnapshotExporter {
	pub fn new(config: SnapshotConfig) -> Self {
		Self {
			serializer: SnapshotSerializer::new(config.clone()),
			config,
		}
	}

	pub fn add_change(&self, change: Change) {
		self.serializer.add_change(change);
	}

	pub fn export(&self) -> io::Result<Vec<SnapshotChunk>> {
		let snapshot = self.serializer.build_snapshot();
		let data = self.serializer.serialize(&snapshot)?;

		let chunk_size = self.config.chunk_size;
		let total_chunks = data.len().div_ceil(chunk_size);

		let chunks: Vec<SnapshotChunk> = data
			.chunks(chunk_size)
			.enumerate()
			.map(|(i, chunk_data)| {
				SnapshotChunk::new(
					i as u64,
					total_chunks as u64,
					Bytes::copy_from_slice(chunk_data),
					i == total_chunks - 1,
				)
			})
			.collect();

		debug!(chunks = chunks.len(), "snapshot exported");
		Ok(chunks)
	}

	pub fn export_buffer(&self) -> io::Result<Bytes> {
		let snapshot = self.serializer.build_snapshot();
		self.serializer.serialize(&snapshot)
	}
}

/// Snapshot importer for receiving streamed snapshots.
pub struct SnapshotImporter {
	serializer: SnapshotSerializer,
	received_chunks: RwLock<Vec<Bytes>>,
	total_chunks: RwLock<Option<u64>>,
}

impl SnapshotImporter {
	pub fn new() -> Self {
		Self {
			serializer: SnapshotSerializer::new(SnapshotConfig::default()),
			received_chunks: RwLock::new(Vec::new()),
			total_chunks: RwLock::new(None),
		}
	}

	pub fn import_chunk(&self, chunk: SnapshotChunk) -> io::Result<()> {
		let mut total = self.total_chunks.write();
		match *total {
			Some(t) if chunk.sequence >= t => {
				return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid chunk"))
			}
			None => {
				*total = Some(chunk.total);
			}
			_ => {}
		}
		drop(total);

		let mut chunks = self.received_chunks.write();
		let idx = chunk.sequence as usize;
		if idx >= chunks.len() {
			chunks.resize(idx + 1, Bytes::new());
		}
		chunks[idx] = chunk.data;
		Ok(())
	}

	pub fn is_complete(&self) -> bool {
		let chunks = self.received_chunks.read();
		match *self.total_chunks.read() {
			Some(t) => chunks.iter().take(t as usize).all(|c| !c.is_empty()),
			None => false,
		}
	}

	pub fn finalize(&self) -> io::Result<DatabaseSnapshot> {
		if !self.is_complete() {
			return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete"));
		}

		let chunks = self.received_chunks.read();
		let data: Vec<u8> = chunks.iter().flat_map(|c| c.as_ref().iter().copied()).collect();
		self.serializer.deserialize(&data)
	}

	pub fn reset(&self) {
		*self.received_chunks.write() = Vec::new();
		*self.total_chunks.write() = None;
	}
}

impl Default for SnapshotImporter {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::Bytes;

	#[test]
	fn test_snapshot_config() {
		let config = SnapshotConfig::default();
		assert!(config.compress);
	}

	#[test]
	fn test_snapshot_serializer() {
		let serializer = SnapshotSerializer::new(SnapshotConfig::default());
		let change = Change::set("ns", "db", Bytes::from("key"), Bytes::from("value"));
		serializer.add_change(change);
		assert_eq!(serializer.pending_count(), 1);

		let snapshot = serializer.build_snapshot();
		assert_eq!(snapshot.namespaces.len(), 1);
		assert!(snapshot.validate());
	}

	#[test]
	fn test_snapshot_export_import() {
		let exporter = SnapshotExporter::new(SnapshotConfig::default());
		exporter.add_change(Change::set("ns", "db", Bytes::from("key"), Bytes::from("value")));

		let chunks = exporter.export().unwrap();
		assert!(!chunks.is_empty());

		let importer = SnapshotImporter::new();
		for chunk in chunks {
			importer.import_chunk(chunk).unwrap();
		}

		assert!(importer.is_complete());
		let snapshot = importer.finalize().unwrap();
		assert_eq!(snapshot.namespaces.len(), 1);
	}
}
