# surrealdb-iroh

> **Note:** This crate is independently developed and is not affiliated with SurrealDB. It provides P2P replication for SurrealDB embedded databases but is maintained separately.

A P2P (peer-to-peer) replication layer for SurrealDB embedded databases using the [Iroh](https://iroh.computer/) networking library.

## What It Does

This crate wraps SurrealDB embedded mode and adds replication capabilities:

```
┌────────────────────────────────────────────────────────────┐
│                    Your Application                         │
│                                                            │
│   ┌───────────────────────┐      ┌────────────────────┐   │
│   │    SurrealDB          │      │   surrealdb-iroh   │   │
│   │    Embedded           │◀────▶│   Replication      │   │
│   │                       │      │                    │   │
│   │   - Your data         │      │   - Change batching│   │
│   │   - Queries           │      │   - P2P sync       │   │
│   └───────────────────────┘      │   - Peer discovery│   │
│                                    └─────────┬──────────┘   │
│                                              │              │
│                          ┌───────────────────┼────────────┐  │
│                          │                   │            │  │
│                          ▼                   ▼            │  │
│                   ┌─────────────┐     ┌─────────────┐     │  │
│                   │   Peer A    │◀───▶│   Peer B    │     │  │
│                   │  (your app) │     │ (other app) │     │  │
│                   └─────────────┘     └─────────────┘     │  │
└────────────────────────────────────────────────────────────┘
```

## Integration

```rust
use surrealdb_iroh::{ReplicatorRunner, StorageHook, Change};
use bytes::Bytes;

// 1. Create replicator
let runner = ReplicatorRunner::new(ReplicatorConfig::default()).await?;

// 2. Register hook to receive changes from peers
runner.register_hook(MyStorageHook::new());

// 3. Start replicator
let _handle = runner.start().await?;

// 4. When SurrealDB commits a change, record it
runner.record_change(Change::set("ns", "db", Bytes::from("key"), Bytes::from("val")));

// 5. Share your address with peers
let ticket = runner.generate_ticket().await?;
```

## Features

- **P2P Replication**: Sync changes between SurrealDB embedded instances
- **NAT Traversal**: Iroh handles firewall/NAT traversal automatically
- **Change Batching**: Reduce network overhead with configurable batch size/age
- **Peer Discovery**: DNS, mDNS support for finding peers
- **Connection Pooling**: Reuse QUIC connections
- **Compression**: Optional zstd compression
- **Retry Logic**: Exponential backoff for failed connections
- **Peer Reputation**: Score peers based on sync success
- **Snapshots**: Full state sync for initial peer connection

## Configuration

```rust
use surrealdb_iroh::ReplicatorConfig;
use std::time::Duration;

let config = ReplicatorConfig::default()
    .with_node_id("my-peer")
    .with_sync_interval(Duration::from_secs(30))
    .with_max_peers(10)
    .with_dns(true)
    .with_mdns(true);
```

## Installation

```toml
[dependencies]
surrealdb-iroh = "0.1.0"
```

## Observability

```toml
surrealdb-iroh = { features = ["observability"] }
```

```rust
use surrealdb_iroh::telemetry::init_tracing;
init_tracing("http://localhost:4317", "my-app")?;
```

## Testing

```bash
cargo test --lib    # Unit tests
cargo test         # Integration tests
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request.

## Security

For security issues, please read [SECURITY.md](SECURITY.md).

## License

Business Source License 1.1 - see [LICENSE](LICENSE) for details.