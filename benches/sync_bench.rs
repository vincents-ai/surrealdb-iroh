//! Benchmarks for surrealdb-iroh sync operations.
//!
//! Run with: cargo bench --package surrealdb-iroh

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use surrealdb_iroh::Change;

/// Benchmark change creation and recording.
fn bench_change_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("change_creation");

    for size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(BenchmarkId::new("set", size), |b| {
            b.iter(|| {
                for _ in 0..size {
                    let _change = Change::set(
                        "namespace",
                        "database",
                        Bytes::from(format!("key-{}", black_box(0))),
                        Bytes::from(format!("value-{}", black_box(0))),
                    );
                }
            });
        });
    }

    group.finish();
}

/// Benchmark change encoding.
fn bench_change_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("change_encoding");

    let change = Change::set(
        "namespace",
        "database",
        Bytes::from(vec![0u8; 1024]),
        Bytes::from(vec![1u8; 1024]),
    );

    group.bench_function("bincode_encode", |b| {
        b.iter(|| {
            let bytes = bincode::serialize(black_box(&change)).unwrap();
            black_box(bytes);
        });
    });

    group.bench_function("bincode_decode", |b| {
        let bytes = bincode::serialize(&change).unwrap();
        b.iter(|| {
            let decoded: Change = bincode::deserialize(black_box(&bytes)).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

/// Benchmark sync manager operations.
fn bench_sync_manager(c: &mut Criterion) {
    use surrealdb_iroh::SyncManager;

    let mut group = c.benchmark_group("sync_manager");

    group.bench_function("record_change", |b| {
        let manager = SyncManager::new(10000);

        b.iter(|| {
            let change = Change::set("ns", "db", Bytes::from("key"), Bytes::from("value"));
            let _offset = manager.record_change(change);
        });
    });

    group.bench_function("get_changes_since", |b| {
        let manager = SyncManager::new(10000);

        // Pre-populate with changes
        for i in 0..100 {
            let change = Change::set(
                "ns",
                "db",
                Bytes::from(format!("key-{}", i)),
                Bytes::from("value"),
            );
            manager.record_change(change);
        }

        b.iter(|| {
            let changes = manager.get_changes_since(black_box(50));
            black_box(changes.len());
        });
    });

    group.bench_function("offset", |b| {
        let manager = SyncManager::new(10000);
        b.iter(|| {
            let _offset = manager.offset();
        });
    });

    group.finish();
}

/// Benchmark compression operations (if feature enabled).
#[cfg(feature = "compression")]
fn bench_compression(c: &mut Criterion) {
    use surrealdb_iroh::{compress, CompressionConfig};

    let mut group = c.benchmark_group("compression");

    let data = vec![0u8; 1024 * 10]; // 10KB
    let config = CompressionConfig::default();

    group.bench_function("compress_10kb", |b| {
        b.iter(|| {
            let compressed = compress(black_box(&data), &config);
            black_box(compressed);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_change_creation,
    bench_change_encoding,
    bench_sync_manager,
);
criterion_main!(benches);
