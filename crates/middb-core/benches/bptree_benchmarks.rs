use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use middb_core::bptree::BPTree;
use middb_core::DiskBPTree;
use tempfile::TempDir;

type TestTree = BPTree<64, Vec<u8>, Vec<u8>>;

fn bench_bptree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bptree_insert");
    for count in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("memory", count), &count, |b, &count| {
            b.iter(|| {
                let mut tree = TestTree::new();
                for i in 0..count {
                    let key = format!("key_{i:08}").into_bytes();
                    tree.insert(key, format!("val_{i}").into_bytes());
                }
            });
        });
    }
    group.finish();
}

fn bench_bptree_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("bptree_lookup");
    for count in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("memory", count), &count, |b, &count| {
            let mut tree = TestTree::new();
            for i in 0..count {
                let key = format!("key_{i:08}").into_bytes();
                tree.insert(key, format!("val_{i}").into_bytes());
            }
            let mut idx = 0u64;
            b.iter(|| {
                let key = format!("key_{:08}", idx % count).into_bytes();
                let _ = tree.get(&key);
                idx += 1;
            });
        });
    }
    group.finish();
}

fn bench_bptree_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("bptree_range");
    for count in [1000, 10000] {
        group.bench_with_input(BenchmarkId::new("memory", count), &count, |b, &count| {
            let mut tree = TestTree::new();
            for i in 0..count {
                let key = format!("key_{i:08}").into_bytes();
                tree.insert(key, format!("val_{i}").into_bytes());
            }
            let start = b"key_00000100".to_vec();
            let end = b"key_00000200".to_vec();
            b.iter(|| {
                let _: Vec<_> = tree.range(&start, &end).collect();
            });
        });
    }
    group.finish();
}

fn bench_disk_bptree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_bptree_insert");
    group.sample_size(10);
    for count in [100, 1000] {
        group.bench_with_input(BenchmarkId::new("disk", count), &count, |b, &count| {
            b.iter(|| {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("bench.bpt");
                let mut tree = DiskBPTree::create(path.to_str().unwrap()).unwrap();
                for i in 0..count {
                    let key = format!("key_{i:08}").into_bytes();
                    tree.insert(key, format!("val_{i}").into_bytes()).unwrap();
                }
                tree.flush().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_disk_bptree_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_bptree_lookup");
    for count in [100, 1000] {
        group.bench_with_input(BenchmarkId::new("disk", count), &count, |b, &count| {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("bench.bpt");
            let mut tree = DiskBPTree::create(path.to_str().unwrap()).unwrap();
            for i in 0..count {
                let key = format!("key_{i:08}").into_bytes();
                tree.insert(key, format!("val_{i}").into_bytes()).unwrap();
            }
            tree.flush().unwrap();

            let mut idx = 0u64;
            b.iter(|| {
                let key = format!("key_{:08}", idx % count).into_bytes();
                let _ = tree.get(&key);
                idx += 1;
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_bptree_insert,
    bench_bptree_lookup,
    bench_bptree_range,
    bench_disk_bptree_insert,
    bench_disk_bptree_lookup,
);
criterion_main!(benches);
