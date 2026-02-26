use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use middb_core::{Config, Database};
use tempfile::TempDir;

fn make_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path());
    let db = Database::open(config).unwrap();
    (db, dir)
}

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    for size in [64, 256, 1024, 4096] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let (db, _dir) = make_db();
            let value = vec![0xABu8; size];
            let mut i = 0u64;
            b.iter(|| {
                let key = format!("key_{:012}", i).into_bytes();
                db.put(key, value.clone()).unwrap();
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    for count in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let (db, _dir) = make_db();
            for i in 0..count {
                let key = format!("key_{:012}", i).into_bytes();
                db.put(key, vec![0u8; 128]).unwrap();
            }
            let mut i = 0u64;
            b.iter(|| {
                let key = format!("key_{:012}", i % count).into_bytes();
                let _ = db.get(&key);
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    group.bench_function("delete_existing", |b| {
        let (db, _dir) = make_db();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{:012}", i).into_bytes();
            db.put(key.clone(), vec![0u8; 64]).unwrap();
            db.delete(key).unwrap();
            i += 1;
        });
    });
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    for count in [100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let (db, _dir) = make_db();
            for i in 0..count {
                let key = format!("key_{:012}", i).into_bytes();
                db.put(key, vec![0u8; 64]).unwrap();
            }
            let start = b"key_000000000000".to_vec();
            let end = b"key_999999999999".to_vec();
            b.iter(|| {
                let _ = db.scan(&start, &end);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_put, bench_get, bench_delete, bench_scan);
criterion_main!(benches);
