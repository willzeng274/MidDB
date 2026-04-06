use criterion::{criterion_group, criterion_main, Criterion};
use hdrhistogram::Histogram;
use middb_core::{Config, Database};
use std::time::Instant;
use tempfile::TempDir;

fn make_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path());
    config.sync_writes = false;
    let db = Database::open(config).unwrap();
    (db, dir)
}

fn random_key(state: &mut u64) -> Vec<u8> {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    format!("user_{:012}", *state % 10000).into_bytes()
}

fn run_workload(
    db: &Database,
    ops: usize,
    read_ratio: f64,
    update_ratio: f64,
) -> WorkloadResult {
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut rng_state = 0xDEADBEEFu64;

    // Pre-populate
    for i in 0..10000 {
        let key = format!("user_{i:012}").into_bytes();
        db.put(key, vec![0x42u8; 100]).unwrap();
    }

    let start = Instant::now();
    for _ in 0..ops {
        let op_start = Instant::now();
        let key = random_key(&mut rng_state);

        let r = (rng_state % 100) as f64 / 100.0;
        if r < read_ratio {
            let _ = db.get(&key);
        } else if r < read_ratio + update_ratio {
            let _ = db.put(key, vec![0x42u8; 100]);
        } else {
            let _ = db.put(random_key(&mut rng_state), vec![0x42u8; 100]);
        }

        let elapsed_us = op_start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed_us.max(1));
    }
    let total_elapsed = start.elapsed();

    WorkloadResult {
        ops_per_sec: ops as f64 / total_elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p95_us: hist.value_at_quantile(0.95),
        p99_us: hist.value_at_quantile(0.99),
    }
}

struct WorkloadResult {
    ops_per_sec: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
}

// Workload A: 50% read, 50% update (update-heavy)
fn bench_ycsb_a(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb");
    group.sample_size(10);
    group.bench_function("workload_a_update_heavy", |b| {
        let (db, _dir) = make_db();
        b.iter(|| {
            let result = run_workload(&db, 10000, 0.5, 0.5);
            eprintln!("A: {:.0} ops/s, p50={} p95={} p99={}us",
                result.ops_per_sec, result.p50_us, result.p95_us, result.p99_us);
        });
    });
    group.finish();
}

// Workload B: 95% read, 5% update (read-heavy)
fn bench_ycsb_b(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb");
    group.sample_size(10);
    group.bench_function("workload_b_read_heavy", |b| {
        let (db, _dir) = make_db();
        b.iter(|| {
            let result = run_workload(&db, 10000, 0.95, 0.05);
            eprintln!("B: {:.0} ops/s, p50={} p95={} p99={}us",
                result.ops_per_sec, result.p50_us, result.p95_us, result.p99_us);
        });
    });
    group.finish();
}

// Workload C: 100% read
fn bench_ycsb_c(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb");
    group.sample_size(10);
    group.bench_function("workload_c_read_only", |b| {
        let (db, _dir) = make_db();
        b.iter(|| {
            let result = run_workload(&db, 10000, 1.0, 0.0);
            eprintln!("C: {:.0} ops/s, p50={} p95={} p99={}us",
                result.ops_per_sec, result.p50_us, result.p95_us, result.p99_us);
        });
    });
    group.finish();
}

// Workload D: 95% read, 5% insert (read-latest)
fn bench_ycsb_d(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb");
    group.sample_size(10);
    group.bench_function("workload_d_read_latest", |b| {
        let (db, _dir) = make_db();
        b.iter(|| {
            let result = run_workload(&db, 10000, 0.95, 0.0);
            eprintln!("D: {:.0} ops/s, p50={} p95={} p99={}us",
                result.ops_per_sec, result.p50_us, result.p95_us, result.p99_us);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_ycsb_a, bench_ycsb_b, bench_ycsb_c, bench_ycsb_d);
criterion_main!(benches);
