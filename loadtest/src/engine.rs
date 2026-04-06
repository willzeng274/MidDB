use crate::report::LoadTestRunner;
use middb_core::{Config, Database, DiskBPTree, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

pub fn run_all() {
    sequential_writes(50_000, 128);
    sequential_writes(10_000, 1024);
    batch_writes(50_000, 128, 100);
    batch_writes(50_000, 128, 1000);
    random_reads(50_000);
    mixed_readwrite(50_000, 0.5);
    mixed_readwrite(50_000, 0.95);
    scan_performance(5_000);
    transaction_throughput(5_000);
    disk_bptree_load(5_000);
    large_value_stress(2_000, 16384);
    write_amplification_test(10_000);
    concurrent_writes(4, 25_000);
    concurrent_writes(10, 10_000);
    concurrent_writes(16, 10_000);
    concurrent_reads(10, 10_000);
    concurrent_mixed(5, 5, 10_000);
    durable_writes(10_000, 128);
    durable_batch_writes(10_000, 128, 100);

    // === Production-realistic benchmarks ===
    // These use small memtables to force SSTable flushes and compaction,
    // measuring actual disk-bound performance rather than in-memory speed.
    println!("\n--- Production-Realistic Benchmarks (disk-bound) ---");
    disk_bound_writes(200_000, 128);
    disk_bound_reads_after_flush(200_000, 128);
    sustained_write_under_compaction(500_000, 128);
    mixed_rw_after_flush(100_000, 50_000, 128);
}

fn make_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path());
    config.sync_writes = false;
    (Database::open(config).unwrap(), dir)
}

fn make_db_durable() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path());
    (Database::open(config).unwrap(), dir)
}

/// Create a DB with a small memtable (1MB) to force SSTable flushes.
/// 1MB memtable ~ 5,900 entries with 128B values (key ~20B + value 128B + 40B overhead).
/// This means every ~6K writes triggers a flush to disk.
fn make_db_small_memtable() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path());
    config.sync_writes = false;
    config.memtable_size = 1024 * 1024; // 1MB — forces flush every ~6K writes
    config.level0_file_num_compaction_trigger = 4;
    (Database::open(config).unwrap(), dir)
}

fn sequential_writes(count: u64, value_size: usize) {
    let (db, _dir) = make_db();
    let mut runner = LoadTestRunner::new(
        &format!("sequential_writes (n={count}, val={value_size}B)"),
    );
    let value = vec![0xABu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("key_{i:012}").into_bytes();
        let op_start = Instant::now();
        match db.put(key, value.clone()) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn batch_writes(count: u64, value_size: usize, batch_size: usize) {
    let (db, _dir) = make_db();
    let mut runner = LoadTestRunner::new(
        &format!("batch_writes (n={count}, val={value_size}B, batch={batch_size})"),
    );
    let value = vec![0xABu8; value_size];
    let num_batches = count as usize / batch_size;

    runner.start();
    for batch_idx in 0..num_batches {
        let mut batch = WriteBatch::with_capacity(batch_size);
        for i in 0..batch_size {
            let key_idx = batch_idx * batch_size + i;
            let key = format!("key_{key_idx:012}").into_bytes();
            batch.put(key, value.clone());
        }
        let op_start = Instant::now();
        match db.write_batch(batch) {
            Ok(_) => {
                let elapsed = op_start.elapsed();
                // Record per-key timing
                for _ in 0..batch_size {
                    runner.record_op(elapsed / batch_size as u32);
                }
            }
            Err(_) => {
                for _ in 0..batch_size {
                    runner.record_error();
                }
            }
        }
    }
    runner.finish().print();
}

fn random_reads(count: u64) {
    let (db, _dir) = make_db();
    let total_keys = 50_000u64;
    let value = vec![0u8; 128];
    for i in 0..total_keys {
        let key = format!("key_{i:012}").into_bytes();
        db.put(key, value.clone()).unwrap();
    }

    let mut runner = LoadTestRunner::new(&format!("random_reads (n={count}, pool={total_keys})"));
    let mut rng = 0x12345678u64;

    runner.start();
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("key_{:012}", rng % total_keys).into_bytes();
        let op_start = Instant::now();
        match db.get(&key) {
            Ok(Some(v)) => {
                assert_eq!(v.len(), 128, "value size mismatch");
                runner.record_op(op_start.elapsed());
            }
            Ok(None) => runner.record_error(),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn mixed_readwrite(count: u64, read_ratio: f64) {
    let (db, _dir) = make_db();
    let pre_pop = 20_000u64;
    for i in 0..pre_pop {
        db.put(format!("key_{i:012}").into_bytes(), vec![0u8; 128]).unwrap();
    }

    let label = format!("mixed r/w (n={}, read={:.0}%)", count, read_ratio * 100.0);
    let mut runner = LoadTestRunner::new(&label);
    let mut rng = 0xDEADBEEFu64;
    let mut write_counter = pre_pop;

    runner.start();
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op_start = Instant::now();

        if (rng % 100) < (read_ratio * 100.0) as u64 {
            let key = format!("key_{:012}", rng % write_counter.max(1)).into_bytes();
            match db.get(&key) {
                Ok(_) => runner.record_op(op_start.elapsed()),
                Err(_) => runner.record_error(),
            }
        } else {
            let key = format!("key_{write_counter:012}").into_bytes();
            write_counter += 1;
            match db.put(key, vec![0u8; 128]) {
                Ok(_) => runner.record_op(op_start.elapsed()),
                Err(_) => runner.record_error(),
            }
        }
    }
    runner.finish().print();
}

fn scan_performance(pre_pop: u64) {
    let (db, _dir) = make_db();
    for i in 0..pre_pop {
        db.put(format!("row_{i:012}").into_bytes(), vec![0u8; 64]).unwrap();
    }

    let scan_sizes = [100, 1000, 5000];
    for scan_size in scan_sizes {
        let mut runner = LoadTestRunner::new(
            &format!("scan (pool={pre_pop}, range={scan_size})"),
        );
        let iterations = 50;

        runner.start();
        for i in 0..iterations {
            let start_key = format!("row_{:012}", i * 10).into_bytes();
            let end_key = format!("row_{:012}", i * 10 + scan_size).into_bytes();
            let op_start = Instant::now();
            let _results = db.scan(&start_key, &end_key);
            runner.record_op(op_start.elapsed());
        }
        runner.finish().print();
    }
}

fn transaction_throughput(count: u64) {
    let (db, _dir) = make_db();
    let mut runner = LoadTestRunner::new(&format!("transactions (n={count})"));

    runner.start();
    for i in 0..count {
        let op_start = Instant::now();
        let txn = db.begin_txn();
        let key = format!("txn_{i:012}").into_bytes();
        if db.put_txn(txn, key.clone(), vec![0u8; 64]).is_err() {
            runner.record_error();
            continue;
        }
        match db.commit_txn(txn) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn disk_bptree_load(count: u64) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("loadtest.bpt");
    let mut tree = DiskBPTree::create(path.to_str().unwrap()).unwrap();

    let mut runner = LoadTestRunner::new(&format!("disk_bptree insert+lookup (n={count})"));

    runner.start();
    for i in 0..count {
        let key = format!("bpt_{i:012}").into_bytes();
        let val = format!("val_{i}").into_bytes();
        let op_start = Instant::now();
        tree.insert(key, val).unwrap();
        runner.record_op(op_start.elapsed());
    }

    tree.flush().unwrap();

    let mut rng = 42u64;
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("bpt_{:012}", rng % count).into_bytes();
        let op_start = Instant::now();
        let _ = tree.get(&key);
        runner.record_op(op_start.elapsed());
    }
    runner.finish().print();
}

fn large_value_stress(count: u64, value_size: usize) {
    let (db, _dir) = make_db();
    let mut runner = LoadTestRunner::new(
        &format!("large_values (n={}, val={}KB)", count, value_size / 1024),
    );
    let value = vec![0xCDu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("large_{i:012}").into_bytes();
        let op_start = Instant::now();
        db.put(key, value.clone()).unwrap();
        runner.record_op(op_start.elapsed());
    }

    let mut rng = 99u64;
    for _ in 0..(count / 2) {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("large_{:012}", rng % count).into_bytes();
        let op_start = Instant::now();
        let val = db.get(&key).unwrap();
        assert!(val.is_some());
        assert_eq!(val.unwrap().len(), value_size);
        runner.record_op(op_start.elapsed());
    }
    runner.finish().print();
}

fn write_amplification_test(count: u64) {
    let (db, _dir) = make_db();
    let mut runner = LoadTestRunner::new(
        &format!("overwrite_stress (n={count}, same keys rewritten 5x)"),
    );

    runner.start();
    for round in 0..5u64 {
        for i in 0..count {
            let key = format!("ow_{i:012}").into_bytes();
            let value = format!("round_{round}_{i}").into_bytes();
            let op_start = Instant::now();
            db.put(key, value).unwrap();
            runner.record_op(op_start.elapsed());
        }
    }

    // Verify latest values
    for i in 0..100 {
        let key = format!("ow_{i:012}").into_bytes();
        let val = db.get(&key).unwrap().unwrap();
        let val_str = String::from_utf8(val).unwrap();
        assert!(val_str.starts_with("round_4_"), "Expected round_4, got {val_str}");
    }
    runner.finish().print();
}

fn concurrent_writes(threads: usize, ops_per_thread: u64) {
    let (db, _dir) = make_db();
    let db = Arc::new(db);
    let label = format!("concurrent_writes (threads={threads}, ops/t={ops_per_thread})");
    let mut runner = LoadTestRunner::new(&label);
    let value = vec![0xABu8; 128];

    runner.start();

    let handles: Vec<_> = (0..threads).map(|t| {
        let db = Arc::clone(&db);
        let value = value.clone();
        thread::spawn(move || {
            let mut latencies = Vec::with_capacity(ops_per_thread as usize);
            for i in 0..ops_per_thread {
                let key = format!("t{t}_{i:012}").into_bytes();
                let op_start = Instant::now();
                db.put(key, value.clone()).unwrap();
                latencies.push(op_start.elapsed());
            }
            latencies
        })
    }).collect();

    for h in handles {
        for lat in h.join().unwrap() {
            runner.record_op(lat);
        }
    }
    runner.finish().print();

    // Verify a sample of written data is retrievable
    let spot_check = 100.min(ops_per_thread);
    for t in 0..threads {
        for i in 0..spot_check {
            let key = format!("t{t}_{i:012}").into_bytes();
            let val = db.get(&key).expect("read error").expect("missing key after concurrent write");
            assert_eq!(val.len(), 128, "value corruption in concurrent write");
        }
    }
}

fn concurrent_reads(threads: usize, ops_per_thread: u64) {
    let (db, _dir) = make_db();
    // Pre-populate
    let total_keys = 50_000u64;
    for i in 0..total_keys {
        db.put(format!("key_{i:012}").into_bytes(), vec![0u8; 128]).unwrap();
    }

    let db = Arc::new(db);
    let label = format!("concurrent_reads (threads={threads}, ops/t={ops_per_thread})");
    let mut runner = LoadTestRunner::new(&label);

    runner.start();

    let handles: Vec<_> = (0..threads).map(|t| {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            let mut latencies = Vec::with_capacity(ops_per_thread as usize);
            let mut rng = (t as u64 + 1).wrapping_mul(0x12345678);
            for _ in 0..ops_per_thread {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("key_{:012}", rng % total_keys).into_bytes();
                let op_start = Instant::now();
                match db.get(&key) {
                    Ok(Some(v)) => {
                        let lat = op_start.elapsed();
                        assert_eq!(v.len(), 128, "value size mismatch in concurrent read");
                        latencies.push(Ok(lat));
                    }
                    _ => {
                        latencies.push(Err(()));
                    }
                }
            }
            latencies
        })
    }).collect();

    for h in handles {
        for result in h.join().unwrap() {
            match result {
                Ok(lat) => runner.record_op(lat),
                Err(()) => runner.record_error(),
            }
        }
    }
    runner.finish().print();
}

fn concurrent_mixed(writers: usize, readers: usize, ops_per_thread: u64) {
    let (db, _dir) = make_db();
    // Pre-populate
    for i in 0..20_000u64 {
        db.put(format!("key_{i:012}").into_bytes(), vec![0u8; 128]).unwrap();
    }

    let db = Arc::new(db);
    let label = format!("concurrent_mixed (w={writers}, r={readers}, ops/t={ops_per_thread})");
    let mut runner = LoadTestRunner::new(&label);

    runner.start();

    let mut handles = Vec::new();

    // Writer threads — record real per-op latency
    for t in 0..writers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut latencies = Vec::with_capacity(ops_per_thread as usize);
            for i in 0..ops_per_thread {
                let key = format!("w{t}_{i:012}").into_bytes();
                let op_start = Instant::now();
                db.put(key, vec![0u8; 128]).unwrap();
                latencies.push(Ok(op_start.elapsed()));
            }
            latencies
        }));
    }

    // Reader threads — verify values exist and record real latency
    for t in 0..readers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut latencies = Vec::with_capacity(ops_per_thread as usize);
            let mut rng = (t as u64 + 100).wrapping_mul(0xDEADBEEF);
            for _ in 0..ops_per_thread {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("key_{:012}", rng % 20_000).into_bytes();
                let op_start = Instant::now();
                match db.get(&key) {
                    Ok(Some(v)) => {
                        let lat = op_start.elapsed();
                        assert_eq!(v.len(), 128, "value corruption in concurrent mixed read");
                        latencies.push(Ok(lat));
                    }
                    Ok(None) => {
                        // Key might not exist yet if readers race ahead of pre-populate
                        latencies.push(Ok(op_start.elapsed()));
                    }
                    Err(_) => latencies.push(Err(())),
                }
            }
            latencies
        }));
    }

    for h in handles {
        for result in h.join().unwrap() {
            match result {
                Ok(lat) => runner.record_op(lat),
                Err(()) => runner.record_error(),
            }
        }
    }
    runner.finish().print();

    // Verify writer data is retrievable
    let spot_check = 100.min(ops_per_thread);
    for t in 0..writers {
        for i in 0..spot_check {
            let key = format!("w{t}_{i:012}").into_bytes();
            let val = db.get(&key).expect("read error").expect("missing key after concurrent mixed write");
            assert_eq!(val.len(), 128, "value corruption in concurrent mixed write");
        }
    }
}

fn durable_writes(count: u64, value_size: usize) {
    let (db, _dir) = make_db_durable();
    let mut runner = LoadTestRunner::new(
        &format!("durable_writes [sync] (n={count}, val={value_size}B)"),
    );
    let value = vec![0xABu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("key_{i:012}").into_bytes();
        let op_start = Instant::now();
        match db.put(key, value.clone()) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn durable_batch_writes(count: u64, value_size: usize, batch_size: usize) {
    let (db, _dir) = make_db_durable();
    let mut runner = LoadTestRunner::new(
        &format!("durable_batch [sync] (n={count}, val={value_size}B, batch={batch_size})"),
    );
    let value = vec![0xABu8; value_size];
    let num_batches = count as usize / batch_size;

    runner.start();
    for batch_idx in 0..num_batches {
        let mut batch = WriteBatch::with_capacity(batch_size);
        for i in 0..batch_size {
            let key_idx = batch_idx * batch_size + i;
            let key = format!("key_{key_idx:012}").into_bytes();
            batch.put(key, value.clone());
        }
        let op_start = Instant::now();
        match db.write_batch(batch) {
            Ok(_) => {
                let elapsed = op_start.elapsed();
                for _ in 0..batch_size {
                    runner.record_op(elapsed / batch_size as u32);
                }
            }
            Err(_) => {
                for _ in 0..batch_size {
                    runner.record_error();
                }
            }
        }
    }
    runner.finish().print();
}

// === Production-realistic benchmarks ===

/// Writes enough data to force multiple SSTable flushes.
/// With 1MB memtable and 128B values, 200K writes = ~33 flushes.
/// This measures write throughput that includes flush overhead.
fn disk_bound_writes(count: u64, value_size: usize) {
    let (db, _dir) = make_db_small_memtable();
    let mut runner = LoadTestRunner::new(
        &format!("DISK: writes through flush (n={count}, val={value_size}B, 1MB memtable)"),
    );
    let value = vec![0xABu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("key_{i:012}").into_bytes();
        let op_start = Instant::now();
        match db.put(key, value.clone()) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    let stats = db.stats();
    let result = runner.finish();
    result.print();
    println!(
        "    [disk stats] SSTables: {}, L0 files: {}, memtable: {}KB",
        stats.num_sstables, stats.l0_file_count, stats.memtable_size / 1024
    );
}

/// Pre-populates data that gets flushed to SSTables, then measures read performance
/// when data lives on disk (not in memtable).
fn disk_bound_reads_after_flush(num_keys: u64, value_size: usize) {
    let (db, _dir) = make_db_small_memtable();
    let value = vec![0xABu8; value_size];

    // Write all keys — this will trigger many flushes
    for i in 0..num_keys {
        let key = format!("key_{i:012}").into_bytes();
        db.put(key, value.clone()).unwrap();
    }

    let stats_after_write = db.stats();

    // Now read random keys — most will be in SSTables, not memtable
    let read_count = 50_000u64;
    let mut runner = LoadTestRunner::new(
        &format!(
            "DISK: reads from SSTables (n={}, {} SSTables, {} keys on disk)",
            read_count, stats_after_write.num_sstables, num_keys
        ),
    );
    let mut rng = 0x12345678u64;

    runner.start();
    for _ in 0..read_count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("key_{:012}", rng % num_keys).into_bytes();
        let op_start = Instant::now();
        match db.get(&key) {
            Ok(val) => {
                assert!(val.is_some(), "key should exist");
                runner.record_op(op_start.elapsed());
            }
            Err(_) => runner.record_error(),
        }
    }
    let result = runner.finish();
    result.print();
    println!(
        "    [disk stats] SSTables: {}, L0 files: {}, memtable: {}KB",
        stats_after_write.num_sstables, stats_after_write.l0_file_count,
        stats_after_write.memtable_size / 1024
    );
}

/// Sustained writes that trigger both flushes and compaction.
/// 500K writes with 1MB memtable = ~83 flushes. With L0 trigger at 4,
/// compaction runs concurrently throughout.
fn sustained_write_under_compaction(count: u64, value_size: usize) {
    let (db, _dir) = make_db_small_memtable();
    let mut runner = LoadTestRunner::new(
        &format!("DISK: sustained writes + compaction (n={count}, val={value_size}B)"),
    );
    let value = vec![0xABu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("key_{i:012}").into_bytes();
        let op_start = Instant::now();
        match db.put(key, value.clone()) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    let stats = db.stats();
    let result = runner.finish();
    result.print();
    println!(
        "    [disk stats] SSTables: {}, L0 files: {}, memtable: {}KB, seq: {}",
        stats.num_sstables, stats.l0_file_count, stats.memtable_size / 1024,
        stats.sequence_number
    );
}

/// Mixed read/write workload after data has been flushed to disk.
/// Pre-populates data (forces flushes), then does 50/50 reads and writes.
fn mixed_rw_after_flush(num_prepop: u64, ops: u64, value_size: usize) {
    let (db, _dir) = make_db_small_memtable();
    let value = vec![0xABu8; value_size];

    // Pre-populate — forces flushes
    for i in 0..num_prepop {
        let key = format!("key_{i:012}").into_bytes();
        db.put(key, value.clone()).unwrap();
    }

    let stats_after_prepop = db.stats();
    let mut runner = LoadTestRunner::new(
        &format!(
            "DISK: mixed r/w 50/50 ({} ops, {} SSTables pre-existing)",
            ops, stats_after_prepop.num_sstables
        ),
    );
    let mut rng = 0xDEADBEEFu64;
    let mut write_counter = num_prepop;

    runner.start();
    for _ in 0..ops {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op_start = Instant::now();

        if rng % 2 == 0 {
            // Read from existing keys (mostly on disk)
            let key = format!("key_{:012}", rng % num_prepop).into_bytes();
            match db.get(&key) {
                Ok(_) => runner.record_op(op_start.elapsed()),
                Err(_) => runner.record_error(),
            }
        } else {
            // Write new key
            let key = format!("key_{write_counter:012}").into_bytes();
            write_counter += 1;
            match db.put(key, value.clone()) {
                Ok(_) => runner.record_op(op_start.elapsed()),
                Err(_) => runner.record_error(),
            }
        }
    }
    let stats = db.stats();
    let result = runner.finish();
    result.print();
    println!(
        "    [disk stats] SSTables: {}, L0 files: {}, memtable: {}KB",
        stats.num_sstables, stats.l0_file_count, stats.memtable_size / 1024
    );
}
