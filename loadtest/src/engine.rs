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
    // Concurrent benchmarks
    concurrent_writes(10, 10_000);
    concurrent_reads(10, 10_000);
    concurrent_mixed(5, 5, 10_000);
    // Durability benchmarks (sync_writes=true)
    durable_writes(10_000, 128);
    durable_batch_writes(10_000, 128, 100);
}

fn make_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path());
    config.sync_writes = false;
    (Database::open(config).unwrap(), dir)
}

fn make_db_durable() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path()); // sync_writes=true by default
    (Database::open(config).unwrap(), dir)
}

fn sequential_writes(count: u64, value_size: usize) {
    let (db, _dir) = make_db();
    let mut runner = LoadTestRunner::new(
        &format!("sequential_writes (n={}, val={}B)", count, value_size),
    );
    let value = vec![0xABu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("key_{:012}", i).into_bytes();
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
        &format!("batch_writes (n={}, val={}B, batch={})", count, value_size, batch_size),
    );
    let value = vec![0xABu8; value_size];
    let num_batches = count as usize / batch_size;

    runner.start();
    for batch_idx in 0..num_batches {
        let mut batch = WriteBatch::with_capacity(batch_size);
        for i in 0..batch_size {
            let key_idx = batch_idx * batch_size + i;
            let key = format!("key_{:012}", key_idx).into_bytes();
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
    for i in 0..total_keys {
        let key = format!("key_{:012}", i).into_bytes();
        db.put(key, vec![0u8; 128]).unwrap();
    }

    let mut runner = LoadTestRunner::new(&format!("random_reads (n={}, pool={})", count, total_keys));
    let mut rng = 0x12345678u64;

    runner.start();
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("key_{:012}", rng % total_keys).into_bytes();
        let op_start = Instant::now();
        match db.get(&key) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn mixed_readwrite(count: u64, read_ratio: f64) {
    let (db, _dir) = make_db();
    let pre_pop = 20_000u64;
    for i in 0..pre_pop {
        db.put(format!("key_{:012}", i).into_bytes(), vec![0u8; 128]).unwrap();
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
            let key = format!("key_{:012}", write_counter).into_bytes();
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
        db.put(format!("row_{:012}", i).into_bytes(), vec![0u8; 64]).unwrap();
    }

    let scan_sizes = [100, 1000, 5000];
    for scan_size in scan_sizes {
        let mut runner = LoadTestRunner::new(
            &format!("scan (pool={}, range={})", pre_pop, scan_size),
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
    let mut runner = LoadTestRunner::new(&format!("transactions (n={})", count));

    runner.start();
    for i in 0..count {
        let op_start = Instant::now();
        let txn = db.begin_txn();
        let key = format!("txn_{:012}", i).into_bytes();
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

    let mut runner = LoadTestRunner::new(&format!("disk_bptree insert+lookup (n={})", count));

    runner.start();
    for i in 0..count {
        let key = format!("bpt_{:012}", i).into_bytes();
        let val = format!("val_{}", i).into_bytes();
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
        let key = format!("large_{:012}", i).into_bytes();
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
        &format!("overwrite_stress (n={}, same keys rewritten 5x)", count),
    );

    runner.start();
    for round in 0..5u64 {
        for i in 0..count {
            let key = format!("ow_{:012}", i).into_bytes();
            let value = format!("round_{}_{}", round, i).into_bytes();
            let op_start = Instant::now();
            db.put(key, value).unwrap();
            runner.record_op(op_start.elapsed());
        }
    }

    // Verify latest values
    for i in 0..100 {
        let key = format!("ow_{:012}", i).into_bytes();
        let val = db.get(&key).unwrap().unwrap();
        let val_str = String::from_utf8(val).unwrap();
        assert!(val_str.starts_with("round_4_"), "Expected round_4, got {}", val_str);
    }
    runner.finish().print();
}

fn concurrent_writes(threads: usize, ops_per_thread: u64) {
    let (db, _dir) = make_db();
    let db = Arc::new(db);
    let label = format!("concurrent_writes (threads={}, ops/t={})", threads, ops_per_thread);
    let mut runner = LoadTestRunner::new(&label);
    let value = vec![0xABu8; 128];

    runner.start();
    let total_start = Instant::now();

    let handles: Vec<_> = (0..threads).map(|t| {
        let db = Arc::clone(&db);
        let value = value.clone();
        thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("t{}_{:012}", t, i).into_bytes();
                db.put(key, value.clone()).unwrap();
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_elapsed = total_start.elapsed();
    let total_ops = threads as u64 * ops_per_thread;
    let per_op = total_elapsed / total_ops as u32;
    for _ in 0..total_ops {
        runner.record_op(per_op);
    }
    runner.finish().print();
}

fn concurrent_reads(threads: usize, ops_per_thread: u64) {
    let (db, _dir) = make_db();
    // Pre-populate
    let total_keys = 50_000u64;
    for i in 0..total_keys {
        db.put(format!("key_{:012}", i).into_bytes(), vec![0u8; 128]).unwrap();
    }

    let db = Arc::new(db);
    let label = format!("concurrent_reads (threads={}, ops/t={})", threads, ops_per_thread);
    let mut runner = LoadTestRunner::new(&label);

    runner.start();
    let total_start = Instant::now();

    let handles: Vec<_> = (0..threads).map(|t| {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            let mut rng = (t as u64 + 1).wrapping_mul(0x12345678);
            for _ in 0..ops_per_thread {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("key_{:012}", rng % total_keys).into_bytes();
                let _ = db.get(&key);
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_elapsed = total_start.elapsed();
    let total_ops = threads as u64 * ops_per_thread;
    let per_op = total_elapsed / total_ops as u32;
    for _ in 0..total_ops {
        runner.record_op(per_op);
    }
    runner.finish().print();
}

fn concurrent_mixed(writers: usize, readers: usize, ops_per_thread: u64) {
    let (db, _dir) = make_db();
    // Pre-populate
    for i in 0..20_000u64 {
        db.put(format!("key_{:012}", i).into_bytes(), vec![0u8; 128]).unwrap();
    }

    let db = Arc::new(db);
    let label = format!("concurrent_mixed (w={}, r={}, ops/t={})", writers, readers, ops_per_thread);
    let mut runner = LoadTestRunner::new(&label);

    runner.start();
    let total_start = Instant::now();

    let mut handles = Vec::new();

    // Writer threads
    for t in 0..writers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("w{}_{:012}", t, i).into_bytes();
                db.put(key, vec![0u8; 128]).unwrap();
            }
        }));
    }

    // Reader threads
    for t in 0..readers {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut rng = (t as u64 + 100).wrapping_mul(0xDEADBEEF);
            for _ in 0..ops_per_thread {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("key_{:012}", rng % 20_000).into_bytes();
                let _ = db.get(&key);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let total_elapsed = total_start.elapsed();
    let total_ops = (writers + readers) as u64 * ops_per_thread;
    let per_op = total_elapsed / total_ops as u32;
    for _ in 0..total_ops {
        runner.record_op(per_op);
    }
    runner.finish().print();
}

fn durable_writes(count: u64, value_size: usize) {
    let (db, _dir) = make_db_durable();
    let mut runner = LoadTestRunner::new(
        &format!("durable_writes [sync] (n={}, val={}B)", count, value_size),
    );
    let value = vec![0xABu8; value_size];

    runner.start();
    for i in 0..count {
        let key = format!("key_{:012}", i).into_bytes();
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
        &format!("durable_batch [sync] (n={}, val={}B, batch={})", count, value_size, batch_size),
    );
    let value = vec![0xABu8; value_size];
    let num_batches = count as usize / batch_size;

    runner.start();
    for batch_idx in 0..num_batches {
        let mut batch = WriteBatch::with_capacity(batch_size);
        for i in 0..batch_size {
            let key_idx = batch_idx * batch_size + i;
            let key = format!("key_{:012}", key_idx).into_bytes();
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
