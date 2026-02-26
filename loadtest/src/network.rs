use crate::report::LoadTestRunner;
use middb_core::{Config, Database};
use middb_network::{Client, Request, Server};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

pub async fn run_all() {
    single_client_throughput(5_000).await;
    concurrent_clients(10, 2_000).await;
    concurrent_clients(50, 500).await;
    batch_throughput(500).await;
    pipeline_throughput(1_000).await;
    transaction_over_network(2_000).await;
}

async fn start_server(port: u16) -> (Arc<Database>, TempDir, tokio::task::JoinHandle<()>) {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path());
    config.sync_writes = false;
    let db = Arc::new(Database::open(config).unwrap());
    let addr = format!("127.0.0.1:{}", port);
    let server = Server::from_arc(Arc::clone(&db), addr);
    let handle = tokio::spawn(async move {
        server.run().await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;
    (db, dir, handle)
}

async fn single_client_throughput(count: u64) {
    let (_db, _dir, handle) = start_server(21001).await;
    let mut client = Client::connect("127.0.0.1:21001").await.unwrap();

    let mut runner = LoadTestRunner::new(&format!("net_single_client (n={})", count));
    let value = vec![0xABu8; 128];

    runner.start();
    for i in 0..count {
        let key = format!("net_{:012}", i).into_bytes();
        let op_start = Instant::now();
        match client.put(&key, &value).await {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }

    // Read them back
    let mut rng = 0u64;
    for _ in 0..(count / 2) {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("net_{:012}", rng % count).into_bytes();
        let op_start = Instant::now();
        match client.get(&key).await {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
    handle.abort();
}

async fn concurrent_clients(num_clients: usize, ops_per_client: u64) {
    let port = 21002 + num_clients as u16;
    let (_db, _dir, handle) = start_server(port).await;
    let addr = format!("127.0.0.1:{}", port);

    let start = Instant::now();
    let mut handles = Vec::new();

    for c in 0..num_clients {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr).await.unwrap();
            let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
            let mut errors = 0u64;

            for i in 0..ops_per_client {
                let key = format!("c{}_{:08}", c, i).into_bytes();
                let op_start = Instant::now();
                if client.put(&key, &[0u8; 64]).await.is_ok() {
                    let _ = hist.record(op_start.elapsed().as_micros() as u64);
                } else {
                    errors += 1;
                }
            }
            (hist, errors)
        }));
    }

    let mut combined = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    let mut total_errors = 0u64;
    for h in handles {
        let (hist, errors) = h.await.unwrap();
        combined.add(&hist).unwrap();
        total_errors += errors;
    }
    let elapsed = start.elapsed();
    let total_ops = (num_clients as u64) * ops_per_client;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!("  net_concurrent (clients={}, ops_each={})", num_clients, ops_per_client);
    println!("    ops: {:>10}  |  elapsed: {:.2}s  |  throughput: {:.0} ops/s",
        total_ops, elapsed.as_secs_f64(), ops_per_sec);
    println!("    latency (μs): mean={:.0}  p50={}  p95={}  p99={}  max={}",
        combined.mean(),
        combined.value_at_quantile(0.50),
        combined.value_at_quantile(0.95),
        combined.value_at_quantile(0.99),
        combined.max());
    if total_errors > 0 {
        println!("    errors: {}", total_errors);
    }
    println!();

    handle.abort();
}

async fn batch_throughput(batches: u64) {
    let (_db, _dir, handle) = start_server(21100).await;
    let mut client = Client::connect("127.0.0.1:21100").await.unwrap();

    let batch_size = 50;
    let mut runner = LoadTestRunner::new(
        &format!("net_batch_put (batches={}, batch_size={})", batches, batch_size),
    );

    runner.start();
    for b in 0..batches {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                let key = format!("batch_{}_{}", b, i).into_bytes();
                (key, vec![0u8; 64])
            })
            .collect();

        let op_start = Instant::now();
        match client.batch_put(pairs).await {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }

    // Batch reads
    for b in 0..(batches / 2) {
        let keys: Vec<Vec<u8>> = (0..batch_size)
            .map(|i| format!("batch_{}_{}", b, i).into_bytes())
            .collect();

        let op_start = Instant::now();
        match client.batch_get(keys).await {
            Ok(vals) => {
                assert_eq!(vals.len(), batch_size);
                runner.record_op(op_start.elapsed());
            }
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
    handle.abort();
}

async fn pipeline_throughput(count: u64) {
    let (_db, _dir, handle) = start_server(21101).await;
    let mut client = Client::connect("127.0.0.1:21101").await.unwrap();

    // Pre-populate
    for i in 0..1000u64 {
        client.put(&format!("pipe_{:06}", i).into_bytes(), &[0u8; 64]).await.unwrap();
    }

    let pipeline_size = 20;
    let mut runner = LoadTestRunner::new(
        &format!("net_pipeline (n={}, pipeline_size={})", count, pipeline_size),
    );

    runner.start();
    let batches = count / pipeline_size as u64;
    for b in 0..batches {
        let requests: Vec<Request> = (0..pipeline_size)
            .map(|i| {
                let idx = (b * pipeline_size as u64 + i as u64) % 1000;
                Request::Get { key: format!("pipe_{:06}", idx).into_bytes() }
            })
            .collect();

        let op_start = Instant::now();
        match client.pipeline(requests).await {
            Ok(responses) => {
                assert_eq!(responses.len(), pipeline_size);
                runner.record_op(op_start.elapsed());
            }
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
    handle.abort();
}

async fn transaction_over_network(count: u64) {
    let (_db, _dir, handle) = start_server(21102).await;
    let mut client = Client::connect("127.0.0.1:21102").await.unwrap();

    let mut runner = LoadTestRunner::new(&format!("net_transactions (n={})", count));

    runner.start();
    for i in 0..count {
        let op_start = Instant::now();
        let txn_id = match client.begin_txn().await {
            Ok(id) => id,
            Err(_) => { runner.record_error(); continue; }
        };

        let key = format!("ntxn_{:012}", i);
        if client.txn_put(txn_id, key.as_bytes(), &[0u8; 64]).await.is_err() {
            runner.record_error();
            let _ = client.abort_txn(txn_id).await;
            continue;
        }

        match client.commit_txn(txn_id).await {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
    handle.abort();
}
