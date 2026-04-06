use crate::report::LoadTestRunner;
use middb_cluster::{ClusterConfig, ClusterNode};
use middb_core::{Config, Database};
use middb_network::{Client, RequestHandler, Server};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct TestCluster {
    _nodes: Vec<Arc<ClusterNode>>,
    _dirs: Vec<TempDir>,
    addrs: Vec<String>,
}

impl TestCluster {
    async fn start(n: usize, base_port: u16) -> Self {
        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        let addrs: Vec<String> = (0..n)
            .map(|i| format!("127.0.0.1:{}", base_port + i as u16))
            .collect();

        for i in 0..n {
            let addr = addrs[i].clone();
            let dir = TempDir::new().unwrap();
            let mut config = Config::new(dir.path());
            config.sync_writes = false;
            let db = Database::open(config).unwrap();

            let cluster_config = ClusterConfig {
                replication_factor: 3.min(n),
                write_quorum: if n >= 2 { 2 } else { 1 },
                read_quorum: 1,
                heartbeat_interval: Duration::from_secs(2),
                quorum_timeout: Duration::from_secs(2),
                pool_size: 8,
            };

            let node = ClusterNode::new(db, addr.clone(), cluster_config);
            if i == 0 {
                node.bootstrap().await;
            }

            node.start_heartbeat();
            let handler = node.clone() as Arc<dyn RequestHandler>;
            let server = Server::with_handler(handler, addr.clone());
            tokio::spawn(async move { let _ = server.run().await; });

            // Wait for TCP listener
            for _ in 0..50 {
                if tokio::net::TcpStream::connect(&addr).await.is_ok() { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            if i > 0 {
                node.join(&addrs[0]).await.unwrap();
            }

            nodes.push(node);
            dirs.push(dir);
        }

        TestCluster { _nodes: nodes, _dirs: dirs, addrs }
    }
}

pub async fn run_all() {
    cluster_write_throughput(10_000, 22000).await;
    cluster_read_throughput(10_000, 22010).await;
    cluster_mixed_rw(10_000, 0.5, 22020).await;
    cluster_mixed_rw(10_000, 0.95, 22030).await;
    cluster_concurrent_writes(3, 5_000, 22040).await;
    cluster_correctness_check(1_000, 22050).await;
}

async fn cluster_write_throughput(count: u64, base_port: u16) {
    let cluster = TestCluster::start(3, base_port).await;
    let mut client = Client::connect(&cluster.addrs[0]).await.unwrap();
    let mut runner = LoadTestRunner::new(
        &format!("CLUSTER: writes (n={count}, 3 nodes, RF=3, W=2)"),
    );
    let value = vec![0xABu8; 128];

    runner.start();
    for i in 0..count {
        let key = format!("cw_{i:012}").into_bytes();
        let op_start = Instant::now();
        match client.put(&key, &value).await {
            Ok(()) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

async fn cluster_read_throughput(count: u64, base_port: u16) {
    let cluster = TestCluster::start(3, base_port).await;
    let mut client = Client::connect(&cluster.addrs[0]).await.unwrap();
    let value = vec![0xABu8; 128];

    // Pre-populate
    let prepop = 5_000u64;
    for i in 0..prepop {
        let key = format!("cr_{i:012}").into_bytes();
        client.put(&key, &value).await.unwrap();
    }

    let mut runner = LoadTestRunner::new(
        &format!("CLUSTER: reads (n={count}, 3 nodes, pool={prepop})"),
    );
    let mut rng = 0x12345678u64;

    runner.start();
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("cr_{:012}", rng % prepop).into_bytes();
        let op_start = Instant::now();
        match client.get(&key).await {
            Ok(Some(v)) => {
                assert_eq!(v.len(), 128, "value corruption in cluster read");
                runner.record_op(op_start.elapsed());
            }
            Ok(None) => runner.record_error(),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

async fn cluster_mixed_rw(count: u64, read_ratio: f64, base_port: u16) {
    let cluster = TestCluster::start(3, base_port).await;
    let mut client = Client::connect(&cluster.addrs[0]).await.unwrap();
    let value = vec![0xABu8; 128];

    let prepop = 5_000u64;
    for i in 0..prepop {
        let key = format!("cm_{i:012}").into_bytes();
        client.put(&key, &value).await.unwrap();
    }

    let label = format!("CLUSTER: mixed r/w (n={}, read={:.0}%, 3 nodes)", count, read_ratio * 100.0);
    let mut runner = LoadTestRunner::new(&label);
    let mut rng = 0xDEADBEEFu64;
    let mut write_counter = prepop;

    runner.start();
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op_start = Instant::now();

        if (rng % 100) < (read_ratio * 100.0) as u64 {
            let key = format!("cm_{:012}", rng % write_counter.max(1)).into_bytes();
            match client.get(&key).await {
                Ok(_) => runner.record_op(op_start.elapsed()),
                Err(_) => runner.record_error(),
            }
        } else {
            let key = format!("cm_{write_counter:012}").into_bytes();
            write_counter += 1;
            match client.put(&key, &value).await {
                Ok(()) => runner.record_op(op_start.elapsed()),
                Err(_) => runner.record_error(),
            }
        }
    }
    runner.finish().print();
}

async fn cluster_concurrent_writes(num_clients: usize, ops_per_client: u64, base_port: u16) {
    let cluster = TestCluster::start(3, base_port).await;
    let label = format!(
        "CLUSTER: concurrent writes (clients={num_clients}, ops/c={ops_per_client}, 3 nodes)"
    );
    let mut runner = LoadTestRunner::new(&label);
    let value = vec![0xABu8; 128];

    runner.start();

    let handles: Vec<_> = (0..num_clients).map(|c| {
        let addr = cluster.addrs[c % cluster.addrs.len()].clone();
        let value = value.clone();
        tokio::spawn(async move {
            let mut client = Client::connect(&addr).await.unwrap();
            let mut latencies = Vec::with_capacity(ops_per_client as usize);
            for i in 0..ops_per_client {
                let key = format!("cc{c}_{i:012}").into_bytes();
                let op_start = Instant::now();
                client.put(&key, &value).await.unwrap();
                latencies.push(op_start.elapsed());
            }
            latencies
        })
    }).collect();

    for h in handles {
        for lat in h.await.unwrap() {
            runner.record_op(lat);
        }
    }
    runner.finish().print();

    // Verify data correctness — read through coordinated path
    let mut client = Client::connect(&cluster.addrs[0]).await.unwrap();
    let spot_check = 20.min(ops_per_client);
    let mut missing = 0u64;
    for c in 0..num_clients {
        for i in 0..spot_check {
            let key = format!("cc{c}_{i:012}").into_bytes();
            match client.get(&key).await {
                Ok(Some(v)) => assert_eq!(v.len(), 128),
                _ => missing += 1,
            }
        }
    }
    if missing > 0 {
        println!("    [WARN] {}/{} spot-check keys missing (eventual consistency)", missing, spot_check * num_clients as u64);
    }
}

async fn cluster_correctness_check(count: u64, base_port: u16) {
    let cluster = TestCluster::start(3, base_port).await;
    let mut runner = LoadTestRunner::new(
        &format!("CLUSTER: correctness (n={count}, write+verify all nodes)"),
    );

    let mut c0 = Client::connect(&cluster.addrs[0]).await.unwrap();

    runner.start();

    // Write all keys via node 0
    for i in 0..count {
        let key = format!("vc_{i:012}").into_bytes();
        let val = format!("vv_{i:012}").into_bytes();
        let op_start = Instant::now();
        c0.put(&key, &val).await.unwrap();
        runner.record_op(op_start.elapsed());
    }

    // Verify from each node
    let mut errors = 0u64;
    for node_idx in 0..3 {
        let mut client = Client::connect(&cluster.addrs[node_idx]).await.unwrap();
        for i in 0..count {
            let key = format!("vc_{i:012}").into_bytes();
            let expected = format!("vv_{i:012}").into_bytes();
            match client.get(&key).await {
                Ok(Some(v)) if v == expected => {}
                _ => errors += 1,
            }
        }
    }

    let result = runner.finish();
    result.print();
    if errors > 0 {
        println!("    [FAIL] {errors} verification errors across 3 nodes x {count} keys");
    } else {
        println!("    [OK] All {count} keys verified correct on all 3 nodes");
    }
}
