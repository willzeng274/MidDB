use middb_cluster::{ClusterConfig, ClusterNode};
use middb_core::{Config, Database};
use middb_network::{Client, RequestHandler, Server};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::JoinHandle;

struct TestCluster {
    nodes: Vec<Arc<ClusterNode>>,
    _dirs: Vec<TempDir>,
    _server_handles: Vec<JoinHandle<()>>,
    addrs: Vec<String>,
}

impl TestCluster {
    async fn start(n: usize, base_port: u16) -> Self {
        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        let mut server_handles = Vec::new();
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
                heartbeat_interval: Duration::from_millis(500),
                quorum_timeout: Duration::from_secs(2),
                pool_size: 4,
            };

            let node = ClusterNode::new(db, addr.clone(), cluster_config);

            if i == 0 {
                node.bootstrap().await;
            }

            node.start_heartbeat();

            // Start server first, then join, so the TCP listener is ready
            let handler = node.clone() as Arc<dyn RequestHandler>;
            let server = Server::with_handler(handler, addr);
            let handle = tokio::spawn(async move {
                let _ = server.run().await;
            });

            // Wait for this node's server to be ready
            Self::wait_for_port(base_port + i as u16).await;

            if i > 0 {
                node.join(&addrs[0]).await.unwrap();
            }

            nodes.push(node);
            dirs.push(dir);
            server_handles.push(handle);
        }

        TestCluster {
            nodes,
            _dirs: dirs,
            _server_handles: server_handles,
            addrs,
        }
    }

    async fn wait_for_port(port: u16) {
        let addr = format!("127.0.0.1:{port}");
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("Port {port} never became ready");
    }

    async fn client(&self, node_idx: usize) -> Client {
        Client::connect(&self.addrs[node_idx]).await.unwrap()
    }
}

// ========== Integration Tests ==========

#[tokio::test]
async fn test_bootstrap_and_join() {
    let cluster = TestCluster::start(3, 30000).await;

    for node in &cluster.nodes {
        assert_eq!(node.node_count().await, 3, "All nodes should see 3 members");
    }

    // Verify all nodes know about each other
    for node in &cluster.nodes {
        let nodes = node.cluster_nodes().await;
        for addr in &cluster.addrs {
            assert!(nodes.contains(addr), "Node should know about {addr}");
        }
    }
}

#[tokio::test]
async fn test_write_to_a_read_from_b() {
    let cluster = TestCluster::start(3, 30010).await;

    // Write via node 0
    let mut c0 = cluster.client(0).await;
    c0.put(b"hello", b"world").await.unwrap();

    // Read from node 1
    let mut c1 = cluster.client(1).await;
    let val = c1.get(b"hello").await.unwrap();
    assert_eq!(val, Some(b"world".to_vec()));

    // Read from node 2
    let mut c2 = cluster.client(2).await;
    let val = c2.get(b"hello").await.unwrap();
    assert_eq!(val, Some(b"world".to_vec()));
}

#[tokio::test]
async fn test_write_quorum_survives_node_failure() {
    let cluster = TestCluster::start(3, 30020).await;

    // Kill node 2's server by aborting its handle
    cluster._server_handles[2].abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write via node 0 — should still succeed with W=2 (node 0 local + node 1 remote)
    let mut c0 = cluster.client(0).await;
    c0.put(b"survive", b"yes").await.unwrap();

    // Read from node 0
    let val = c0.get(b"survive").await.unwrap();
    assert_eq!(val, Some(b"yes".to_vec()));

    // Read from node 1
    let mut c1 = cluster.client(1).await;
    let val = c1.get(b"survive").await.unwrap();
    assert_eq!(val, Some(b"yes".to_vec()));
}

#[tokio::test]
async fn test_delete_propagation() {
    let cluster = TestCluster::start(3, 30040).await;

    // Write
    let mut c0 = cluster.client(0).await;
    c0.put(b"del_me", b"val").await.unwrap();

    // Verify present on all nodes
    for i in 0..3 {
        let mut c = cluster.client(i).await;
        assert_eq!(c.get(b"del_me").await.unwrap(), Some(b"val".to_vec()));
    }

    // Delete
    c0.delete(b"del_me").await.unwrap();

    // Verify gone from all nodes
    for i in 0..3 {
        let mut c = cluster.client(i).await;
        assert_eq!(c.get(b"del_me").await.unwrap(), None, "Node {i} still has deleted key");
    }
}

#[tokio::test]
async fn test_batch_operations_across_shards() {
    let cluster = TestCluster::start(3, 30050).await;

    let mut c0 = cluster.client(0).await;

    // Write 100 key-value pairs
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
        .map(|i| (format!("bk_{i:04}").into_bytes(), format!("bv_{i:04}").into_bytes()))
        .collect();
    c0.batch_put(pairs.clone()).await.unwrap();

    // Read all back from node 1
    let mut c1 = cluster.client(1).await;
    let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
    let results = c1.batch_get(keys).await.unwrap();

    for (i, val) in results.iter().enumerate() {
        assert_eq!(val, &Some(pairs[i].1.clone()), "Mismatch at key {i}");
    }
}

#[tokio::test]
async fn test_concurrent_writes_different_nodes() {
    let cluster = TestCluster::start(3, 30060).await;

    let addrs = cluster.addrs.clone();
    let mut handles = Vec::new();

    for (t, addr) in addrs.iter().enumerate() {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr).await.unwrap();
            for i in 0..50 {
                let key = format!("t{t}_{i:04}").into_bytes();
                let val = format!("v{t}_{i:04}").into_bytes();
                client.put(&key, &val).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Read all 150 keys from node 0
    let mut c = cluster.client(0).await;
    for t in 0..3 {
        for i in 0..50 {
            let key = format!("t{t}_{i:04}").into_bytes();
            let expected = format!("v{t}_{i:04}").into_bytes();
            let val = c.get(&key).await.unwrap();
            assert_eq!(val, Some(expected), "Missing key t{t}_{i}");
        }
    }
}

// ========== Correctness Tests ==========

#[tokio::test]
async fn test_write_n_read_all_from_every_node() {
    let cluster = TestCluster::start(3, 30070).await;

    // Write 200 keys via node 0
    let mut c0 = cluster.client(0).await;
    for i in 0..200 {
        let key = format!("ck_{i:04}").into_bytes();
        let val = format!("cv_{i:04}").into_bytes();
        c0.put(&key, &val).await.unwrap();
    }

    // Read all 200 from every node
    for n in 0..3 {
        let mut c = cluster.client(n).await;
        for i in 0..200 {
            let key = format!("ck_{i:04}").into_bytes();
            let expected = format!("cv_{i:04}").into_bytes();
            let val = c.get(&key).await.unwrap();
            assert_eq!(val, Some(expected), "Node {n} missing key ck_{i:04}");
        }
    }
}

#[tokio::test]
async fn test_overwrite_latest_value_wins() {
    let cluster = TestCluster::start(3, 30080).await;

    let mut c0 = cluster.client(0).await;

    c0.put(b"ow_key", b"v1").await.unwrap();
    c0.put(b"ow_key", b"v2").await.unwrap();
    c0.put(b"ow_key", b"v3").await.unwrap();

    // All nodes should return v3
    for n in 0..3 {
        let mut c = cluster.client(n).await;
        let val = c.get(b"ow_key").await.unwrap();
        assert_eq!(val, Some(b"v3".to_vec()), "Node {n} has stale value");
    }
}

#[tokio::test]
async fn test_delete_verified_on_all_replicas() {
    let cluster = TestCluster::start(3, 30090).await;

    let mut c0 = cluster.client(0).await;

    // Write 50 keys
    for i in 0..50 {
        let key = format!("dk_{i:04}").into_bytes();
        c0.put(&key, b"present").await.unwrap();
    }

    // Delete the first 25
    for i in 0..25 {
        let key = format!("dk_{i:04}").into_bytes();
        c0.delete(&key).await.unwrap();
    }

    // Verify on all nodes
    for n in 0..3 {
        let mut c = cluster.client(n).await;
        for i in 0..50 {
            let key = format!("dk_{i:04}").into_bytes();
            let val = c.get(&key).await.unwrap();
            if i < 25 {
                assert_eq!(val, None, "Node {n} still has deleted key dk_{i:04}");
            } else {
                assert_eq!(val, Some(b"present".to_vec()), "Node {n} missing key dk_{i:04}");
            }
        }
    }
}

#[tokio::test]
async fn test_replication_to_local_db() {
    let cluster = TestCluster::start(3, 30100).await;

    // Write via coordinated path
    let mut c0 = cluster.client(0).await;
    for i in 0..50 {
        let key = format!("rep_{i:04}").into_bytes();
        let val = format!("val_{i:04}").into_bytes();
        c0.put(&key, &val).await.unwrap();
    }

    // With RF=3 and 3 nodes, every key should be on every node's local DB
    for (n, node) in cluster.nodes.iter().enumerate() {
        let mut found = 0;
        for i in 0..50 {
            let key = format!("rep_{i:04}").into_bytes();
            if node.local_db.get(&key).unwrap().is_some() {
                found += 1;
            }
        }
        assert_eq!(found, 50, "Node {n} only has {found}/50 keys in local DB");
    }
}

#[tokio::test]
async fn test_single_node_cluster() {
    let cluster = TestCluster::start(1, 30110).await;

    let mut c = cluster.client(0).await;
    c.put(b"solo", b"node").await.unwrap();
    let val = c.get(b"solo").await.unwrap();
    assert_eq!(val, Some(b"node".to_vec()));

    c.delete(b"solo").await.unwrap();
    let val = c.get(b"solo").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_ping_through_cluster() {
    let cluster = TestCluster::start(3, 30120).await;

    for i in 0..3 {
        let mut c = cluster.client(i).await;
        c.ping().await.unwrap();
    }
}
