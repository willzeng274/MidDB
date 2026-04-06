use crate::hash_ring::ConsistentHashRing;
use crate::membership::{MembershipManager, NodeStatus};
use middb_core::Database;
use middb_network::{Client, ConnectionPool, Request, RequestHandler, Response};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct ClusterConfig {
    pub replication_factor: usize,
    pub write_quorum: usize,
    pub read_quorum: usize,
    pub heartbeat_interval: Duration,
    pub quorum_timeout: Duration,
    pub pool_size: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            replication_factor: 3,
            write_quorum: 2,
            read_quorum: 1,
            heartbeat_interval: Duration::from_secs(2),
            quorum_timeout: Duration::from_millis(500),
            pool_size: 8,
        }
    }
}

pub struct ClusterNode {
    pub local_db: Arc<Database>,
    ring: Arc<RwLock<ConsistentHashRing>>,
    membership: Arc<MembershipManager>,
    pools: Arc<RwLock<HashMap<String, ConnectionPool>>>,
    self_addr: String,
    cluster_config: ClusterConfig,
    ring_version: AtomicU64,
}

impl ClusterNode {
    pub fn new(
        db: Database,
        self_addr: String,
        cluster_config: ClusterConfig,
    ) -> Arc<Self> {
        Arc::new(ClusterNode {
            local_db: Arc::new(db),
            ring: Arc::new(RwLock::new(ConsistentHashRing::new())),
            membership: Arc::new(MembershipManager::with_defaults()),
            pools: Arc::new(RwLock::new(HashMap::new())),
            self_addr,
            cluster_config,
            ring_version: AtomicU64::new(0),
        })
    }

    /// Bootstrap as the first node in a new cluster.
    pub async fn bootstrap(self: &Arc<Self>) {
        let mut ring = self.ring.write().await;
        ring.add_node(&self.self_addr);
        self.membership.register_node(&self.self_addr).await;
        self.ring_version.fetch_add(1, Ordering::SeqCst);
        eprintln!("[cluster] bootstrapped as {}", self.self_addr);
    }

    /// Join an existing cluster by contacting a seed node.
    pub async fn join(self: &Arc<Self>, seed_addr: &str) -> io::Result<()> {
        let mut client = Client::connect(seed_addr).await?;
        let (nodes, ring_version) = client.join_cluster(&self.self_addr).await?;

        let mut ring = self.ring.write().await;
        for node in &nodes {
            ring.add_node(node);
        }
        ring.add_node(&self.self_addr);
        drop(ring);

        self.ring_version.store(ring_version + 1, Ordering::SeqCst);

        // Open connection pools to all peers
        let mut pools = self.pools.write().await;
        for node in &nodes {
            if node != &self.self_addr && !pools.contains_key(node) {
                pools.insert(
                    node.clone(),
                    ConnectionPool::new(node.clone(), self.cluster_config.pool_size),
                );
            }
        }
        drop(pools);

        // Register all nodes in membership
        for node in &nodes {
            self.membership.register_node(node).await;
        }
        self.membership.register_node(&self.self_addr).await;

        // Notify all other peers about our existence. The seed already knows
        // (it handled our JoinCluster), but other nodes don't.
        for node in &nodes {
            if node == seed_addr || node == &self.self_addr {
                continue;
            }
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(node) {
                if let Ok(mut conn) = pool.get().await {
                    if let Err(e) = conn.client().join_cluster(&self.self_addr).await {
                        eprintln!("[cluster] failed to notify {} about join: {}", node, e);
                    }
                }
            }
        }

        eprintln!("[cluster] joined via {}, cluster has {} nodes", seed_addr, nodes.len() + 1);
        Ok(())
    }

    /// Handle a JoinCluster request from a new node.
    async fn handle_join(&self, new_addr: &str) -> Response {
        // Add to ring
        {
            let mut ring = self.ring.write().await;
            ring.add_node(new_addr);
        }
        self.ring_version.fetch_add(1, Ordering::SeqCst);

        // Add connection pool
        {
            let mut pools = self.pools.write().await;
            if !pools.contains_key(new_addr) {
                pools.insert(
                    new_addr.to_string(),
                    ConnectionPool::new(new_addr.to_string(), self.cluster_config.pool_size),
                );
            }
        }

        self.membership.register_node(new_addr).await;

        // Return current cluster state
        let ring = self.ring.read().await;
        let nodes = ring.nodes().to_vec();
        let version = self.ring_version.load(Ordering::SeqCst);

        eprintln!("[cluster] node {} joined, cluster now has {} nodes", new_addr, nodes.len());

        Response::ClusterState {
            nodes,
            ring_version: version,
        }
    }

    fn is_local(&self, addr: &str) -> bool {
        addr == self.self_addr
    }

    /// Coordinated PUT: fan out to W replicas, wait for quorum.
    pub async fn coordinated_put(&self, key: Vec<u8>, value: Vec<u8>) -> Response {
        let replicas = {
            let ring = self.ring.read().await;
            ring.get_nodes_for_replication(&key, self.cluster_config.replication_factor)
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        if replicas.is_empty() {
            return Response::Error("No nodes available".to_string());
        }

        let mut handles = Vec::with_capacity(replicas.len());

        for node in &replicas {
            if self.is_local(node) {
                let key = key.clone();
                let value = value.clone();
                let db = Arc::clone(&self.local_db);
                handles.push(tokio::spawn(async move {
                    db.put(key, value).map_err(|e| io::Error::other(e.to_string()))
                }));
            } else {
                let pools = Arc::clone(&self.pools);
                let node = node.clone();
                let key = key.clone();
                let value = value.clone();
                handles.push(tokio::spawn(async move {
                    let pools = pools.read().await;
                    let pool = pools.get(&node)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool"))?;
                    let mut conn = pool.get().await?;
                    conn.client().replicate_write(&key, &value).await
                }));
            }
        }

        let result = tokio::time::timeout(
            self.cluster_config.quorum_timeout,
            wait_for_quorum(&mut handles, self.cluster_config.write_quorum),
        ).await;

        // Abort any remaining tasks to prevent resource leaks
        for h in handles { h.abort(); }

        match result {
            Ok(Ok(())) => Response::Ok,
            Ok(Err(e)) => Response::Error(format!("Quorum not met: {e}")),
            Err(_) => Response::Error("Write quorum timeout".to_string()),
        }
    }

    /// Coordinated DELETE: fan out to W replicas, wait for quorum.
    pub async fn coordinated_delete(&self, key: Vec<u8>) -> Response {
        let replicas = {
            let ring = self.ring.read().await;
            ring.get_nodes_for_replication(&key, self.cluster_config.replication_factor)
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        if replicas.is_empty() {
            return Response::Error("No nodes available".to_string());
        }

        let mut handles = Vec::with_capacity(replicas.len());

        for node in &replicas {
            if self.is_local(node) {
                let key = key.clone();
                let db = Arc::clone(&self.local_db);
                handles.push(tokio::spawn(async move {
                    db.delete(key).map_err(|e| io::Error::other(e.to_string()))
                }));
            } else {
                let pools = Arc::clone(&self.pools);
                let node = node.clone();
                let key = key.clone();
                handles.push(tokio::spawn(async move {
                    let pools = pools.read().await;
                    let pool = pools.get(&node)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool"))?;
                    let mut conn = pool.get().await?;
                    conn.client().replicate_delete(&key).await
                }));
            }
        }

        let result = tokio::time::timeout(
            self.cluster_config.quorum_timeout,
            wait_for_quorum(&mut handles, self.cluster_config.write_quorum),
        ).await;

        for h in handles { h.abort(); }

        match result {
            Ok(Ok(())) => Response::Ok,
            Ok(Err(e)) => Response::Error(format!("Quorum not met: {e}")),
            Err(_) => Response::Error("Delete quorum timeout".to_string()),
        }
    }

    /// Coordinated GET: R=1 by default (read from preferred replica).
    pub async fn coordinated_get(&self, key: Vec<u8>) -> Response {
        let replicas = {
            let ring = self.ring.read().await;
            ring.get_nodes_for_replication(&key, self.cluster_config.replication_factor)
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        // Try local first if we're a replica
        for node in &replicas {
            if self.is_local(node) {
                return match self.local_db.get(&key) {
                    Ok(value) => Response::Value(value),
                    Err(e) => Response::Error(e.to_string()),
                };
            }
        }

        // Not a local replica — ask the primary
        if let Some(primary) = replicas.first() {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(primary) {
                match pool.get().await {
                    Ok(mut conn) => {
                        match conn.client().get(&key).await {
                            Ok(value) => return Response::Value(value),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Err(e) => return Response::Error(e.to_string()),
                }
            }
        }

        Response::Error("No nodes available for read".to_string())
    }

    /// Coordinated BatchPut: fan out in chunks to avoid pool exhaustion.
    pub async fn coordinated_batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Response {
        let count = pairs.len();
        let chunk_size = self.cluster_config.pool_size.max(1);
        for chunk in pairs.chunks(chunk_size) {
            let futs: Vec<_> = chunk.iter()
                .map(|(key, value)| self.coordinated_put(key.clone(), value.clone()))
                .collect();
            let results = futures::future::join_all(futs).await;
            for r in &results {
                if let Response::Error(e) = r {
                    return Response::Error(e.clone());
                }
            }
        }
        Response::BatchOk { count }
    }

    /// Coordinated BatchGet: fan out in chunks.
    pub async fn coordinated_batch_get(&self, keys: Vec<Vec<u8>>) -> Response {
        let mut values = Vec::with_capacity(keys.len());
        let chunk_size = self.cluster_config.pool_size.max(1);
        for chunk in keys.chunks(chunk_size) {
            let futs: Vec<_> = chunk.iter()
                .map(|key| self.coordinated_get(key.clone()))
                .collect();
            let results = futures::future::join_all(futs).await;
            for r in results {
                match r {
                    Response::Value(v) => values.push(v),
                    _ => values.push(None),
                }
            }
        }
        Response::Values(values)
    }

    /// Start background heartbeat loop.
    pub fn start_heartbeat(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(node.cluster_config.heartbeat_interval);
            loop {
                interval.tick().await;

                let peers: Vec<String> = {
                    let ring = node.ring.read().await;
                    ring.nodes().iter()
                        .filter(|n| n.as_str() != node.self_addr)
                        .cloned()
                        .collect()
                };

                let ring_version = node.ring_version.load(Ordering::SeqCst);

                for peer in &peers {
                    let pools = node.pools.read().await;
                    if let Some(pool) = pools.get(peer) {
                        if let Ok(mut conn) = pool.get().await {
                            if conn.client().heartbeat(&node.self_addr, ring_version).await.is_ok() {
                                node.membership.record_heartbeat(peer).await;
                            }
                        }
                    }
                }

                // Check health and remove dead nodes
                let changes = node.membership.check_health().await;
                for (addr, status) in changes {
                    if status == NodeStatus::Dead {
                        eprintln!("[cluster] node {addr} declared dead, removing");
                        let mut ring = node.ring.write().await;
                        ring.remove_node(&addr);
                        let mut pools = node.pools.write().await;
                        pools.remove(&addr);
                        node.ring_version.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        })
    }

    pub fn self_addr(&self) -> &str {
        &self.self_addr
    }

    pub async fn node_count(&self) -> usize {
        let ring = self.ring.read().await;
        ring.node_count()
    }

    pub async fn cluster_nodes(&self) -> Vec<String> {
        let ring = self.ring.read().await;
        ring.nodes().to_vec()
    }

    pub fn ring_version(&self) -> u64 {
        self.ring_version.load(Ordering::SeqCst)
    }
}

/// RequestHandler implementation — dispatches client requests through
/// coordinated paths and handles cluster-internal messages locally.
impl RequestHandler for ClusterNode {
    fn handle(&self, request: Request) -> Pin<Box<dyn Future<Output = Response> + Send + '_>> {
        Box::pin(async move {
            match request {
                // Client-facing: route through quorum
                Request::Get { key } => self.coordinated_get(key).await,
                Request::Put { key, value } => self.coordinated_put(key, value).await,
                Request::Delete { key } => self.coordinated_delete(key).await,
                Request::BatchGet { keys } => self.coordinated_batch_get(keys).await,
                Request::BatchPut { pairs } => self.coordinated_batch_put(pairs).await,

                // Cluster-internal: write directly to local DB
                Request::ReplicateWrite { key, value } => {
                    match self.local_db.put(key, value) {
                        Ok(()) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::ReplicateDelete { key } => {
                    match self.local_db.delete(key) {
                        Ok(()) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }

                // Cluster membership
                Request::Heartbeat { node_id, .. } => {
                    self.membership.record_heartbeat(&node_id).await;
                    Response::HeartbeatAck
                }
                Request::JoinCluster { node_addr } => {
                    self.handle_join(&node_addr).await
                }
                Request::GetClusterState => {
                    let ring = self.ring.read().await;
                    let nodes = ring.nodes().to_vec();
                    Response::ClusterState {
                        nodes,
                        ring_version: self.ring_version.load(Ordering::SeqCst),
                    }
                }

                // Local operations (transactions, queries, ping)
                Request::Query { sql } => {
                    use middb_network::server::handle_request;
                    handle_request(&self.local_db, Request::Query { sql })
                }
                Request::BeginTxn => {
                    let txn_id = self.local_db.begin_txn();
                    Response::TxnStarted { txn_id }
                }
                Request::CommitTxn { txn_id } => {
                    match self.local_db.commit_txn(txn_id) {
                        Ok(()) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::AbortTxn { txn_id } => {
                    match self.local_db.abort_txn(txn_id) {
                        Ok(()) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::TxnGet { txn_id, key } => {
                    match self.local_db.get_txn(txn_id, &key) {
                        Ok(value) => Response::Value(value),
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::TxnPut { txn_id, key, value } => {
                    match self.local_db.put_txn(txn_id, key, value) {
                        Ok(()) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::TxnDelete { txn_id, key } => {
                    match self.local_db.delete_txn(txn_id, key) {
                        Ok(()) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::Ping => Response::Pong,
            }
        })
    }
}

/// Wait for `quorum` out of `handles` to succeed.
/// Drains completed handles from the vec; remaining handles can be aborted by caller.
async fn wait_for_quorum(
    handles: &mut Vec<tokio::task::JoinHandle<io::Result<()>>>,
    quorum: usize,
) -> io::Result<()> {
    let mut successes = 0;
    let mut last_err = None;

    // Drain handles one by one, collecting results
    let owned: Vec<_> = std::mem::take(handles);
    let mut remaining = Vec::new();

    for handle in owned {
        if successes >= quorum {
            remaining.push(handle);
            continue;
        }
        match handle.await {
            Ok(Ok(())) => {
                successes += 1;
            }
            Ok(Err(e)) => last_err = Some(e),
            Err(e) => last_err = Some(io::Error::other(e.to_string())),
        }
    }

    *handles = remaining;

    if successes >= quorum {
        return Ok(());
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::other(
            format!("Only {successes}/{quorum} replicas succeeded"),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use middb_core::Config;
    use tempfile::TempDir;

    fn make_node(addr: &str) -> (Arc<ClusterNode>, TempDir) {
        let dir = TempDir::new().unwrap();
        let mut config = Config::new(dir.path());
        config.sync_writes = false;
        let db = Database::open(config).unwrap();
        let mut cluster_config = ClusterConfig::default();
        // Single node: quorum of 1
        cluster_config.replication_factor = 1;
        cluster_config.write_quorum = 1;
        cluster_config.read_quorum = 1;
        let node = ClusterNode::new(db, addr.to_string(), cluster_config);
        (node, dir)
    }

    #[tokio::test]
    async fn test_bootstrap_and_local_ops() {
        let (node, _dir) = make_node("127.0.0.1:9001");
        node.bootstrap().await;

        assert_eq!(node.node_count().await, 1);

        // Write through coordinated path
        let resp = node.coordinated_put(b"key1".to_vec(), b"val1".to_vec()).await;
        assert!(matches!(resp, Response::Ok));

        // Read through coordinated path
        let resp = node.coordinated_get(b"key1".to_vec()).await;
        match resp {
            Response::Value(Some(v)) => assert_eq!(v, b"val1"),
            other => panic!("Expected Value, got {other:?}"),
        }

        // Delete
        let resp = node.coordinated_delete(b"key1".to_vec()).await;
        assert!(matches!(resp, Response::Ok));

        let resp = node.coordinated_get(b"key1".to_vec()).await;
        assert!(matches!(resp, Response::Value(None)));
    }

    #[tokio::test]
    async fn test_request_handler() {
        let (node, _dir) = make_node("127.0.0.1:9002");
        node.bootstrap().await;

        let resp = node.handle(Request::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }).await;
        assert!(matches!(resp, Response::Ok));

        let resp = node.handle(Request::Get { key: b"k".to_vec() }).await;
        match resp {
            Response::Value(Some(v)) => assert_eq!(v, b"v"),
            other => panic!("Expected Value, got {other:?}"),
        }
    }
}
