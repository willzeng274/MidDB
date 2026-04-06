use crate::hash_ring::ConsistentHashRing;
use crate::shard::ShardInfo;
use middb_network::ConnectionPool;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Coordinator {
    ring: Arc<RwLock<ConsistentHashRing>>,
    pools: Arc<RwLock<HashMap<String, ConnectionPool>>>,
    pool_size: usize,
}

impl Coordinator {
    pub fn new(pool_size: usize) -> Self {
        Coordinator {
            ring: Arc::new(RwLock::new(ConsistentHashRing::new())),
            pools: Arc::new(RwLock::new(HashMap::new())),
            pool_size,
        }
    }

    pub async fn add_node(&self, addr: &str) {
        let mut ring = self.ring.write().await;
        ring.add_node(addr);
        let mut pools = self.pools.write().await;
        if !pools.contains_key(addr) {
            pools.insert(addr.to_string(), ConnectionPool::new(addr.to_string(), self.pool_size));
        }
    }

    pub async fn remove_node(&self, addr: &str) {
        let mut ring = self.ring.write().await;
        ring.remove_node(addr);
        let mut pools = self.pools.write().await;
        pools.remove(addr);
    }

    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let node = self.route_key(key).await?;
        let pools = self.pools.read().await;
        let pool = pools.get(&node)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool for node"))?;
        let mut conn = pool.get().await?;
        conn.client().get(key).await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let node = self.route_key(key).await?;
        let pools = self.pools.read().await;
        let pool = pools.get(&node)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool for node"))?;
        let mut conn = pool.get().await?;
        conn.client().put(key, value).await
    }

    pub async fn delete(&self, key: &[u8]) -> io::Result<()> {
        let node = self.route_key(key).await?;
        let pools = self.pools.read().await;
        let pool = pools.get(&node)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool for node"))?;
        let mut conn = pool.get().await?;
        conn.client().delete(key).await
    }

    pub async fn scatter_query(&self, sql: &str) -> io::Result<Vec<middb_network::QueryResult>> {
        let ring = self.ring.read().await;
        let nodes: Vec<String> = ring.nodes().to_vec();
        drop(ring);

        let mut handles = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let pools = Arc::clone(&self.pools);
            let node = node.clone();
            let sql = sql.to_string();
            handles.push(tokio::spawn(async move {
                let pools = pools.read().await;
                let pool = pools.get(&node)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool"))?;
                let mut conn = pool.get().await?;
                conn.client().query(&sql).await
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            let result = handle.await
                .map_err(io::Error::other)??;
            results.push(result);
        }
        Ok(results)
    }

    pub async fn batch_put_distributed(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> io::Result<usize> {
        let mut routed: HashMap<String, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();

        for (key, value) in pairs {
            let node = self.route_key(&key).await?;
            routed.entry(node).or_default().push((key, value));
        }

        let mut total = 0;
        let pools = self.pools.read().await;
        for (node, node_pairs) in routed {
            let pool = pools.get(&node)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool"))?;
            let mut conn = pool.get().await?;
            let count = conn.client().batch_put(node_pairs).await?;
            total += count;
        }
        Ok(total)
    }

    pub async fn batch_get_distributed(&self, keys: Vec<Vec<u8>>) -> io::Result<Vec<Option<Vec<u8>>>> {
        let mut routed: HashMap<String, Vec<(usize, Vec<u8>)>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let node = self.route_key(key).await?;
            routed.entry(node).or_default().push((idx, key.clone()));
        }

        let mut results = vec![None; keys.len()];
        let pools = self.pools.read().await;
        for (node, node_keys) in routed {
            let pool = pools.get(&node)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No pool"))?;
            let mut conn = pool.get().await?;
            let batch_keys: Vec<Vec<u8>> = node_keys.iter().map(|(_, k)| k.clone()).collect();
            let values = conn.client().batch_get(batch_keys).await?;
            for ((idx, _), val) in node_keys.iter().zip(values) {
                results[*idx] = val;
            }
        }
        Ok(results)
    }

    pub async fn shard_info(&self) -> Vec<ShardInfo> {
        let ring = self.ring.read().await;
        ring.nodes().iter().map(|node| {
            ShardInfo {
                node_addr: node.clone(),
                status: crate::shard::ShardStatus::Active,
            }
        }).collect()
    }

    async fn route_key(&self, key: &[u8]) -> io::Result<String> {
        let ring = self.ring.read().await;
        ring.get_node(key)
            .map(|s| s.to_string())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No nodes in cluster"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_routing() {
        let coord = Coordinator::new(4);
        coord.add_node("127.0.0.1:8001").await;
        coord.add_node("127.0.0.1:8002").await;

        let info = coord.shard_info().await;
        assert_eq!(info.len(), 2);
    }

    #[tokio::test]
    async fn test_coordinator_no_nodes() {
        let coord = Coordinator::new(4);
        let err = coord.route_key(b"test").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_remove_node() {
        let coord = Coordinator::new(4);
        coord.add_node("127.0.0.1:8001").await;
        coord.add_node("127.0.0.1:8002").await;
        coord.remove_node("127.0.0.1:8001").await;

        let info = coord.shard_info().await;
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].node_addr, "127.0.0.1:8002");
    }
}
