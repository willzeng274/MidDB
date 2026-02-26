use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

const DEFAULT_VIRTUAL_NODES: u32 = 150;

#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    ring: BTreeMap<u64, String>,
    virtual_nodes: u32,
    nodes: Vec<String>,
}

impl ConsistentHashRing {
    pub fn new() -> Self {
        ConsistentHashRing {
            ring: BTreeMap::new(),
            virtual_nodes: DEFAULT_VIRTUAL_NODES,
            nodes: Vec::new(),
        }
    }

    pub fn with_virtual_nodes(virtual_nodes: u32) -> Self {
        ConsistentHashRing {
            ring: BTreeMap::new(),
            virtual_nodes,
            nodes: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: &str) {
        if self.nodes.contains(&node.to_string()) {
            return;
        }
        self.nodes.push(node.to_string());
        for i in 0..self.virtual_nodes {
            let hash = Self::hash_key(&format!("{}:{}", node, i));
            self.ring.insert(hash, node.to_string());
        }
    }

    pub fn remove_node(&mut self, node: &str) {
        self.nodes.retain(|n| n != node);
        for i in 0..self.virtual_nodes {
            let hash = Self::hash_key(&format!("{}:{}", node, i));
            self.ring.remove(&hash);
        }
    }

    pub fn get_node(&self, key: &[u8]) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = Self::hash_bytes(key);
        let node = self.ring.range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, v)| v.as_str());
        node
    }

    pub fn get_nodes_for_replication(&self, key: &[u8], replicas: usize) -> Vec<&str> {
        if self.ring.is_empty() {
            return vec![];
        }
        let hash = Self::hash_bytes(key);
        let mut result = Vec::with_capacity(replicas);
        let mut seen = std::collections::HashSet::new();

        let iter = self.ring.range(hash..)
            .chain(self.ring.iter());

        for (_, node) in iter {
            if seen.insert(node.as_str()) {
                result.push(node.as_str());
                if result.len() == replicas {
                    break;
                }
            }
        }
        result
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn get_key_ranges(&self) -> Vec<(String, u64, u64)> {
        if self.nodes.is_empty() {
            return vec![];
        }

        let mut ranges: std::collections::HashMap<String, (u64, u64)> = std::collections::HashMap::new();
        let positions: Vec<_> = self.ring.iter().collect();

        for (i, (hash, node)) in positions.iter().enumerate() {
            let start = if i == 0 {
                positions.last().map(|(h, _)| *h + 1).unwrap_or(0)
            } else {
                positions[i - 1].0 + 1
            };
            ranges.entry(node.to_string())
                .and_modify(|(s, e)| {
                    if start < *s { *s = start; }
                    if **hash > *e { *e = **hash; }
                })
                .or_insert((start, **hash));
        }

        ranges.into_iter().map(|(node, (s, e))| (node, s, e)).collect()
    }

    fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn hash_bytes(key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for ConsistentHashRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_route() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        assert_eq!(ring.node_count(), 3);

        let node = ring.get_node(b"test_key").unwrap();
        assert!(["node1", "node2", "node3"].contains(&node));
    }

    #[test]
    fn test_consistent_routing() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node2");

        let node_a = ring.get_node(b"key_abc").unwrap().to_string();
        let node_b = ring.get_node(b"key_abc").unwrap().to_string();
        assert_eq!(node_a, node_b);
    }

    #[test]
    fn test_remove_node_minimal_disruption() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("key_{}", i).into_bytes()).collect();
        let before: Vec<String> = keys.iter()
            .map(|k| ring.get_node(k).unwrap().to_string())
            .collect();

        ring.remove_node("node3");

        let mut changed = 0;
        for (i, key) in keys.iter().enumerate() {
            let after = ring.get_node(key).unwrap();
            if after != before[i] {
                changed += 1;
            }
        }

        // Removing 1 of 3 nodes should move roughly 1/3 of keys
        assert!(changed < 500, "Too many keys moved: {}", changed);
    }

    #[test]
    fn test_distribution() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        let mut counts = std::collections::HashMap::new();
        for i in 0..3000 {
            let key = format!("key_{}", i);
            let node = ring.get_node(key.as_bytes()).unwrap();
            *counts.entry(node.to_string()).or_insert(0) += 1;
        }

        for (node, count) in &counts {
            let ratio = *count as f64 / 3000.0;
            assert!(ratio > 0.2 && ratio < 0.5,
                "Node {} has {:.1}% of keys (expected ~33%)", node, ratio * 100.0);
        }
    }

    #[test]
    fn test_replication() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        let replicas = ring.get_nodes_for_replication(b"test_key", 2);
        assert_eq!(replicas.len(), 2);
        assert_ne!(replicas[0], replicas[1]);
    }

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new();
        assert!(ring.get_node(b"key").is_none());
        assert!(ring.get_nodes_for_replication(b"key", 3).is_empty());
    }

    #[test]
    fn test_duplicate_add() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node1");
        assert_eq!(ring.node_count(), 1);
    }
}
