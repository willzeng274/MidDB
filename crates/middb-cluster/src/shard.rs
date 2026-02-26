use crate::hash_ring::ConsistentHashRing;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardStatus {
    Active,
    Rebalancing,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub node_addr: String,
    pub status: ShardStatus,
}

#[derive(Debug)]
pub struct RebalancePlan {
    pub migrations: Vec<KeyMigration>,
}

#[derive(Debug)]
pub struct KeyMigration {
    pub key_range_start: Vec<u8>,
    pub key_range_end: Vec<u8>,
    pub from_node: String,
    pub to_node: String,
}

pub fn compute_rebalance_plan(
    old_ring: &ConsistentHashRing,
    new_ring: &ConsistentHashRing,
    sample_keys: &[Vec<u8>],
) -> RebalancePlan {
    let mut migrations: HashMap<(String, String), Vec<&Vec<u8>>> = HashMap::new();

    for key in sample_keys {
        let old_node = old_ring.get_node(key).map(|s| s.to_string());
        let new_node = new_ring.get_node(key).map(|s| s.to_string());

        if let (Some(old), Some(new)) = (old_node, new_node) {
            if old != new {
                migrations.entry((old, new)).or_default().push(key);
            }
        }
    }

    let migration_plans: Vec<KeyMigration> = migrations
        .into_iter()
        .map(|((from, to), keys)| {
            let start = keys.iter().min().cloned().cloned().unwrap_or_default();
            let end = keys.iter().max().cloned().cloned().unwrap_or_default();
            KeyMigration {
                key_range_start: start,
                key_range_end: end,
                from_node: from,
                to_node: to,
            }
        })
        .collect();

    RebalancePlan { migrations: migration_plans }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_plan_on_add() {
        let mut old_ring = ConsistentHashRing::new();
        old_ring.add_node("node1");
        old_ring.add_node("node2");

        let mut new_ring = ConsistentHashRing::new();
        new_ring.add_node("node1");
        new_ring.add_node("node2");
        new_ring.add_node("node3");

        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key_{}", i).into_bytes()).collect();
        let plan = compute_rebalance_plan(&old_ring, &new_ring, &keys);

        assert!(!plan.migrations.is_empty());
        for m in &plan.migrations {
            assert_eq!(m.to_node, "node3");
        }
    }

    #[test]
    fn test_no_rebalance_when_no_change() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node("node1");
        ring.add_node("node2");

        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key_{}", i).into_bytes()).collect();
        let plan = compute_rebalance_plan(&ring, &ring, &keys);

        assert!(plan.migrations.is_empty());
    }

    #[test]
    fn test_rebalance_plan_on_remove() {
        let mut old_ring = ConsistentHashRing::new();
        old_ring.add_node("node1");
        old_ring.add_node("node2");
        old_ring.add_node("node3");

        let mut new_ring = ConsistentHashRing::new();
        new_ring.add_node("node1");
        new_ring.add_node("node2");

        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key_{}", i).into_bytes()).collect();
        let plan = compute_rebalance_plan(&old_ring, &new_ring, &keys);

        assert!(!plan.migrations.is_empty());
        for m in &plan.migrations {
            assert_eq!(m.from_node, "node3");
        }
    }
}
