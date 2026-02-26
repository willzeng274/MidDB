use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Clone)]
pub struct NodeState {
    pub addr: String,
    pub status: NodeStatus,
    pub last_heartbeat: Instant,
}

pub struct MembershipManager {
    nodes: Arc<RwLock<HashMap<String, NodeState>>>,
    heartbeat_timeout: Duration,
    suspect_timeout: Duration,
}

impl MembershipManager {
    pub fn new(heartbeat_timeout: Duration, suspect_timeout: Duration) -> Self {
        MembershipManager {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout,
            suspect_timeout,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(Duration::from_secs(5), Duration::from_secs(15))
    }

    pub async fn register_node(&self, addr: &str) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(addr.to_string(), NodeState {
            addr: addr.to_string(),
            status: NodeStatus::Alive,
            last_heartbeat: Instant::now(),
        });
    }

    pub async fn record_heartbeat(&self, addr: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(addr) {
            node.last_heartbeat = Instant::now();
            node.status = NodeStatus::Alive;
        }
    }

    pub async fn check_health(&self) -> Vec<(String, NodeStatus)> {
        let mut nodes = self.nodes.write().await;
        let now = Instant::now();
        let mut changes = Vec::new();

        for (addr, node) in nodes.iter_mut() {
            let elapsed = now.duration_since(node.last_heartbeat);
            let new_status = if elapsed > self.suspect_timeout {
                NodeStatus::Dead
            } else if elapsed > self.heartbeat_timeout {
                NodeStatus::Suspect
            } else {
                NodeStatus::Alive
            };

            if new_status != node.status {
                node.status = new_status.clone();
                changes.push((addr.clone(), new_status));
            }
        }

        changes
    }

    pub async fn alive_nodes(&self) -> Vec<String> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|n| n.status == NodeStatus::Alive)
            .map(|n| n.addr.clone())
            .collect()
    }

    pub async fn all_nodes(&self) -> Vec<NodeState> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    pub async fn remove_dead_nodes(&self) -> Vec<String> {
        let mut nodes = self.nodes.write().await;
        let dead: Vec<String> = nodes.iter()
            .filter(|(_, n)| n.status == NodeStatus::Dead)
            .map(|(k, _)| k.clone())
            .collect();
        for addr in &dead {
            nodes.remove(addr);
        }
        dead
    }

    pub async fn run_health_checker(
        self: Arc<Self>,
        interval: Duration,
        mut on_change: impl FnMut(String, NodeStatus) + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let changes = self.check_health().await;
                for (addr, status) in changes {
                    on_change(addr, status);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_heartbeat() {
        let mgr = MembershipManager::with_defaults();
        mgr.register_node("127.0.0.1:8001").await;
        mgr.register_node("127.0.0.1:8002").await;

        let alive = mgr.alive_nodes().await;
        assert_eq!(alive.len(), 2);
    }

    #[tokio::test]
    async fn test_health_check_timeout() {
        let mgr = MembershipManager::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
        );
        mgr.register_node("127.0.0.1:8001").await;

        tokio::time::sleep(Duration::from_millis(60)).await;
        let changes = mgr.check_health().await;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1, NodeStatus::Suspect);

        tokio::time::sleep(Duration::from_millis(60)).await;
        let changes = mgr.check_health().await;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1, NodeStatus::Dead);
    }

    #[tokio::test]
    async fn test_remove_dead_nodes() {
        let mgr = MembershipManager::new(
            Duration::from_millis(10),
            Duration::from_millis(20),
        );
        mgr.register_node("127.0.0.1:8001").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        mgr.check_health().await;

        let removed = mgr.remove_dead_nodes().await;
        assert_eq!(removed.len(), 1);
        assert_eq!(mgr.alive_nodes().await.len(), 0);
    }

    #[tokio::test]
    async fn test_heartbeat_recovery() {
        let mgr = MembershipManager::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
        );
        mgr.register_node("127.0.0.1:8001").await;

        tokio::time::sleep(Duration::from_millis(60)).await;
        mgr.check_health().await;

        mgr.record_heartbeat("127.0.0.1:8001").await;
        let alive = mgr.alive_nodes().await;
        assert_eq!(alive.len(), 1);
    }
}
