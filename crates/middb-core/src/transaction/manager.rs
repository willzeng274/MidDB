use crate::{Key, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub type TxnId = u64;
pub type Version = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub enum WriteOp {
    Put(Value),
    Delete,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: TxnId,
    pub start_version: Version,
    pub status: TxnStatus,
    pub read_set: HashSet<Key>,
    pub write_set: HashMap<Key, WriteOp>,
}

impl Transaction {
    pub fn new(id: TxnId, start_version: Version) -> Self {
        Transaction {
            id,
            start_version,
            status: TxnStatus::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
        }
    }

    pub fn record_read(&mut self, key: Key) {
        self.read_set.insert(key);
    }

    pub fn record_put(&mut self, key: Key, value: Value) {
        self.write_set.insert(key, WriteOp::Put(value));
    }

    pub fn record_delete(&mut self, key: Key) {
        self.write_set.insert(key, WriteOp::Delete);
    }

    pub fn get_local(&self, key: &Key) -> Option<&WriteOp> {
        self.write_set.get(key)
    }

    pub fn is_active(&self) -> bool {
        self.status == TxnStatus::Active
    }

    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }
}

#[derive(Debug, Clone)]
struct CommittedWrite {
    version: Version,
    value: Option<Value>,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    current_version: AtomicU64,
    active_txns: RwLock<HashMap<TxnId, Transaction>>,
    committed_versions: RwLock<HashMap<Key, Vec<CommittedWrite>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            current_version: AtomicU64::new(0),
            active_txns: RwLock::new(HashMap::new()),
            committed_versions: RwLock::new(HashMap::new()),
        }
    }

    pub fn begin(&self) -> TxnId {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_version = self.current_version.load(Ordering::SeqCst);

        let txn = Transaction::new(txn_id, start_version);

        let mut active = self.active_txns.write().unwrap();
        active.insert(txn_id, txn);

        txn_id
    }

    pub fn record_read(&self, txn_id: TxnId, key: Key) -> Result<(), TxnError> {
        let mut active = self.active_txns.write().unwrap();
        let txn = active.get_mut(&txn_id).ok_or(TxnError::TxnNotFound(txn_id))?;

        if !txn.is_active() {
            return Err(TxnError::TxnNotActive(txn_id));
        }

        txn.record_read(key);
        Ok(())
    }

    pub fn record_write(&self, txn_id: TxnId, key: Key, value: Option<Value>) -> Result<(), TxnError> {
        let mut active = self.active_txns.write().unwrap();
        let txn = active.get_mut(&txn_id).ok_or(TxnError::TxnNotFound(txn_id))?;

        if !txn.is_active() {
            return Err(TxnError::TxnNotActive(txn_id));
        }

        match value {
            Some(v) => txn.record_put(key, v),
            None => txn.record_delete(key),
        }
        Ok(())
    }

    pub fn get_local(&self, txn_id: TxnId, key: &Key) -> Result<Option<WriteOp>, TxnError> {
        let active = self.active_txns.read().unwrap();
        let txn = active.get(&txn_id).ok_or(TxnError::TxnNotFound(txn_id))?;
        Ok(txn.get_local(key).cloned())
    }

    pub fn get_start_version(&self, txn_id: TxnId) -> Result<Version, TxnError> {
        let active = self.active_txns.read().unwrap();
        let txn = active.get(&txn_id).ok_or(TxnError::TxnNotFound(txn_id))?;
        Ok(txn.start_version)
    }

    pub fn commit(&self, txn_id: TxnId) -> Result<(Version, Vec<(Key, WriteOp)>), TxnError> {
        let txn = {
            let mut active = self.active_txns.write().unwrap();
            active.remove(&txn_id).ok_or(TxnError::TxnNotFound(txn_id))?
        };

        if !txn.is_active() {
            return Err(TxnError::TxnNotActive(txn_id));
        }

        self.check_conflicts(&txn)?;

        let commit_version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;

        let writes: Vec<(Key, WriteOp)> = txn.write_set.into_iter().collect();

        {
            let mut committed = self.committed_versions.write().unwrap();
            for (key, op) in &writes {
                let value = match op {
                    WriteOp::Put(v) => Some(v.clone()),
                    WriteOp::Delete => None,
                };

                let write = CommittedWrite {
                    version: commit_version,
                    value,
                };

                committed
                    .entry(key.clone())
                    .or_insert_with(Vec::new)
                    .push(write);
            }
        }

        Ok((commit_version, writes))
    }

    pub fn abort(&self, txn_id: TxnId) -> Result<(), TxnError> {
        let mut active = self.active_txns.write().unwrap();
        active.remove(&txn_id).ok_or(TxnError::TxnNotFound(txn_id))?;
        Ok(())
    }

    fn check_conflicts(&self, txn: &Transaction) -> Result<(), TxnError> {
        let committed = self.committed_versions.read().unwrap();

        for key in &txn.read_set {
            if let Some(versions) = committed.get(key) {
                for write in versions {
                    if write.version > txn.start_version {
                        return Err(TxnError::Conflict(key.clone()));
                    }
                }
            }
        }

        for key in txn.write_set.keys() {
            if let Some(versions) = committed.get(key) {
                for write in versions {
                    if write.version > txn.start_version {
                        return Err(TxnError::Conflict(key.clone()));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn get_visible_value(&self, key: &Key, start_version: Version) -> Option<Value> {
        let committed = self.committed_versions.read().unwrap();

        if let Some(versions) = committed.get(key) {
            let mut latest: Option<&CommittedWrite> = None;

            for write in versions {
                if write.version <= start_version {
                    if latest.is_none() || write.version > latest.unwrap().version {
                        latest = Some(write);
                    }
                }
            }

            if let Some(w) = latest {
                return w.value.clone();
            }
        }

        None
    }

    pub fn active_count(&self) -> usize {
        self.active_txns.read().unwrap().len()
    }

    pub fn current_version(&self) -> Version {
        self.current_version.load(Ordering::SeqCst)
    }

    pub fn gc(&self, min_version: Version) {
        let mut committed = self.committed_versions.write().unwrap();

        for versions in committed.values_mut() {
            versions.retain(|w| w.version >= min_version);
        }

        committed.retain(|_, versions| !versions.is_empty());
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TxnError {
    TxnNotFound(TxnId),
    TxnNotActive(TxnId),
    Conflict(Key),
}

impl std::fmt::Display for TxnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnError::TxnNotFound(id) => write!(f, "transaction {} not found", id),
            TxnError::TxnNotActive(id) => write!(f, "transaction {} not active", id),
            TxnError::Conflict(key) => write!(f, "conflict on key {:?}", key),
        }
    }
}

impl std::error::Error for TxnError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_transaction() {
        let tm = TransactionManager::new();
        let txn1 = tm.begin();
        let txn2 = tm.begin();

        assert_eq!(txn1, 1);
        assert_eq!(txn2, 2);
        assert_eq!(tm.active_count(), 2);
    }

    #[test]
    fn test_commit_transaction() {
        let tm = TransactionManager::new();
        let txn = tm.begin();

        tm.record_write(txn, b"key".to_vec(), Some(b"value".to_vec())).unwrap();
        let (version, writes) = tm.commit(txn).unwrap();

        assert_eq!(version, 1);
        assert_eq!(writes.len(), 1);
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn test_abort_transaction() {
        let tm = TransactionManager::new();
        let txn = tm.begin();

        tm.record_write(txn, b"key".to_vec(), Some(b"value".to_vec())).unwrap();
        tm.abort(txn).unwrap();

        assert_eq!(tm.active_count(), 0);
        assert!(tm.get_visible_value(&b"key".to_vec(), 100).is_none());
    }

    #[test]
    fn test_snapshot_isolation() {
        let tm = TransactionManager::new();

        let t1 = tm.begin();
        tm.record_write(t1, b"key".to_vec(), Some(b"v1".to_vec())).unwrap();
        tm.commit(t1).unwrap();

        let t2 = tm.begin();
        let start_version = tm.get_start_version(t2).unwrap();

        let t3 = tm.begin();
        tm.record_write(t3, b"key".to_vec(), Some(b"v2".to_vec())).unwrap();
        tm.commit(t3).unwrap();

        let visible = tm.get_visible_value(&b"key".to_vec(), start_version);
        assert_eq!(visible, Some(b"v1".to_vec()));

        tm.commit(t2).unwrap();
    }

    #[test]
    fn test_write_conflict() {
        let tm = TransactionManager::new();

        let t1 = tm.begin();
        let t2 = tm.begin();

        tm.record_read(t1, b"key".to_vec()).unwrap();
        tm.record_write(t2, b"key".to_vec(), Some(b"v2".to_vec())).unwrap();
        tm.commit(t2).unwrap();

        let result = tm.commit(t1);
        assert!(matches!(result, Err(TxnError::Conflict(_))));
    }

    #[test]
    fn test_gc() {
        let tm = TransactionManager::new();

        for i in 0..5 {
            let t = tm.begin();
            tm.record_write(t, b"key".to_vec(), Some(format!("v{}", i).into_bytes())).unwrap();
            tm.commit(t).unwrap();
        }

        tm.gc(3);

        let visible = tm.get_visible_value(&b"key".to_vec(), 2);
        assert!(visible.is_none());

        let visible = tm.get_visible_value(&b"key".to_vec(), 5);
        assert_eq!(visible, Some(b"v4".to_vec()));
    }

    #[test]
    fn test_delete_visibility() {
        let tm = TransactionManager::new();

        let t1 = tm.begin();
        tm.record_write(t1, b"key".to_vec(), Some(b"value".to_vec())).unwrap();
        tm.commit(t1).unwrap();

        let t2 = tm.begin();
        tm.record_write(t2, b"key".to_vec(), None).unwrap();
        tm.commit(t2).unwrap();

        assert!(tm.get_visible_value(&b"key".to_vec(), 2).is_none());
        assert_eq!(tm.get_visible_value(&b"key".to_vec(), 1), Some(b"value".to_vec()));
    }
}
