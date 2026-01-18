use crate::catalog::{Catalog, CatalogError, TableSchema};
use crate::compaction::{CompactionRunner, VersionSet};
use crate::config::Config;
use crate::memtable::MemTable;
use crate::sstable::SSTableReader;
use crate::transaction::{TransactionManager, TxnError, TxnId, WriteOp};
use crate::wal::{WalEntry, WalReader, WalWriter};
use crate::{Error, Key, Result, SequenceNumber, Value};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub struct Database {
    config: Config,
    memtable: Arc<RwLock<MemTable<Key, Value>>>,
    wal: Arc<RwLock<WalWriter>>,
    version_set: Arc<RwLock<VersionSet>>,
    sstable_readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
    catalog: Arc<RwLock<Catalog>>,
    sequence: Arc<AtomicU64>,
    txn_manager: Arc<TransactionManager>,
}

impl Database {
    pub fn open(config: Config) -> Result<Self> {
        config.validate().map_err(|e| Error::InvalidConfig(e))?;

        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(&config.wal_dir)?;

        let wal_path = config.wal_dir.join("wal.log");
        let wal = WalWriter::create(&wal_path)?;

        let memtable = MemTable::with_threshold(config.memtable_size);

        let version_set = VersionSet::new();
        let sstable_readers = HashMap::new();

        let sequence = Self::recover_from_wal(&wal_path, &memtable)?;

        Ok(Database {
            config,
            memtable: Arc::new(RwLock::new(memtable)),
            wal: Arc::new(RwLock::new(wal)),
            version_set: Arc::new(RwLock::new(version_set)),
            sstable_readers: Arc::new(RwLock::new(sstable_readers)),
            catalog: Arc::new(RwLock::new(Catalog::new())),
            sequence: Arc::new(AtomicU64::new(sequence)),
            txn_manager: Arc::new(TransactionManager::new()),
        })
    }

    pub fn begin_txn(&self) -> TxnId {
        self.txn_manager.begin()
    }

    pub fn get_txn(&self, txn_id: TxnId, key: &Key) -> Result<Option<Value>> {
        if let Ok(Some(op)) = self.txn_manager.get_local(txn_id, key) {
            return Ok(match op {
                WriteOp::Put(v) => Some(v.clone()),
                WriteOp::Delete => None,
            });
        }

        self.txn_manager.record_read(txn_id, key.clone())
            .map_err(|_| Error::TransactionConflict)?;

        if let Some(start_version) = self.txn_manager.get_start_version(txn_id).ok() {
            if let Some(value) = self.txn_manager.get_visible_value(key, start_version) {
                return Ok(Some(value));
            }
        }

        self.get(key)
    }

    pub fn put_txn(&self, txn_id: TxnId, key: Key, value: Value) -> Result<()> {
        self.txn_manager.record_write(txn_id, key, Some(value))
            .map_err(|_| Error::TransactionConflict)
    }

    pub fn delete_txn(&self, txn_id: TxnId, key: Key) -> Result<()> {
        self.txn_manager.record_write(txn_id, key, None)
            .map_err(|_| Error::TransactionConflict)
    }

    pub fn commit_txn(&self, txn_id: TxnId) -> Result<()> {
        let (_version, writes) = self.txn_manager.commit(txn_id)
            .map_err(|e| match e {
                TxnError::Conflict(_) => Error::TransactionConflict,
                _ => Error::Internal(e.to_string()),
            })?;

        for (key, op) in writes {
            match op {
                WriteOp::Put(value) => self.put(key, value)?,
                WriteOp::Delete => self.delete(key)?,
            }
        }

        Ok(())
    }

    pub fn abort_txn(&self, txn_id: TxnId) -> Result<()> {
        self.txn_manager.abort(txn_id)
            .map_err(|e| Error::Internal(e.to_string()))
    }

    pub fn create_table(&self, schema: TableSchema) -> std::result::Result<(), CatalogError> {
        let mut catalog = self.catalog.write().unwrap();
        catalog.register_table(schema)
    }

    pub fn drop_table(&self, name: &str) -> std::result::Result<TableSchema, CatalogError> {
        let mut catalog = self.catalog.write().unwrap();
        catalog.drop_table(name)
    }

    pub fn get_schema(&self, name: &str) -> Option<TableSchema> {
        let catalog = self.catalog.read().unwrap();
        catalog.get_table(name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let catalog = self.catalog.read().unwrap();
        catalog.list_tables().into_iter().map(|s| s.to_string()).collect()
    }

    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        Arc::clone(&self.catalog)
    }

    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        {
            let mut wal = self.wal.write().unwrap();
            let entry = WalEntry::put(seq, key.clone(), value.clone());
            wal.append(&entry)?;
            wal.sync()?;
        }

        {
            let mut memtable = self.memtable.write().unwrap();
            memtable.put(key, value).map_err(|e| Error::Internal(e))?;

            if memtable.should_flush() {
                drop(memtable);
                self.flush_memtable()?;
            }
        }

        Ok(())
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        {
            let memtable = self.memtable.read().unwrap();
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value.clone()));
            }
        }

        let sstable_readers = self.sstable_readers.read().unwrap();
        let version_set = self.version_set.read().unwrap();
        let version = version_set.current();

        for metadata in version.files_for_key(key) {
            if let Some(reader) = sstable_readers.get(&metadata.file_id) {
                if let Some(value) = reader.get(key)? {
                    if value == b"\x00TOMBSTONE" {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    pub fn delete(&self, key: Key) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        {
            let mut wal = self.wal.write().unwrap();
            let entry = WalEntry::delete(seq, key.clone());
            wal.append(&entry)?;
            wal.sync()?;
        }

        {
            let mut memtable = self.memtable.write().unwrap();
            memtable.delete(key).map_err(|e| Error::Internal(e))?;

            if memtable.should_flush() {
                drop(memtable);
                self.flush_memtable()?;
            }
        }

        Ok(())
    }

    fn flush_memtable(&self) -> Result<()> {
        let file_id = {
            let vs = self.version_set.read().unwrap();
            vs.next_file_id()
        };
        let sstable_path = self.config.data_dir.join(format!("sst_{:08}.sst", file_id));

        let memtable_to_flush = {
            let mut mt = self.memtable.write().unwrap();
            let new_memtable = MemTable::with_threshold(self.config.memtable_size);
            std::mem::replace(&mut *mt, new_memtable)
        };

        let metadata = memtable_to_flush.flush_to_sstable(
            &sstable_path,
            file_id,
            0,
            self.config.block_size,
        )?;

        let reader = SSTableReader::open(&sstable_path)?;

        {
            let mut vs = self.version_set.write().unwrap();
            vs.add_file(0, metadata);
        }

        {
            let mut readers = self.sstable_readers.write().unwrap();
            readers.insert(file_id, reader);
        }

        self.maybe_compact()?;

        Ok(())
    }

    fn maybe_compact(&self) -> Result<()> {
        let runner = CompactionRunner::new(
            Arc::clone(&self.version_set),
            Arc::clone(&self.sstable_readers),
            self.config.clone(),
        );

        while runner.maybe_compact()? {}

        Ok(())
    }

    fn recover_from_wal(
        wal_path: &PathBuf,
        _memtable: &MemTable<Key, Value>,
    ) -> Result<SequenceNumber> {
        if !wal_path.exists() {
            return Ok(0);
        }

        let mut reader = WalReader::open(wal_path)?;
        let entries = reader.read_all()?;

        let mut max_seq = 0;

        for entry in entries {
            max_seq = max_seq.max(entry.sequence_number);
        }

        Ok(max_seq + 1)
    }

    pub fn stats(&self) -> DatabaseStats {
        let memtable = self.memtable.read().unwrap();
        let version_set = self.version_set.read().unwrap();
        let version = version_set.current();

        let num_sstables = version.all_files().count();

        DatabaseStats {
            memtable_size: memtable.approx_size(),
            memtable_entries: memtable.len(),
            num_sstables,
            sequence_number: self.sequence.load(Ordering::SeqCst),
            l0_file_count: version.l0_file_count(),
        }
    }

    pub fn close(self) -> Result<()> {
        {
            let memtable = self.memtable.read().unwrap();
            if !memtable.is_empty() {
                drop(memtable);
                self.flush_memtable()?;
            }
        }

        {
            let mut wal = self.wal.write().unwrap();
            wal.sync()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub memtable_size: usize,
    pub memtable_entries: usize,
    pub num_sstables: usize,
    pub sequence_number: u64,
    pub l0_file_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{DataType, TableSchemaBuilder};
    use tempfile::TempDir;

    #[test]
    fn test_database_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());

        let db = Database::open(config).unwrap();

        db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        db.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        assert_eq!(db.get(&b"key1".to_vec()).unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(&b"key2".to_vec()).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(&b"key3".to_vec()).unwrap(), None);

        db.delete(b"key1".to_vec()).unwrap();
        assert_eq!(db.get(&b"key1".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_database_stats() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());

        let db = Database::open(config).unwrap();

        db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();

        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 1);
        assert!(stats.memtable_size > 0);
    }

    #[test]
    fn test_database_catalog() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());
        let db = Database::open(config).unwrap();

        let schema = TableSchemaBuilder::new("users")
            .column("id", DataType::Int64, false)
            .column("name", DataType::String, false)
            .column("active", DataType::Bool, true)
            .build();

        db.create_table(schema).unwrap();

        assert!(db.list_tables().contains(&"users".to_string()));

        let retrieved = db.get_schema("users").unwrap();
        assert_eq!(retrieved.name, "users");
        assert_eq!(retrieved.column_count(), 3);

        db.drop_table("users").unwrap();
        assert!(db.get_schema("users").is_none());
    }

    #[test]
    fn test_database_transaction_commit() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());
        let db = Database::open(config).unwrap();

        let txn = db.begin_txn();

        db.put_txn(txn, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        db.put_txn(txn, b"key2".to_vec(), b"value2".to_vec()).unwrap();

        let v1 = db.get_txn(txn, &b"key1".to_vec()).unwrap();
        assert_eq!(v1, Some(b"value1".to_vec()));

        assert!(db.get(&b"key1".to_vec()).unwrap().is_none());

        db.commit_txn(txn).unwrap();

        assert_eq!(db.get(&b"key1".to_vec()).unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(&b"key2".to_vec()).unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_database_transaction_abort() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());
        let db = Database::open(config).unwrap();

        let txn = db.begin_txn();
        db.put_txn(txn, b"key1".to_vec(), b"value1".to_vec()).unwrap();

        db.abort_txn(txn).unwrap();

        assert!(db.get(&b"key1".to_vec()).unwrap().is_none());
    }

    #[test]
    fn test_database_transaction_delete() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());
        let db = Database::open(config).unwrap();

        db.put(b"key1".to_vec(), b"initial".to_vec()).unwrap();
        assert_eq!(db.get(&b"key1".to_vec()).unwrap(), Some(b"initial".to_vec()));

        let txn = db.begin_txn();
        db.delete_txn(txn, b"key1".to_vec()).unwrap();
        db.commit_txn(txn).unwrap();

        assert!(db.get(&b"key1".to_vec()).unwrap().is_none());
    }
}
