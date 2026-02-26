use crate::cache::BlockCache;
use crate::catalog::{Catalog, CatalogError, TableSchema};
use crate::compaction::{CompactionWorker, VersionSet};
use crate::config::Config;
use crate::memtable::MemTable;
use crate::sstable::SSTableReader;
use crate::transaction::{TransactionManager, TxnError, TxnId, WriteOp};
use crate::wal::{WalEntry, WalReader, WalWriter};
use crate::{Error, Key, Result, SequenceNumber, Value};
use crossbeam::channel;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

struct WalRequest {
    entries: Vec<WalEntry>,
    result_tx: channel::Sender<Result<()>>,
}

pub struct Database {
    config: Config,
    memtable: Arc<RwLock<MemTable<Key, Value>>>,
    immutable_memtable: Arc<RwLock<Option<MemTable<Key, Value>>>>,
    wal: Arc<RwLock<WalWriter>>,
    wal_tx: Option<channel::Sender<WalRequest>>,
    _wal_thread: Option<thread::JoinHandle<()>>,
    version_set: Arc<RwLock<VersionSet>>,
    sstable_readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
    block_cache: Arc<BlockCache>,
    catalog: Arc<RwLock<Catalog>>,
    sequence: Arc<AtomicU64>,
    flushing: AtomicBool,
    txn_manager: Arc<TransactionManager>,
    _compaction_worker: Option<CompactionWorker>,
}

impl Database {
    pub fn open(config: Config) -> Result<Self> {
        config.validate().map_err(|e| Error::InvalidConfig(e))?;

        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(&config.wal_dir)?;

        let wal_path = config.wal_dir.join("wal.log");
        let wal = WalWriter::create(&wal_path)?;

        let mut memtable = MemTable::with_threshold(config.memtable_size);

        let version_set = VersionSet::new();
        let sstable_readers = HashMap::new();

        let sequence = Self::recover_from_wal(&wal_path, &mut memtable)?;

        let version_set = Arc::new(RwLock::new(version_set));
        let sstable_readers = Arc::new(RwLock::new(sstable_readers));
        let block_cache = Arc::new(BlockCache::new(config.block_cache_size));

        let compaction_worker = CompactionWorker::start(
            Arc::clone(&version_set),
            Arc::clone(&sstable_readers),
            config.clone(),
        );

        let wal = Arc::new(RwLock::new(wal));

        // Start WAL group commit thread when sync_writes is enabled.
        // Multiple concurrent writers send entries to this thread, which batches
        // them into a single append_batch + fsync, amortizing the fsync cost.
        let (wal_tx, wal_thread) = if config.sync_writes {
            let (tx, rx) = channel::bounded::<WalRequest>(256);
            let wal_clone = Arc::clone(&wal);
            let handle = thread::Builder::new()
                .name("middb-wal-writer".to_string())
                .spawn(move || {
                    Self::wal_writer_loop(wal_clone, rx);
                })
                .expect("failed to spawn WAL writer thread");
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        Ok(Database {
            config,
            memtable: Arc::new(RwLock::new(memtable)),
            immutable_memtable: Arc::new(RwLock::new(None)),
            wal,
            wal_tx,
            _wal_thread: wal_thread,
            version_set,
            sstable_readers,
            block_cache,
            catalog: Arc::new(RwLock::new(Catalog::new())),
            sequence: Arc::new(AtomicU64::new(sequence)),
            flushing: AtomicBool::new(false),
            txn_manager: Arc::new(TransactionManager::new()),
            _compaction_worker: Some(compaction_worker),
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

    /// Commit a transaction's writes to durable storage.
    ///
    /// Note: committed writes are applied via `put()`/`delete()` which store only the latest
    /// value in the memtable (no per-version history). The MVCC version index in
    /// TransactionManager tracks visibility for in-flight transactions. Direct `put()`/`delete()`
    /// calls (outside transactions) are non-transactional and not tracked by MVCC.
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
        let mut catalog = self.catalog.write();
        catalog.register_table(schema)
    }

    pub fn drop_table(&self, name: &str) -> std::result::Result<TableSchema, CatalogError> {
        let mut catalog = self.catalog.write();
        catalog.drop_table(name)
    }

    pub fn get_schema(&self, name: &str) -> Option<TableSchema> {
        let catalog = self.catalog.read();
        catalog.get_table(name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let catalog = self.catalog.read();
        catalog.list_tables().into_iter().map(|s| s.to_string()).collect()
    }

    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        Arc::clone(&self.catalog)
    }

    /// Write WAL entries via group commit (sync_writes=true) or direct append (sync_writes=false).
    fn write_wal(&self, entries: Vec<WalEntry>) -> Result<()> {
        if let Some(ref tx) = self.wal_tx {
            // Group commit path: send to WAL writer thread, wait for batch fsync
            let (result_tx, result_rx) = channel::bounded(1);
            tx.send(WalRequest { entries, result_tx })
                .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?;
            result_rx.recv()
                .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?
        } else {
            // Direct path: append without sync
            let mut wal = self.wal.write();
            wal.append_batch(&entries)?;
            Ok(())
        }
    }

    fn wal_writer_loop(wal: Arc<RwLock<WalWriter>>, rx: channel::Receiver<WalRequest>) {
        const MAX_BATCH: usize = 256;
        let mut batch = Vec::with_capacity(MAX_BATCH);

        while let Ok(first) = rx.recv() {
            batch.push(first);

            // Drain additional pending requests (non-blocking)
            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }

            // Collect all entries, write once, sync once
            let all_entries: Vec<WalEntry> = batch.iter()
                .flat_map(|req| req.entries.iter().cloned())
                .collect();

            let result = {
                let mut wal = wal.write();
                wal.append_batch(&all_entries).and_then(|_| wal.sync())
            };

            // Notify all waiters
            for req in batch.drain(..) {
                let _ = req.result_tx.send(result.as_ref().map(|_| ()).map_err(|e| {
                    Error::Internal(format!("WAL sync failed: {}", e))
                }));
            }
        }
    }

    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::put(seq, key.clone(), value.clone());

        self.write_wal(vec![entry])?;

        {
            let mut memtable = self.memtable.write();
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
            let memtable = self.memtable.read();
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value.clone()));
            }
        }

        {
            let imm = self.immutable_memtable.read();
            if let Some(ref imm_mt) = *imm {
                if let Some(value) = imm_mt.get(key) {
                    return Ok(Some(value.clone()));
                }
            }
        }

        let sstable_readers = self.sstable_readers.read();
        let version_set = self.version_set.read();
        let version = version_set.current();

        for metadata in version.files_for_key(key) {
            if let Some(reader) = sstable_readers.get(&metadata.file_id) {
                if let Some(tagged_value) = reader.get(key)? {
                    if tagged_value.is_empty() {
                        continue;
                    }
                    match tagged_value[0] {
                        0x02 => return Ok(None), // tombstone
                        0x01 => return Ok(Some(tagged_value[1..].to_vec())), // value
                        _ => {
                            // Legacy format (no prefix) — treat as raw value
                            return Ok(Some(tagged_value));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn delete(&self, key: Key) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::delete(seq, key.clone());

        self.write_wal(vec![entry])?;

        {
            let mut memtable = self.memtable.write();
            memtable.delete(key).map_err(|e| Error::Internal(e))?;

            if memtable.should_flush() {
                drop(memtable);
                self.flush_memtable()?;
            }
        }

        Ok(())
    }

    fn flush_memtable(&self) -> Result<()> {
        // Guard: only one flush at a time. If another thread is flushing,
        // skip — the data is safe in the active memtable and will be flushed next time.
        if self.flushing.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Ok(());
        }

        let result = self.flush_memtable_inner();
        self.flushing.store(false, Ordering::SeqCst);
        result
    }

    fn flush_memtable_inner(&self) -> Result<()> {
        let file_id = {
            let vs = self.version_set.read();
            vs.next_file_id()
        };
        let sstable_path = self.config.data_dir.join(format!("sst_{:08}.sst", file_id));

        // Swap active memtable to immutable — holds write lock only briefly
        {
            let mut mt = self.memtable.write();
            let new_memtable = MemTable::with_threshold(self.config.memtable_size);
            let old_memtable = std::mem::replace(&mut *mt, new_memtable);
            let mut imm = self.immutable_memtable.write();
            *imm = Some(old_memtable);
        }
        // Write lock released — new writes can proceed to the fresh active memtable

        // Flush immutable memtable to SSTable (no locks held on active memtable)
        let metadata = {
            let imm = self.immutable_memtable.read();
            let imm_mt = imm.as_ref().unwrap();
            imm_mt.flush_to_sstable(
                &sstable_path,
                file_id,
                0,
                self.config.block_size,
            )?
        };

        let reader = SSTableReader::open_with_cache(&sstable_path, file_id, Some(Arc::clone(&self.block_cache)))?;

        {
            let mut vs = self.version_set.write();
            vs.add_file(0, metadata);
        }

        {
            let mut readers = self.sstable_readers.write();
            readers.insert(file_id, reader);
        }

        // Clear immutable memtable
        {
            let mut imm = self.immutable_memtable.write();
            *imm = None;
        }

        Ok(())
    }

    fn recover_from_wal(
        wal_path: &PathBuf,
        memtable: &mut MemTable<Key, Value>,
    ) -> Result<SequenceNumber> {
        if !wal_path.exists() {
            return Ok(0);
        }

        let mut reader = WalReader::open(wal_path)?;
        let entries = reader.read_all()?;

        let mut max_seq = 0;

        for entry in entries {
            max_seq = max_seq.max(entry.sequence_number);

            match entry.entry_type {
                crate::wal::EntryType::Put => {
                    if let Some(value) = entry.value {
                        let _ = memtable.put(entry.key, value);
                    }
                }
                crate::wal::EntryType::Delete => {
                    let _ = memtable.delete(entry.key);
                }
                crate::wal::EntryType::BTreePageWrite => {
                    // B-tree page writes are not replayed into memtable
                }
            }
        }

        // Truncate WAL after successful recovery — replayed data is now in the memtable
        // and will be persisted to SSTables through normal flush path
        if max_seq > 0 {
            let _ = fs::File::create(wal_path); // truncate to zero
        }

        Ok(max_seq + 1)
    }

    pub fn stats(&self) -> DatabaseStats {
        let memtable = self.memtable.read();
        let version_set = self.version_set.read();
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

    pub fn scan(&self, start: &Key, end: &Key) -> Vec<(Key, Value)> {
        let memtable = self.memtable.read();
        let mut results = Vec::new();

        for (k, entry) in memtable.range(start, end) {
            if let crate::memtable::ValueEntry::Value(v) = entry {
                results.push((k.clone(), v.clone()));
            }
        }

        let imm = self.immutable_memtable.read();
        if let Some(ref imm_mt) = *imm {
            for (k, entry) in imm_mt.range(start, end) {
                if let crate::memtable::ValueEntry::Value(v) = entry {
                    if !results.iter().any(|(rk, _)| rk == k) {
                        results.push((k.clone(), v.clone()));
                    }
                }
            }
            results.sort_by(|(a, _), (b, _)| a.cmp(b));
        }

        results
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // Build all WAL entries and collect ops
        let mut wal_entries = Vec::with_capacity(batch.len());
        for op in &batch.ops {
            let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
            match op {
                WriteBatchOp::Put(key, value) => {
                    wal_entries.push(WalEntry::put(seq, key.clone(), value.clone()));
                }
                WriteBatchOp::Delete(key) => {
                    wal_entries.push(WalEntry::delete(seq, key.clone()));
                }
            }
        }

        self.write_wal(wal_entries)?;

        // Apply all ops to memtable
        {
            let mut memtable = self.memtable.write();
            for op in batch.ops {
                match op {
                    WriteBatchOp::Put(key, value) => {
                        memtable.put(key, value).map_err(|e| Error::Internal(e))?;
                    }
                    WriteBatchOp::Delete(key) => {
                        memtable.delete(key).map_err(|e| Error::Internal(e))?;
                    }
                }
            }

            if memtable.should_flush() {
                drop(memtable);
                self.flush_memtable()?;
            }
        }

        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        {
            let memtable = self.memtable.read();
            if !memtable.is_empty() {
                drop(memtable);
                self.flush_memtable()?;
            }
        }

        // Stop WAL writer thread by dropping the sender
        drop(self.wal_tx.take());
        if let Some(handle) = self._wal_thread.take() {
            let _ = handle.join();
        }

        {
            let mut wal = self.wal.write();
            wal.sync()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum WriteBatchOp {
    Put(Key, Value),
    Delete(Key),
}

pub struct WriteBatch {
    ops: Vec<WriteBatchOp>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch { ops: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        WriteBatch { ops: Vec::with_capacity(capacity) }
    }

    pub fn put(&mut self, key: Key, value: Value) {
        self.ops.push(WriteBatchOp::Put(key, value));
    }

    pub fn delete(&mut self, key: Key) {
        self.ops.push(WriteBatchOp::Delete(key));
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn test_crash_recovery_from_wal() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write data and drop without calling close() (simulating crash)
        {
            let config = Config::new(&path);
            let db = Database::open(config).unwrap();
            db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
            db.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
            db.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
            db.delete(b"key2".to_vec()).unwrap();
            // Drop without close — data is in WAL but not flushed to SSTable
        }

        // Reopen — should recover data from WAL
        {
            let config = Config::new(&path);
            let db = Database::open(config).unwrap();
            assert_eq!(db.get(&b"key1".to_vec()).unwrap(), Some(b"value1".to_vec()));
            assert_eq!(db.get(&b"key2".to_vec()).unwrap(), None); // was deleted
            assert_eq!(db.get(&b"key3".to_vec()).unwrap(), Some(b"value3".to_vec()));
        }
    }
}
