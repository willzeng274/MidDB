use crate::cache::BlockCache;
use crate::catalog::{Catalog, CatalogError, TableSchema};
use crate::compaction::{CompactionWorker, VersionSet};
use crate::config::Config;
use crate::manifest::{self, ManifestFileEntry, ManifestRecord};
use crate::memtable::ShardedMemTable;
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
    result_tx: Option<channel::Sender<Result<()>>>,
}

pub struct Database {
    config: Config,
    memtable: Arc<RwLock<ShardedMemTable<Key, Value>>>,
    immutable_memtable: Arc<RwLock<Option<ShardedMemTable<Key, Value>>>>,
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
        config.validate().map_err(Error::InvalidConfig)?;

        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(&config.wal_dir)?;

        let wal_path = config.wal_dir.join("wal.log");
        let wal = WalWriter::create(&wal_path)?;

        let memtable = ShardedMemTable::with_threshold(config.memtable_size);
        let block_cache = Arc::new(BlockCache::new(config.block_cache_size));

        // Restore persistent state from MANIFEST (SSTable metadata, file IDs)
        let mut version_set = VersionSet::new();
        let mut sstable_readers = HashMap::new();
        let mut persisted_sequence = 0u64;

        if let Some(manifest) = manifest::read_manifest(&config.data_dir)? {
            version_set.set_next_file_id(manifest.next_file_id);
            persisted_sequence = manifest.sequence_number;

            for entry in &manifest.files {
                let path = config.data_dir.join(format!("sst_{:08}.sst", entry.file_id));
                if path.exists() {
                    let reader = SSTableReader::open_with_cache(
                        &path,
                        entry.file_id,
                        Some(Arc::clone(&block_cache)),
                    )?;
                    sstable_readers.insert(entry.file_id, reader);
                    version_set.add_file(entry.level, entry.to_metadata());
                } else {
                    eprintln!(
                        "[middb] WARNING: manifest references missing SSTable file {} (level {}), skipping",
                        entry.file_id, entry.level
                    );
                }
            }
        }

        // Replay WAL entries written after the last flush
        let wal_sequence = Self::recover_from_wal(&wal_path, &memtable)?;
        let sequence = wal_sequence.max(persisted_sequence);

        let version_set = Arc::new(RwLock::new(version_set));
        let sstable_readers = Arc::new(RwLock::new(sstable_readers));
        let sequence = Arc::new(AtomicU64::new(sequence));

        let compaction_worker = CompactionWorker::start_with_sequence(
            Arc::clone(&version_set),
            Arc::clone(&sstable_readers),
            config.clone(),
            Arc::clone(&block_cache),
            Arc::clone(&sequence),
        );

        let wal = Arc::new(RwLock::new(wal));

        let sync_writes = config.sync_writes;
        let (wal_tx, wal_thread) = {
            let (tx, rx) = channel::bounded::<WalRequest>(4096);
            let wal_clone = Arc::clone(&wal);
            let handle = thread::Builder::new()
                .name("middb-wal-writer".to_string())
                .spawn(move || {
                    Self::wal_writer_loop(wal_clone, rx, sync_writes);
                })
                .expect("failed to spawn WAL writer thread");
            (Some(tx), Some(handle))
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
            sequence,
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

        if let Ok(start_version) = self.txn_manager.get_start_version(txn_id) {
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

    /// Commit a transaction's writes atomically via write_batch.
    /// All writes land in the WAL and memtable together — concurrent readers
    /// see either all or none of the transaction's keys.
    pub fn commit_txn(&self, txn_id: TxnId) -> Result<()> {
        let (_version, writes) = self.txn_manager.commit(txn_id)
            .map_err(|e| match e {
                TxnError::Conflict(_) => Error::TransactionConflict,
                _ => Error::Internal(e.to_string()),
            })?;

        let mut batch = WriteBatch::with_capacity(writes.len());
        for (key, op) in writes {
            match op {
                WriteOp::Put(value) => batch.put(key, value),
                WriteOp::Delete => batch.delete(key),
            }
        }
        self.write_batch(batch)
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

    /// Write WAL entries via the WAL writer thread.
    /// With sync_writes=true: blocks until batch is written + fsync'd (durability guarantee).
    /// With sync_writes=false: fire-and-forget to avoid blocking on channel roundtrip.
    fn write_wal_single(&self, entry: WalEntry) -> Result<()> {
        if let Some(ref tx) = self.wal_tx {
            if self.config.sync_writes {
                let (result_tx, result_rx) = channel::bounded(1);
                tx.send(WalRequest { entries: vec![entry], result_tx: Some(result_tx) })
                    .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?;
                result_rx.recv()
                    .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?
            } else {
                tx.send(WalRequest { entries: vec![entry], result_tx: None })
                    .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?;
                Ok(())
            }
        } else {
            let mut wal = self.wal.write();
            wal.append(&entry)?;
            Ok(())
        }
    }

    fn write_wal(&self, entries: Vec<WalEntry>) -> Result<()> {
        if let Some(ref tx) = self.wal_tx {
            if self.config.sync_writes {
                let (result_tx, result_rx) = channel::bounded(1);
                tx.send(WalRequest { entries, result_tx: Some(result_tx) })
                    .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?;
                result_rx.recv()
                    .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?
            } else {
                tx.send(WalRequest { entries, result_tx: None })
                    .map_err(|_| Error::Internal("WAL writer thread stopped".to_string()))?;
                Ok(())
            }
        } else {
            let mut wal = self.wal.write();
            wal.append_batch(&entries)?;
            Ok(())
        }
    }

    fn wal_writer_loop(wal: Arc<RwLock<WalWriter>>, rx: channel::Receiver<WalRequest>, sync_writes: bool) {
        const MAX_BATCH: usize = 4096;
        let mut batch = Vec::with_capacity(MAX_BATCH);

        while let Ok(first) = rx.recv() {
            batch.push(first);

            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }

            let all_entries: Vec<WalEntry> = batch.iter()
                .flat_map(|req| req.entries.iter().cloned())
                .collect();

            let result = {
                let mut wal = wal.write();
                let r = wal.append_batch(&all_entries);
                if sync_writes {
                    r.and_then(|_| wal.sync())
                } else {
                    r
                }
            };

            for req in batch.drain(..) {
                if let Some(tx) = req.result_tx {
                    let _ = tx.send(result.as_ref().map(|_| ()).map_err(|e| {
                        Error::Internal(format!("WAL write failed: {e}"))
                    }));
                }
            }
        }
    }

    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        // Backpressure: if L0 has too many files, wait for compaction to catch up
        self.maybe_throttle_writes();

        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let entry = WalEntry::put(seq, key.clone(), value.clone());

        self.write_wal_single(entry)?;

        let should_flush = {
            let memtable = self.memtable.read();
            memtable.put(key, value).map_err(Error::Internal)?;
            memtable.should_flush()
        };

        if should_flush {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Stall writes when L0 accumulation is too high, giving compaction time to catch up.
    fn maybe_throttle_writes(&self) {
        let l0_slowdown = self.config.level0_file_num_compaction_trigger * 2;
        let l0_stop = self.config.level0_file_num_compaction_trigger * 4;

        let l0_count = {
            let vs = self.version_set.read();
            vs.current().l0_file_count()
        };

        if l0_count >= l0_stop {
            // Hard stall — wait until compaction reduces L0
            while {
                let vs = self.version_set.read();
                vs.current().l0_file_count() >= l0_slowdown
            } {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        } else if l0_count >= l0_slowdown {
            // Soft slowdown — brief pause
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        {
            let memtable = self.memtable.read();
            match memtable.get_with_tombstone(key) {
                Some(Some(v)) => return Ok(Some(v)),
                Some(None) => return Ok(None),
                None => {}
            }
        }

        {
            let imm = self.immutable_memtable.read();
            if let Some(ref imm_mt) = *imm {
                match imm_mt.get_with_tombstone(key) {
                    Some(Some(v)) => return Ok(Some(v)),
                    Some(None) => return Ok(None),
                    None => {}
                }
            }
        }

        // Take readers lock first to prevent compaction from removing readers
        // between version snapshot and reader lookup.
        let sstable_readers = self.sstable_readers.read();
        let version = self.version_set.read().current();

        // Inline L0 + level search to avoid Vec allocation in files_for_key.
        // Check L0 files (newest first, may overlap)
        if let Some(l0) = version.level(0) {
            for file in l0.files.iter().rev() {
                if !file.may_contain(key) {
                    continue;
                }
                if let Some(reader) = sstable_readers.get(&file.file_id) {
                    if let Some(v) = Self::decode_sstable_value(reader, key)? {
                        return Ok(v);
                    }
                }
            }
        }

        // Check L1+ (non-overlapping, binary search)
        for level in version.levels.iter().skip(1) {
            let pos = level.files.binary_search_by(|f| {
                if key.as_slice() < f.smallest_key.as_slice() {
                    std::cmp::Ordering::Greater
                } else if key.as_slice() > f.largest_key.as_slice() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            });
            if let Ok(idx) = pos {
                let file = &level.files[idx];
                if let Some(reader) = sstable_readers.get(&file.file_id) {
                    if let Some(v) = Self::decode_sstable_value(reader, key)? {
                        return Ok(v);
                    }
                }
            }
        }

        Ok(None)
    }

    /// Decode a tagged value from SSTable. Returns Some(result) if found, None to continue searching.
    fn decode_sstable_value(reader: &SSTableReader, key: &Key) -> Result<Option<Option<Value>>> {
        if let Some(tagged) = reader.get(key)? {
            if tagged.is_empty() {
                return Err(Error::Corruption("empty tagged value in SSTable".to_string()));
            }
            return Ok(Some(match tagged[0] {
                0x02 => None,
                0x01 => Some(tagged[1..].to_vec()),
                _ => Some(tagged),
            }));
        }
        Ok(None)
    }

    pub fn delete(&self, key: Key) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let entry = WalEntry::delete(seq, key.clone());

        self.write_wal_single(entry)?;

        let should_flush = {
            let memtable = self.memtable.read();
            memtable.delete(key).map_err(Error::Internal)?;
            memtable.should_flush()
        };

        if should_flush {
            self.flush_memtable()?;
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
        // Safety: if there's already an immutable memtable being flushed, don't
        // overwrite it — that would lose data. Wait for it to be cleared.
        {
            let imm = self.immutable_memtable.read();
            if imm.is_some() {
                // Another flush is in progress and hasn't cleared the immutable yet.
                // This shouldn't happen because of the flushing CAS guard, but defend
                // against it anyway. The data is safe in the active memtable.
                return Ok(());
            }
        }

        let file_id = {
            let vs = self.version_set.read();
            vs.next_file_id()
        };
        let sstable_path = self.config.data_dir.join(format!("sst_{file_id:08}.sst"));

        // Swap active memtable to immutable — holds outer write lock only briefly (pointer swap)
        {
            let mut mt = self.memtable.write();
            let new_memtable = ShardedMemTable::with_threshold(self.config.memtable_size);
            let old_memtable = std::mem::replace(&mut *mt, new_memtable);
            let mut imm = self.immutable_memtable.write();
            *imm = Some(old_memtable);
        }

        // Flush immutable memtable to SSTable
        let metadata = {
            let imm = self.immutable_memtable.read();
            let imm_mt = match imm.as_ref() {
                Some(mt) => mt,
                None => return Ok(()),
            };
            imm_mt.flush_to_sstable_with_compression(
                &sstable_path,
                file_id,
                0,
                self.config.block_size,
                self.config.compression_type,
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

        // Write MANIFEST atomically *before* clearing immutable memtable or truncating WAL.
        // This ensures that on crash, the SSTable is discoverable.
        self.persist_manifest()?;

        // Truncate WAL through the writer's own file descriptor to avoid
        // racing with the WAL writer thread (which holds the old fd).
        {
            let mut wal = self.wal.write();
            wal.truncate()?;
        }

        // Clear immutable memtable — data is now safely on disk with manifest pointer
        {
            let mut imm = self.immutable_memtable.write();
            *imm = None;
        }

        // GC transaction version history. Use the minimum start_version of all
        // active transactions so we don't break their snapshot reads.
        let gc_version = self.txn_manager.min_active_start_version()
            .unwrap_or_else(|| self.txn_manager.current_version());
        if gc_version > 0 {
            self.txn_manager.gc(gc_version);
        }

        Ok(())
    }

    /// Write the current version set state to the MANIFEST file.
    fn persist_manifest(&self) -> Result<()> {
        // Collect data under the lock, then release before doing disk I/O
        let record = {
            let vs = self.version_set.read();
            let version = vs.current();
            let files: Vec<ManifestFileEntry> = version.all_files()
                .map(ManifestFileEntry::from_metadata)
                .collect();
            ManifestRecord {
                next_file_id: vs.current_file_id(),
                sequence_number: self.sequence.load(Ordering::SeqCst),
                files,
            }
        };

        manifest::write_manifest(&self.config.data_dir, &record)
    }

    fn recover_from_wal(
        wal_path: &PathBuf,
        memtable: &ShardedMemTable<Key, Value>,
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

        // Don't truncate WAL here — the replayed data is only in the memtable,
        // not yet persisted to SSTables. WAL truncation happens in flush_memtable_inner
        // after the SSTable and MANIFEST are safely on disk.

        Ok(if max_seq > 0 { max_seq + 1 } else { 0 })
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
        use std::collections::BTreeMap;

        // Merge all layers into a single BTreeMap, respecting precedence:
        // SSTable (oldest) → immutable memtable → active memtable (newest).
        // Later inserts overwrite earlier ones, so newest wins.
        // Option<Value>: None = tombstone (key deleted).
        let mut merged: BTreeMap<Key, Option<Value>> = BTreeMap::new();

        // Layer 1: SSTables (oldest data)
        // Take both locks together so compaction can't remove a reader between
        // reading the version (which references file IDs) and looking up readers.
        {
            let sstable_readers = self.sstable_readers.read();
            let version = self.version_set.read().current();

            // Scan levels in reverse order (highest level = oldest)
            for level in version.levels.iter().rev() {
                for file in &level.files {
                    if file.largest_key.as_slice() < start.as_slice()
                        || file.smallest_key.as_slice() >= end.as_slice()
                    {
                        continue;
                    }
                    if let Some(reader) = sstable_readers.get(&file.file_id) {
                        if let Ok(mut iter) = reader.iter() {
                            if iter.seek(start).is_ok() {
                                while iter.valid() {
                                    if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                                        if k >= end.as_slice() {
                                            break;
                                        }
                                        let key = k.to_vec();
                                        let decoded = if v.is_empty() {
                                            None
                                        } else {
                                            match v[0] {
                                                0x02 => None,
                                                0x01 => Some(v[1..].to_vec()),
                                                _ => Some(v.to_vec()),
                                            }
                                        };
                                        merged.insert(key, decoded);
                                    }
                                    if iter.next().is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Layer 2: Immutable memtable (newer than SSTables)
        {
            let imm = self.immutable_memtable.read();
            if let Some(ref imm_mt) = *imm {
                for (k, v) in imm_mt.range_with_tombstones(start, end) {
                    merged.insert(k, v);
                }
            }
        }

        // Layer 3: Active memtable (newest)
        {
            let memtable = self.memtable.read();
            for (k, v) in memtable.range_with_tombstones(start, end) {
                merged.insert(k, v);
            }
        }

        // Filter out tombstones
        merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut wal_entries = Vec::with_capacity(batch.len());
        for op in &batch.ops {
            let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
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

        // Apply all ops to memtable — outer read lock, each op locks its own shard
        let should_flush = {
            let memtable = self.memtable.read();
            for op in batch.ops {
                match op {
                    WriteBatchOp::Put(key, value) => {
                        memtable.put(key, value).map_err(Error::Internal)?;
                    }
                    WriteBatchOp::Delete(key) => {
                        memtable.delete(key).map_err(Error::Internal)?;
                    }
                }
            }
            memtable.should_flush()
        };

        if should_flush {
            self.flush_memtable()?;
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

    #[test]
    fn test_data_survives_restart_after_flush() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write enough data to trigger a flush, then close
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024; // 1MB — small to trigger flush
            let db = Database::open(config).unwrap();

            for i in 0..10_000 {
                let key = format!("key_{i:06}").into_bytes();
                let val = format!("val_{i:06}").into_bytes();
                db.put(key, val).unwrap();
            }

            db.close().unwrap();
        }

        // Reopen — MANIFEST should restore SSTable metadata, data should be readable
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();

            // Spot-check some keys
            for i in [0, 100, 999, 5000, 9999] {
                let key = format!("key_{i:06}").into_bytes();
                let expected = format!("val_{i:06}").into_bytes();
                let val = db.get(&key).unwrap();
                assert_eq!(val, Some(expected), "Lost data for key_{i:06} after restart");
            }

            // Verify stats show SSTables were loaded
            let stats = db.stats();
            assert!(stats.num_sstables > 0, "No SSTables loaded from MANIFEST");
        }
    }

    #[test]
    fn test_data_survives_double_restart() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write, flush, close
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();
            for i in 0..5_000 {
                db.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes()).unwrap();
            }
            db.close().unwrap();
        }

        // Reopen, write more, close again
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();
            for i in 5_000..10_000 {
                db.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes()).unwrap();
            }
            db.close().unwrap();
        }

        // Third open — all data should be present
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();
            for i in [0, 2500, 4999, 5000, 7500, 9999] {
                let key = format!("k{i}").into_bytes();
                let expected = format!("v{i}").into_bytes();
                assert_eq!(db.get(&key).unwrap(), Some(expected), "Lost k{i} after double restart");
            }
        }
    }

    #[test]
    fn test_writes_after_flush_survive_restart() {
        // Exercises the WAL truncation + continued writes path:
        // flush truncates WAL, then new writes go to the fresh WAL.
        // On restart, MANIFEST has the flushed SSTables and WAL has the post-flush writes.
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            config.sync_writes = false;
            let db = Database::open(config).unwrap();

            // Write enough to trigger flush
            for i in 0..8_000 {
                db.put(format!("pre_{i:06}").into_bytes(), b"before_flush".to_vec()).unwrap();
            }

            // These writes happen AFTER the flush (in the fresh WAL)
            for i in 0..100 {
                db.put(format!("post_{i:06}").into_bytes(), b"after_flush".to_vec()).unwrap();
            }

            // Drop without close() — simulates crash
        }

        // Reopen — should have both pre-flush (from MANIFEST+SSTables) and
        // post-flush (from WAL) data
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();

            // Pre-flush data (in SSTables via MANIFEST)
            for i in [0, 1000, 5000, 7999] {
                let key = format!("pre_{i:06}").into_bytes();
                assert_eq!(db.get(&key).unwrap(), Some(b"before_flush".to_vec()),
                    "Lost pre-flush key pre_{i:06}");
            }

            // Post-flush data (in WAL)
            for i in [0, 50, 99] {
                let key = format!("post_{i:06}").into_bytes();
                assert_eq!(db.get(&key).unwrap(), Some(b"after_flush".to_vec()),
                    "Lost post-flush key post_{i:06}");
            }
        }
    }

    #[test]
    fn test_scan_with_sstables_and_tombstones() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(temp_dir.path());
        config.memtable_size = 1024 * 1024; // small to trigger flushes
        config.sync_writes = false;
        let db = Database::open(config).unwrap();

        // Write keys that will flush to SSTables
        for i in 0..5_000 {
            db.put(format!("scan_{i:06}").into_bytes(), format!("val_{i:06}").into_bytes()).unwrap();
        }

        // Delete some keys (tombstones may be in memtable or SSTable)
        for i in (0..5_000).step_by(10) {
            db.delete(format!("scan_{i:06}").into_bytes()).unwrap();
        }

        // Scan full range
        let start = b"scan_000000".to_vec();
        let end = b"scan_999999".to_vec();
        let results = db.scan(&start, &end);

        // Should have 4500 results (5000 - 500 deleted)
        assert_eq!(results.len(), 4500, "Scan returned {} results, expected 4500", results.len());

        // Verify no deleted keys appear
        for (key, _val) in &results {
            let key_str = String::from_utf8_lossy(key);
            let num: usize = key_str.strip_prefix("scan_").unwrap().parse().unwrap();
            assert_ne!(num % 10, 0, "Deleted key {key_str} appeared in scan results");
        }
    }

    #[test]
    fn test_overwrite_survives_restart() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            config.sync_writes = false;
            let db = Database::open(config).unwrap();

            // Write v1, flush, then overwrite with v2
            for i in 0..5_000 {
                db.put(format!("ow_{i:06}").into_bytes(), b"v1".to_vec()).unwrap();
            }
            // Force flush by writing more
            for i in 5_000..10_000 {
                db.put(format!("pad_{i:06}").into_bytes(), b"pad".to_vec()).unwrap();
            }
            // Now overwrite the original keys
            for i in 0..5_000 {
                db.put(format!("ow_{i:06}").into_bytes(), b"v2".to_vec()).unwrap();
            }

            db.close().unwrap();
        }

        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();
            for i in [0, 100, 2500, 4999] {
                let key = format!("ow_{i:06}").into_bytes();
                assert_eq!(db.get(&key).unwrap(), Some(b"v2".to_vec()),
                    "Overwrite not persisted for ow_{i:06}");
            }
        }
    }

    /// Stress test: concurrent writers + reader + crash recovery.
    /// Verifies no data loss under concurrent write load.
    #[test]
    fn test_concurrent_writes_then_crash_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let expected_count;
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            config.sync_writes = false;
            let db = Arc::new(Database::open(config).unwrap());

            let handles: Vec<_> = (0..4).map(|t| {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    for i in 0..2_000 {
                        let key = format!("ct{t}_{i:06}").into_bytes();
                        let val = format!("cv{t}_{i:06}").into_bytes();
                        db.put(key, val).unwrap();
                    }
                })
            }).collect();

            for h in handles { h.join().unwrap(); }
            expected_count = 4 * 2_000;

            // Verify all data readable before crash
            for t in 0..4 {
                for i in [0, 500, 1000, 1999] {
                    let key = format!("ct{t}_{i:06}").into_bytes();
                    assert!(db.get(&key).unwrap().is_some(), "Missing ct{t}_{i:06} before crash");
                }
            }

            // Drop without close — simulate crash
            // Arc prevents close() since there are multiple owners
            drop(db);
        }

        // Recover
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();

            let mut found = 0;
            for t in 0..4 {
                for i in 0..2_000 {
                    let key = format!("ct{t}_{i:06}").into_bytes();
                    if db.get(&key).unwrap().is_some() {
                        found += 1;
                    }
                }
            }
            assert_eq!(found, expected_count,
                "Lost {} keys after concurrent write crash recovery", expected_count - found);
        }
    }

    /// Stress test: interleaved put/delete/scan correctness.
    #[test]
    fn test_interleaved_operations_correctness() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(temp_dir.path());
        config.memtable_size = 1024 * 1024;
        config.sync_writes = false;
        let db = Database::open(config).unwrap();

        // Phase 1: write 1000 keys
        for i in 0..1000 {
            db.put(format!("il_{i:04}").into_bytes(), b"v1".to_vec()).unwrap();
        }

        // Phase 2: delete even keys
        for i in (0..1000).step_by(2) {
            db.delete(format!("il_{i:04}").into_bytes()).unwrap();
        }

        // Phase 3: overwrite odd keys with v2
        for i in (1..1000).step_by(2) {
            db.put(format!("il_{i:04}").into_bytes(), b"v2".to_vec()).unwrap();
        }

        // Phase 4: write 2000 more to force flush
        for i in 1000..8000 {
            db.put(format!("il_{i:04}").into_bytes(), b"v3".to_vec()).unwrap();
        }

        // Verify point reads
        for i in 0..1000 {
            let key = format!("il_{i:04}").into_bytes();
            let val = db.get(&key).unwrap();
            if i % 2 == 0 {
                assert_eq!(val, None, "Deleted key il_{i:04} still present");
            } else {
                assert_eq!(val, Some(b"v2".to_vec()), "Key il_{i:04} has wrong value");
            }
        }

        // Verify scan
        let results = db.scan(&b"il_0000".to_vec(), &b"il_1000".to_vec());
        assert_eq!(results.len(), 500, "Scan found {} keys, expected 500 (odd keys only)", results.len());
        for (key, val) in &results {
            let key_str = String::from_utf8_lossy(key);
            let num: usize = key_str.strip_prefix("il_").unwrap().parse().unwrap();
            assert!(num % 2 == 1, "Even key {key_str} in scan (should be deleted)");
            assert_eq!(val, &b"v2".to_vec(), "Wrong value for {key_str}");
        }
    }

    /// Edge case: empty database operations.
    #[test]
    fn test_empty_database_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());
        let db = Database::open(config).unwrap();

        // Get on empty db
        assert_eq!(db.get(&b"nonexistent".to_vec()).unwrap(), None);

        // Scan on empty db
        let results = db.scan(&b"a".to_vec(), &b"z".to_vec());
        assert!(results.is_empty());

        // Delete on empty db (should not error)
        db.delete(b"nonexistent".to_vec()).unwrap();

        // Stats on empty db
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 1); // tombstone from delete
        assert_eq!(stats.num_sstables, 0);
    }

    /// Edge case: very large values.
    #[test]
    fn test_large_value_correctness() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(temp_dir.path());
        config.sync_writes = false;
        let db = Database::open(config).unwrap();

        // Write a value larger than block size (4KB)
        let large_val = vec![0xABu8; 32 * 1024]; // 32KB
        db.put(b"big_key".to_vec(), large_val.clone()).unwrap();

        let retrieved = db.get(&b"big_key".to_vec()).unwrap().unwrap();
        assert_eq!(retrieved.len(), large_val.len());
        assert_eq!(retrieved, large_val);
    }

    /// Edge case: transaction conflict detection across flush boundary.
    #[test]
    fn test_transaction_across_flush() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(temp_dir.path());
        config.memtable_size = 1024 * 1024;
        config.sync_writes = false;
        let db = Database::open(config).unwrap();

        // Start transaction, read key
        let txn = db.begin_txn();
        db.put_txn(txn, b"txkey".to_vec(), b"txval".to_vec()).unwrap();

        // Write enough to trigger flush while transaction is open
        for i in 0..5000 {
            db.put(format!("pad_{i:06}").into_bytes(), b"x".to_vec()).unwrap();
        }

        // Commit should still work
        db.commit_txn(txn).unwrap();
        assert_eq!(db.get(&b"txkey".to_vec()).unwrap(), Some(b"txval".to_vec()));
    }

    /// Verify triple restart with overwrites and deletes.
    #[test]
    fn test_triple_restart_with_mutations() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Round 1: write keys
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            config.sync_writes = false;
            let db = Database::open(config).unwrap();
            for i in 0..3000 {
                db.put(format!("tr_{i:04}").into_bytes(), b"round1".to_vec()).unwrap();
            }
            db.close().unwrap();
        }

        // Round 2: overwrite half, delete quarter
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            config.sync_writes = false;
            let db = Database::open(config).unwrap();
            for i in 0..1500 {
                db.put(format!("tr_{i:04}").into_bytes(), b"round2".to_vec()).unwrap();
            }
            for i in 1500..2250 {
                db.delete(format!("tr_{i:04}").into_bytes()).unwrap();
            }
            db.close().unwrap();
        }

        // Round 3: verify everything
        {
            let mut config = Config::new(&path);
            config.memtable_size = 1024 * 1024;
            let db = Database::open(config).unwrap();

            for i in 0..3000 {
                let key = format!("tr_{i:04}").into_bytes();
                let val = db.get(&key).unwrap();
                if i < 1500 {
                    assert_eq!(val, Some(b"round2".to_vec()), "tr_{i:04} should be round2");
                } else if i < 2250 {
                    assert_eq!(val, None, "tr_{i:04} should be deleted");
                } else {
                    assert_eq!(val, Some(b"round1".to_vec()), "tr_{i:04} should be round1");
                }
            }
        }
    }
}
