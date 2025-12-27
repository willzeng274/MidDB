use crate::config::Config;
use crate::memtable::MemTable;
use crate::sstable::{SSTableMetadata, SSTableReader};
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
    sstables: Arc<RwLock<Vec<SSTableMetadata>>>,
    sstable_readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
    sequence: Arc<AtomicU64>,
    next_file_id: Arc<AtomicU64>,
}

impl Database {
    pub fn open(config: Config) -> Result<Self> {
        config.validate().map_err(|e| Error::InvalidConfig(e))?;
        
        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(&config.wal_dir)?;
        
        let wal_path = config.wal_dir.join("wal.log");
        let wal = WalWriter::create(&wal_path)?;
        
        let memtable = MemTable::with_threshold(config.memtable_size);
        
        let sstables = Self::load_sstables(&config.data_dir)?;
        let sstable_readers = Self::load_sstable_readers(&sstables)?;
        
        let sequence = Self::recover_from_wal(&wal_path, &memtable)?;
        
        Ok(Database {
            config,
            memtable: Arc::new(RwLock::new(memtable)),
            wal: Arc::new(RwLock::new(wal)),
            sstables: Arc::new(RwLock::new(sstables)),
            sstable_readers: Arc::new(RwLock::new(sstable_readers)),
            sequence: Arc::new(AtomicU64::new(sequence)),
            next_file_id: Arc::new(AtomicU64::new(1)),
        })
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
        let sstables = self.sstables.read().unwrap();
        
        for metadata in sstables.iter().rev() {
            if !metadata.may_contain(key) {
                continue;
            }
            
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
        let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.config.data_dir.join(format!("sst_{:08}.sst", file_id));
        
        let memtable_to_flush = {
            let mut mt = self.memtable.write().unwrap();
            let new_memtable = MemTable::with_threshold(self.config.memtable_size);
            std::mem::replace(&mut *mt, new_memtable)
        };
        
        let metadata = memtable_to_flush.flush_to_sstable(
            &sstable_path,
            file_id,
            0, // Level 0
            self.config.block_size,
        )?;
        
        let reader = SSTableReader::open(&sstable_path)?;
        
        {
            let mut sstables = self.sstables.write().unwrap();
            sstables.push(metadata);
        }
        
        {
            let mut readers = self.sstable_readers.write().unwrap();
            readers.insert(file_id, reader);
        }
        
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
            
            match entry.entry_type {
                crate::wal::EntryType::Put => {}
                crate::wal::EntryType::Delete => {}
            }
        }
        
        Ok(max_seq + 1)
    }
    
    fn load_sstables(data_dir: &PathBuf) -> Result<Vec<SSTableMetadata>> {
        let sstables = Vec::new();
        
        if !data_dir.exists() {
            return Ok(sstables);
        }
        
        Ok(sstables)
    }
    
    fn load_sstable_readers(
        _sstables: &[SSTableMetadata],
    ) -> Result<HashMap<u64, SSTableReader>> {
        let readers = HashMap::new();
        
        Ok(readers)
    }
    
    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats {
        let memtable = self.memtable.read().unwrap();
        let sstables = self.sstables.read().unwrap();
        
        DatabaseStats {
            memtable_size: memtable.approx_size(),
            memtable_entries: memtable.len(),
            num_sstables: sstables.len(),
            sequence_number: self.sequence.load(Ordering::SeqCst),
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
        
        // Sync WAL
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_database_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path());
        
        let db = Database::open(config).unwrap();
        
        // Put
        db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        db.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        
        // Get
        assert_eq!(db.get(&b"key1".to_vec()).unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(&b"key2".to_vec()).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(&b"key3".to_vec()).unwrap(), None);
        
        // Delete
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
}

