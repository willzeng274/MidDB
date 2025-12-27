use middb_core::{Config, Database};
use tempfile::TempDir;

#[test]
fn test_database_basic_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).unwrap();
    
    db.put(b"test_key".to_vec(), b"test_value".to_vec()).unwrap();
    
    let value = db.get(&b"test_key".to_vec()).unwrap();
    assert_eq!(value, Some(b"test_value".to_vec()));
    
    db.close().unwrap();
}

#[test]
fn test_database_delete() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).unwrap();
    
    db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    assert_eq!(db.get(&b"key1".to_vec()).unwrap(), Some(b"value1".to_vec()));
    
    db.delete(b"key1".to_vec()).unwrap();
    assert_eq!(db.get(&b"key1".to_vec()).unwrap(), None);
    
    db.close().unwrap();
}

#[test]
fn test_database_update() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).unwrap();
    
    db.put(b"key".to_vec(), b"value1".to_vec()).unwrap();
    db.put(b"key".to_vec(), b"value2".to_vec()).unwrap();
    
    assert_eq!(db.get(&b"key".to_vec()).unwrap(), Some(b"value2".to_vec()));
    
    db.close().unwrap();
}

#[test]
fn test_database_multiple_keys() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).unwrap();
    
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        db.put(key.into_bytes(), value.into_bytes()).unwrap();
    }
    
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let expected_value = format!("value{}", i);
        let actual_value = db.get(&key.into_bytes()).unwrap();
        assert_eq!(actual_value, Some(expected_value.into_bytes()));
    }
    
    db.close().unwrap();
}

#[test]
fn test_sstable_write_and_read() {
    use middb_core::sstable::{SSTableWriter, SSTableReader};
    use tempfile::NamedTempFile;
    
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    {
        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        let metadata = writer.finish(1, 0).unwrap();
        assert_eq!(metadata.num_entries, 10);
    }
    
    let reader = SSTableReader::open(path).unwrap();
    
    for i in 0..10 {
        let key = format!("key{}", i);
        let expected_value = format!("value{}", i);
        let actual_value = reader.get(key.as_bytes()).unwrap();
        assert_eq!(actual_value, Some(expected_value.into_bytes()));
    }
}

#[test]
fn test_wal_durability() {
    use middb_core::wal::{WalWriter, WalReader, WalEntry};
    use tempfile::NamedTempFile;
    
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    
    {
        let mut writer = WalWriter::create(path).unwrap();
        writer.append(&WalEntry::put(1, b"key1".to_vec(), b"value1".to_vec())).unwrap();
        writer.append(&WalEntry::put(2, b"key2".to_vec(), b"value2".to_vec())).unwrap();
        writer.sync().unwrap();
    }
    
    {
        let mut reader = WalReader::open(path).unwrap();
        let entries = reader.read_all().unwrap();
        
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"key1");
        assert_eq!(entries[0].value, Some(b"value1".to_vec()));
        assert_eq!(entries[1].key, b"key2");
        assert_eq!(entries[1].value, Some(b"value2".to_vec()));
    }
}

#[test]
fn test_bloom_filter_integration() {
    use middb_core::bloom::BloomFilter;
    
    let mut filter = BloomFilter::new(100, 10);
    
    for i in 0..100 {
        let key = format!("key{}", i);
        filter.insert(key.as_bytes());
    }
    
    for i in 0..100 {
        let key = format!("key{}", i);
        assert!(filter.may_contain(key.as_bytes()));
    }
    
    let mut false_positives = 0;
    for i in 100..200 {
        let key = format!("key{}", i);
        if filter.may_contain(key.as_bytes()) {
            false_positives += 1;
        }
    }
    
    assert!(false_positives < 5);
}

#[test]
fn test_storage_page_operations() {
    use middb_core::storage::{Page, MemStorage};
    
    let mut storage = MemStorage::new();
    
    let page_id = storage.allocate_page().unwrap();
    let mut page = Page::new();
    
    page.write_at(0, b"test data").unwrap();
    storage.write_page(page_id, &page).unwrap();
    
    let read_page = storage.read_page(page_id).unwrap();
    assert_eq!(read_page.get_slice(0, 9).unwrap(), b"test data");
}
