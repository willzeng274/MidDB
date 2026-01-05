use middb_core::{MemTable, sstable::SSTableReader};
use tempfile::NamedTempFile;

fn main() {
    println!("LSM Tree Component Demo\n");
    
    println!("Creating MemTable (threshold: 1 MB)");
    let mut memtable = MemTable::with_threshold(1024 * 1024);
    
    println!("\nInserting 50 entries into MemTable");
    for i in 0..50 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        memtable.put(key, value).unwrap();
    }
    println!("MemTable size: {} bytes", memtable.approx_size());
    println!("MemTable entries: {}", memtable.len());
    
    println!("\nReading from MemTable:");
    for key in ["key000", "key025", "key049"] {
        match memtable.get(&key.to_string()) {
            Some(value) => println!("  {} => {}", key, value),
            None => println!("  {} => Not found", key),
        }
    }
    
    println!("\nDeleting key025 (creates tombstone)");
    memtable.delete("key025".to_string()).unwrap();
    match memtable.get(&"key025".to_string()) {
        Some(_) => println!("ERROR: Deleted key still accessible"),
        None => println!("Deleted key now returns None"),
    }
    
    println!("\nFlushing MemTable to SSTable");
    let temp_file = NamedTempFile::new().unwrap();
    let sstable_path = temp_file.path();
    
    let metadata = memtable
        .flush_to_sstable(sstable_path, 1, 0, 4096)
        .unwrap();
    
    println!("SSTable created at: {:?}", sstable_path);
    println!("File size: {} bytes", metadata.file_size);
    println!("Entries: {}", metadata.num_entries);
    println!("Key range: {} to {}", 
        String::from_utf8_lossy(&metadata.smallest_key),
        String::from_utf8_lossy(&metadata.largest_key));
    
    println!("\nReading from SSTable:");
    let reader = SSTableReader::open(sstable_path).unwrap();
    
    for key in ["key000", "key025", "key049"] {
        match reader.get(key.as_bytes()) {
            Ok(Some(value)) => {
                if value == b"\x00TOMBSTONE" {
                    println!("  {} => TOMBSTONE", key);
                } else {
                    println!("  {} => {}", key, String::from_utf8_lossy(&value));
                }
            }
            Ok(None) => println!("  {} => Not found", key),
            Err(e) => println!("  {} => Error: {}", key, e),
        }
    }
    
    println!("\nBloom filter test:");
    let mut bloom_rejections = 0;
    let start = std::time::Instant::now();
    
    for i in 1000..1100 {
        let key = format!("nonexistent{:03}", i);
        match reader.get(key.as_bytes()) {
            Ok(None) => bloom_rejections += 1,
            _ => {}
        }
    }
    
    let duration = start.elapsed();
    println!("Checked 100 non-existent keys in {:?}", duration);
    println!("Bloom filter prevented {} disk reads", bloom_rejections);
    
    println!("\nSSTable iteration:");
    let mut iter = reader.iter().unwrap();
    
    let mut count = 0;
    let mut sample_keys = Vec::new();
    
    while iter.valid() {
        if count < 5 || count >= 48 {
            sample_keys.push((
                String::from_utf8_lossy(iter.key().unwrap()).to_string(),
                iter.value().map(|v| v.len()).unwrap_or(0),
            ));
        }
        iter.next().unwrap();
        count += 1;
    }
    
    println!("Iterated through {} entries", count);
    println!("Sample entries:");
    for (key, value_len) in &sample_keys[..5.min(sample_keys.len())] {
        println!("  {} ({} bytes)", key, value_len);
    }
    if sample_keys.len() > 5 {
        println!("  ...");
        for (key, value_len) in &sample_keys[5..] {
            println!("  {} ({} bytes)", key, value_len);
        }
    }
}
