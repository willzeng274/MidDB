use middb_core::{Config, Database};
use tempfile::TempDir;

fn main() {
    println!("MidDB Database Demo\n");
    
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    
    let mut config = Config::new(temp_dir.path());
    config.memtable_size = 1024 * 1024;
    config.block_size = 4096;
    
    let db = Database::open(config).expect("Failed to open database");
    
    println!("Inserting 100 entries");
    for i in 0..100 {
        let key = format!("user_{:04}", i);
        let value = format!(
            "{{\"id\":{},\"name\":\"User {}\",\"email\":\"user{}@example.com\"}}",
            i, i, i
        );
        db.put(key.as_bytes().to_vec(), value.as_bytes().to_vec())
            .expect("Failed to put");
    }
    
    let stats = db.stats();
    println!("\nDatabase Statistics:");
    println!("  MemTable size: {} bytes", stats.memtable_size);
    println!("  MemTable entries: {}", stats.memtable_entries);
    println!("  SSTables: {}", stats.num_sstables);
    println!("  Sequence: {}", stats.sequence_number);
    
    println!("\nReading sample keys:");
    for key in ["user_0000", "user_0025", "user_0050", "user_0075", "user_0099"] {
        match db.get(&key.as_bytes().to_vec()) {
            Ok(Some(value)) => {
                let value_str = String::from_utf8_lossy(&value);
                println!("  {} => {} bytes", key, value_str.len());
            }
            Ok(None) => println!("  {} => Not found", key),
            Err(e) => println!("  {} => Error: {}", key, e),
        }
    }
    
    println!("\nUpdating user_0050");
    let update_key = b"user_0050".to_vec();
    let new_value = b"{\"id\":50,\"name\":\"Updated\",\"email\":\"updated@example.com\"}".to_vec();
    db.put(update_key.clone(), new_value).expect("Failed to update");
    
    println!("\nDeleting user_0025");
    db.delete(b"user_0025".to_vec()).expect("Failed to delete");
    
    match db.get(&b"user_0025".to_vec()) {
        Ok(None) => println!("Key deleted successfully"),
        Ok(Some(_)) => println!("ERROR: Key still exists"),
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\nWriting more data to trigger flush");
    for i in 100..200 {
        let key = format!("batch_{:04}", i);
        let value = format!("value_{}", i).repeat(100);
        db.put(key.as_bytes().to_vec(), value.as_bytes().to_vec())
            .expect("Failed to put");
    }
    
    let final_stats = db.stats();
    println!("\nFinal Statistics:");
    println!("  MemTable entries: {}", final_stats.memtable_entries);
    println!("  SSTables: {}", final_stats.num_sstables);
    println!("  Total writes: {}", final_stats.sequence_number);
    
    db.close().expect("Failed to close database");
}
