use middb_core::MemTable;

fn main() {
    println!("MemTable Demo\n");

    let mut memtable = MemTable::with_threshold(1024);
    println!("Created MemTable with 1KB flush threshold");
    println!("Initial size: {} bytes\n", memtable.approx_size());

    println!("Inserting data...");
    for i in 0..10 {
        let key = format!("user_{:03}", i);
        let value = format!("{{\"id\":{},\"name\":\"User {}\",\"email\":\"user{}@example.com\"}}", i, i, i);
        memtable.put(key.clone(), value.clone()).unwrap();
        println!("  PUT {} => {} bytes", key, value.len());
    }
    println!("Size after inserts: {} bytes", memtable.approx_size());
    println!("Should flush: {}\n", memtable.should_flush());

    println!("Retrieving data:");
    for i in [0, 5, 9, 15] {
        let key = format!("user_{:03}", i);
        match memtable.get(&key) {
            Some(v) => println!("  GET {} => {} bytes", key, v.len()),
            None => println!("  GET {} => None", key),
        }
    }
    println!();

    println!("Deleting keys (creates tombstones):");
    for i in [2, 5, 7] {
        let key = format!("user_{:03}", i);
        memtable.delete(key.clone()).unwrap();
        println!("  DELETE {}", key);
    }
    println!("Size after deletes: {} bytes", memtable.approx_size());
    println!("Entry count (including tombstones): {}\n", memtable.len());

    println!("Retrieving deleted keys:");
    for i in [2, 5, 7] {
        let key = format!("user_{:03}", i);
        match memtable.get(&key) {
            Some(v) => println!("  GET {} => {} (ERROR: should be None)", key, v),
            None => println!("  GET {} => None", key),
        }
    }
    println!();

    println!("All entries (including tombstones):");
    for (key, entry) in memtable.iter() {
        match entry {
            middb_core::memtable::ValueEntry::Value(v) => {
                println!("  {} => VALUE ({} bytes)", key, v.len());
            }
            middb_core::memtable::ValueEntry::Tombstone => {
                println!("  {} => TOMBSTONE", key);
            }
        }
    }
    println!();

    println!("Range query [user_003, user_007):");
    for (key, entry) in memtable.range(&"user_003".to_string(), &"user_007".to_string()) {
        match entry {
            middb_core::memtable::ValueEntry::Value(v) => {
                println!("  {} => VALUE ({} bytes)", key, v.len());
            }
            middb_core::memtable::ValueEntry::Tombstone => {
                println!("  {} => TOMBSTONE", key);
            }
        }
    }
    println!();

    println!("Memory Statistics:");
    println!("  Approximate size: {} bytes", memtable.approx_size());
    println!("  Flush threshold: {} bytes", memtable.flush_threshold());
    println!("  Should flush: {}", memtable.should_flush());
    println!("  Entry count: {} (includes {} tombstones)", memtable.len(), 3);
}
