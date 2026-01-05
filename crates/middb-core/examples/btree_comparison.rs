use middb_core::{BPTree, MemTable};

fn main() {
    println!("B+Tree vs SkipList Comparison\n");
    
    let test_data = vec![
        ("apple", "A red fruit"),
        ("banana", "A yellow fruit"),
        ("cherry", "A small red fruit"),
        ("date", "A sweet fruit"),
        ("elderberry", "A dark purple berry"),
        ("fig", "A soft sweet fruit"),
        ("grape", "A small round fruit"),
        ("honeydew", "A melon"),
    ];
    
    println!("Test dataset: {} entries\n", test_data.len());
    
    // B+Tree demonstration
    println!("=== B+Tree (FANOUT=4) ===");
    let mut btree = BPTree::<4, _, _>::new();
    
    println!("Inserting data...");
    for (key, value) in &test_data {
        btree.insert(*key, *value);
    }
    println!("Inserted {} entries\n", btree.len());
    
    // Point lookups
    println!("Point lookups:");
    for key in ["apple", "date", "honeydew", "nonexistent"] {
        match btree.get(&key) {
            Some(value) => println!("  {} => {}", key, value),
            None => println!("  {} => Not found", key),
        }
    }
    println!();
    
    // Range query
    println!("Range query [cherry, grape):");
    for (key, value) in btree.range(&"cherry", &"grape") {
        println!("  {} => {}", key, value);
    }
    println!();
    
    // Iteration
    println!("Full iteration (sorted):");
    for (key, value) in btree.iter() {
        println!("  {} => {}", key, value);
    }
    println!();
    
    // MemTable (SkipList) demonstration
    println!("=== MemTable (SkipList-backed) ===");
    let mut memtable = MemTable::new();
    
    println!("Inserting data...");
    for (key, value) in &test_data {
        memtable
            .put(key.to_string(), value.to_string())
            .unwrap();
    }
    println!("Inserted {} entries", memtable.len());
    println!("Approximate size: {} bytes\n", memtable.approx_size());
    
    // Point lookups
    println!("Point lookups:");
    for key in ["apple", "date", "honeydew", "nonexistent"] {
        match memtable.get(&key.to_string()) {
            Some(value) => println!("  {} => {}", key, value),
            None => println!("  {} => Not found", key),
        }
    }
    println!();
    
    // Range query
    println!("Range query [cherry, grape):");
    for (key, entry) in memtable.range(&"cherry".to_string(), &"grape".to_string()) {
        match entry {
            middb_core::ValueEntry::Value(value) => {
                println!("  {} => {}", key, value);
            }
            middb_core::ValueEntry::Tombstone => {
                println!("  {} => TOMBSTONE", key);
            }
        }
    }
    println!();
    
    // Comparison
    println!("=== Comparison ===");
    println!();
    println!("B+Tree:");
    println!("  ✓ Excellent cache locality (page-based)");
    println!("  ✓ Guaranteed O(log n) operations");
    println!("  ✓ Low height (fanout controls tree depth)");
    println!("  ✓ Ideal for disk-based indexes");
    println!("  ✗ More complex implementation");
    println!();
    println!("SkipList (in MemTable):");
    println!("  ✓ Simpler implementation");
    println!("  ✓ O(log n) expected time");
    println!("  ✓ Better for in-memory operations");
    println!("  ✓ No rebalancing needed");
    println!("  ✗ Probabilistic (can degrade to O(n) in theory)");
    println!("  ✗ Cache-unfriendly (pointer chasing)");
    println!();
    println!("Use Case:");
    println!("  • B+Tree: Secondary indexes on disk");
    println!("  • SkipList: In-memory MemTable for writes");
}
