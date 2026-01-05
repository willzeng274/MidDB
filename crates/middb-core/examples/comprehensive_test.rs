use middb_core::{SkipList, MemTable, memtable::ValueEntry};
use std::collections::BTreeMap;

fn test_skiplist_correctness() {
    println!("SkipList Correctness Tests\n");

    println!("Test 1: Sequential insertion");
    let mut list = SkipList::new();
    for i in 0..100 {
        list.insert(i, i * 2);
    }
    assert_eq!(list.len(), 100);
    for i in 0..100 {
        assert_eq!(list.get(&i), Some(&(i * 2)), "Failed at key {}", i);
    }
    println!("  All 100 sequential inserts and retrievals correct\n");

    println!("Test 2: Random insertion order");
    let mut list = SkipList::new();
    let keys = vec![50, 25, 75, 12, 37, 62, 87, 6, 18, 31, 43, 56, 68, 81, 93];
    for &key in &keys {
        list.insert(key, key);
    }

    let sorted: Vec<_> = list.iter().map(|(k, _)| *k).collect();
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(sorted, expected);
    println!("  Sorted iteration works correctly\n");

    println!("Test 3: Update existing keys");
    let mut list = SkipList::new();
    list.insert(1, "original");
    list.insert(1, "updated");
    assert_eq!(list.get(&1), Some(&"updated"));
    assert_eq!(list.len(), 1);
    println!("  Updates work correctly\n");

    println!("Test 4: Deletions");
    let mut list = SkipList::new();
    for i in 0..10 {
        list.insert(i, i);
    }
    for i in (0..10).step_by(2) {
        list.remove(&i);
    }
    assert_eq!(list.len(), 5);
    for i in (0..10).step_by(2) {
        assert_eq!(list.get(&i), None);
    }
    for i in (1..10).step_by(2) {
        assert_eq!(list.get(&i), Some(&i));
    }
    println!("  Deletions work correctly\n");

    println!("Test 5: Range queries");
    let mut list = SkipList::new();
    for i in 0..100 {
        list.insert(i, i);
    }

    let range_tests = vec![
        (0, 10, 10),
        (25, 75, 50),
        (90, 100, 10),
        (50, 51, 1),
    ];

    for (start, end, expected_count) in range_tests {
        let count = list.range(&start, &end).count();
        assert_eq!(count, expected_count,
                   "Range [{}, {}) expected {} items, got {}",
                   start, end, expected_count, count);
    }
    println!("  Range queries work correctly\n");
}

fn test_memtable_features() {
    println!("MemTable Feature Tests\n");

    println!("Test 1: Basic put/get operations");
    let mut mt = MemTable::new();
    mt.put("key1".to_string(), "value1".to_string()).unwrap();
    mt.put("key2".to_string(), "value2".to_string()).unwrap();
    assert_eq!(mt.get(&"key1".to_string()), Some(&"value1".to_string()));
    assert_eq!(mt.get(&"key2".to_string()), Some(&"value2".to_string()));
    println!("  Basic operations work\n");

    println!("Test 2: Tombstone deletion");
    let mut mt = MemTable::new();
    mt.put("key1".to_string(), "value1".to_string()).unwrap();
    mt.delete("key1".to_string()).unwrap();

    assert_eq!(mt.get(&"key1".to_string()), None);

    let mut tombstone_found = false;
    for (key, entry) in mt.iter() {
        if key == "key1" {
            assert!(matches!(entry, ValueEntry::Tombstone));
            tombstone_found = true;
        }
    }
    assert!(tombstone_found);
    println!("  Tombstones work correctly\n");

    println!("Test 3: Memory tracking");
    let mut mt = MemTable::with_threshold(1000);
    let initial_size = mt.approx_size();
    assert_eq!(initial_size, 0);

    mt.put("key".to_string(), "value".to_string()).unwrap();
    let size_after_insert = mt.approx_size();
    assert!(size_after_insert > 0);

    mt.put("key2".to_string(), "value2".to_string()).unwrap();
    let size_after_second = mt.approx_size();
    assert!(size_after_second > size_after_insert);
    println!("  Memory tracking increases correctly\n");

    println!("Test 4: Flush threshold detection");
    let mut mt = MemTable::with_threshold(100);
    assert!(!mt.should_flush());

    // Add enough data to trigger flush
    for i in 0..20 {
        mt.put(format!("key{}", i), format!("value{}", i)).unwrap();
    }
    assert!(mt.should_flush());
    println!("  Flush threshold detection works\n");

    println!("Test 5: Range queries with mixed entries");
    let mut mt = MemTable::new();
    for i in 0..10 {
        mt.put(format!("key{:02}", i), format!("value{}", i)).unwrap();
    }
    mt.delete("key03".to_string()).unwrap();
    mt.delete("key05".to_string()).unwrap();

    let start_key = "key02".to_string();
    let end_key = "key07".to_string();
    let range_items: Vec<_> = mt.range(&start_key, &end_key).collect();
    assert_eq!(range_items.len(), 5);
    
    let mut value_count = 0;
    let mut tombstone_count = 0;
    for (_, entry) in range_items {
        match entry {
            ValueEntry::Value(_) => value_count += 1,
            ValueEntry::Tombstone => tombstone_count += 1,
        }
    }
    assert_eq!(value_count, 3);
    assert_eq!(tombstone_count, 2);
    println!("  Range queries with tombstones work\n");
}

fn compare_with_btreemap() {
    println!("Comparison with BTreeMap\n");

    let test_size = 1000;

    let mut skiplist = SkipList::new();
    let mut btreemap = BTreeMap::new();

    for i in 0..test_size {
        skiplist.insert(i, i * 2);
        btreemap.insert(i, i * 2);
    }

    println!("Test size: {} elements\n", test_size);

    println!("Verifying all elements match...");
    for i in 0..test_size {
        assert_eq!(skiplist.get(&i), btreemap.get(&i));
    }
    println!("  All {} elements match\n", test_size);

    println!("Verifying iteration order...");
    let skip_items: Vec<_> = skiplist.iter().collect();
    let btree_items: Vec<_> = btreemap.iter().collect();
    assert_eq!(skip_items.len(), btree_items.len());

    for i in 0..skip_items.len() {
        assert_eq!(skip_items[i], btree_items[i]);
    }
    println!("  Iteration order matches\n");

    println!("Verifying range queries...");
    let ranges = vec![(100, 200), (400, 600), (800, 900)];

    for (start, end) in ranges {
        let skip_range: Vec<_> = skiplist.range(&start, &end).collect();
        let btree_range: Vec<_> = btreemap.range(start..end).collect();

        assert_eq!(skip_range.len(), btree_range.len());
        for i in 0..skip_range.len() {
            assert_eq!(skip_range[i], btree_range[i]);
        }
    }
    println!("  All range queries match\n");
}

fn stress_test() {
    println!("Stress Tests\n");

    println!("Test 1: Large dataset (10,000 elements)");
    let mut list = SkipList::new();
    for i in 0..10000 {
        list.insert(i, i);
    }
    assert_eq!(list.len(), 10000);
    println!("  Inserted 10,000 elements\n");

    println!("Test 2: Many updates");
    for i in 0..10000 {
        list.insert(i, i * 2);
    }
    assert_eq!(list.len(), 10000);
    for i in 0..10000 {
        assert_eq!(list.get(&i), Some(&(i * 2)));
    }
    println!("  Updated 10,000 elements\n");

    println!("Test 3: Many deletions");
    for i in (0..10000).step_by(2) {
        list.remove(&i);
    }
    assert_eq!(list.len(), 5000);
    println!("  Deleted 5,000 elements\n");

    println!("Test 4: Edge cases");
    let mut list = SkipList::new();

    assert_eq!(list.get(&1), None);
    assert_eq!(list.remove(&1), None);
    assert_eq!(list.len(), 0);

    list.insert(1, 1);
    assert_eq!(list.len(), 1);
    assert_eq!(list.remove(&1), Some(1));
    assert_eq!(list.len(), 0);

    println!("  Edge cases handled correctly\n");
}

fn main() {
    println!("MidDB Comprehensive Test Suite\n");

    test_skiplist_correctness();
    test_memtable_features();
    compare_with_btreemap();
    stress_test();

    println!("All tests passed");
}
