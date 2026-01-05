use middb_core::SkipList;
use std::collections::BTreeMap;
use std::time::Instant;

fn benchmark_inserts<F: FnMut(i32, i32)>(mut insert_fn: F, name: &str, count: usize) {
    let start = Instant::now();

    for i in 0..count as i32 {
        insert_fn(i, i * 2);
    }

    let duration = start.elapsed();
    println!("{:20} {} inserts: {:?} ({:.2} ops/sec)",
        name, count, duration, count as f64 / duration.as_secs_f64());
}

fn benchmark_lookups<F: FnMut(i32) -> Option<i32>>(mut get_fn: F, name: &str, count: usize) {
    let start = Instant::now();

    for i in 0..count as i32 {
        let _ = get_fn(i);
    }

    let duration = start.elapsed();
    println!("{:20} {} lookups: {:?} ({:.2} ops/sec)",
        name, count, duration, count as f64 / duration.as_secs_f64());
}

fn benchmark_iteration<I: Iterator<Item = (i32, i32)>>(iter: I, name: &str) {
    let start = Instant::now();
    let count = iter.count();
    let duration = start.elapsed();
    println!("{:20} iterate {} items: {:?} ({:.2} items/sec)",
        name, count, duration, count as f64 / duration.as_secs_f64());
}

fn main() {
    println!("=== Performance Comparison: SkipList vs BTreeMap ===\n");

    let sizes = vec![1_000, 10_000, 100_000];

    for &size in &sizes {
        println!("--- Dataset size: {} ---", size);

        // Insert benchmarks
        println!("\nInsertion:");
        let mut skip_list = SkipList::new();
        benchmark_inserts(
            |k, v| skip_list.insert(k, v),
            "SkipList",
            size
        );

        let mut btree = BTreeMap::new();
        benchmark_inserts(
            |k, v| { btree.insert(k, v); },
            "BTreeMap",
            size
        );

        // Lookup benchmarks
        println!("\nLookup:");
        benchmark_lookups(
            |k| skip_list.get(&k).copied(),
            "SkipList",
            size
        );

        benchmark_lookups(
            |k| btree.get(&k).copied(),
            "BTreeMap",
            size
        );

        // Iteration benchmarks
        println!("\nIteration:");
        benchmark_iteration(
            skip_list.iter().map(|(k, v)| (*k, *v)),
            "SkipList"
        );

        benchmark_iteration(
            btree.iter().map(|(k, v)| (*k, *v)),
            "BTreeMap"
        );

        println!();
    }

    // Memory overhead comparison
    println!("=== Memory Overhead Comparison ===");
    println!("SkipList:");
    println!("  - Node overhead: ~40 bytes per node (estimated)");
    println!("  - Additional pointers: varies by height (avg 1.33 levels with p=0.25)");
    println!("  - Total overhead: ~40-80 bytes per entry");
    println!("\nBTreeMap:");
    println!("  - Node overhead: ~24 bytes per node (estimated)");
    println!("  - B-tree internal nodes: varies by tree structure");
    println!("  - Total overhead: ~40-60 bytes per entry");

    println!("\n=== Characteristics ===");
    println!("SkipList:");
    println!("  ✓ O(log n) average case insert, search, delete");
    println!("  ✓ Simple to implement");
    println!("  ✓ Good cache locality for sequential access");
    println!("  ✗ Probabilistic balancing (can have worst-case O(n))");
    println!("  ✗ Higher memory overhead than balanced trees");

    println!("\nBTreeMap:");
    println!("  ✓ O(log n) guaranteed insert, search, delete");
    println!("  ✓ Excellent cache locality");
    println!("  ✓ Deterministic performance");
    println!("  ✗ More complex implementation");
    println!("  ✗ Rebalancing overhead on modifications");
}
