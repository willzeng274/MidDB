use middb_core::SkipList;
use std::collections::HashMap;

fn analyze_skiplist_structure<K: Ord + Default + std::fmt::Debug, V: Default>(
    list: &SkipList<K, V>,
    name: &str,
) {
    println!("=== {} ===", name);
    println!("Total elements: {}", list.len());

    // We can't directly access internal structure, but we can analyze behavior
    println!("\nIterating through elements:");
    let mut count = 0;
    for (key, _) in list.iter() {
        print!("{:?} ", key);
        count += 1;
        if count % 10 == 0 {
            println!();
        }
    }
    if count % 10 != 0 {
        println!();
    }
    println!();
}

fn demonstrate_height_distribution() {
    println!("=== Height Distribution Analysis ===\n");
    println!("With P=0.25, we expect:");
    println!("  Level 1: 100% of nodes");
    println!("  Level 2: ~25% of nodes");
    println!("  Level 3: ~6.25% of nodes");
    println!("  Level 4: ~1.56% of nodes");
    println!("  etc.\n");

    // Simulate random height generation
    let iterations = 10000;
    let mut height_counts = HashMap::new();

    for _ in 0..iterations {
        let height = random_height();
        *height_counts.entry(height).or_insert(0) += 1;
    }

    println!("Actual distribution from {} nodes:", iterations);
    let mut heights: Vec<_> = height_counts.keys().collect();
    heights.sort();

    for height in heights {
        let count = height_counts[height];
        let percentage = (count as f64 / iterations as f64) * 100.0;
        let bar = "█".repeat((percentage / 2.0) as usize);
        println!("  Level {:2}: {:5} nodes ({:5.2}%) {}",
                 height, count, percentage, bar);
    }
    println!();
}

fn random_height() -> usize {
    use std::cell::Cell;
    thread_local! {
        static SEED: Cell<u64> = Cell::new(12345);
    }

    const P: f64 = 0.25;
    const MAX_HEIGHT: usize = 16;

    let mut height = 1;
    while height < MAX_HEIGHT {
        let rand = SEED.with(|seed| {
            let s = seed.get();
            let next = s.wrapping_mul(1103515245).wrapping_add(12345);
            seed.set(next);
            ((next / 65536) % 32768) as f64 / 32768.0
        });

        if rand < P {
            height += 1;
        } else {
            break;
        }
    }
    height
}

fn main() {
    println!("=== SkipList Visualization Example ===\n");

    // Demonstrate height distribution
    demonstrate_height_distribution();

    // Create skip lists with different sizes
    println!("=== Building Skip Lists ===\n");

    // Small skip list
    let mut small_list = SkipList::new();
    for i in 1..=20 {
        small_list.insert(i, i * 10);
    }
    analyze_skiplist_structure(&small_list, "Small List (20 elements)");

    // Medium skip list
    let mut medium_list = SkipList::new();
    for i in 1..=100 {
        medium_list.insert(i, i * 10);
    }
    analyze_skiplist_structure(&medium_list, "Medium List (100 elements)");

    // Demonstrate range queries at different granularities
    println!("=== Range Query Examples ===\n");

    println!("Range [10, 15):");
    for (k, v) in small_list.range(&10, &15) {
        println!("  {} => {}", k, v);
    }
    println!();

    println!("Range [50, 60):");
    for (k, v) in medium_list.range(&50, &60) {
        println!("  {} => {}", k, v);
    }
    println!();

    // Demonstrate operations
    println!("=== Operation Visualization ===\n");

    let mut demo_list = SkipList::new();

    println!("Inserting: 5, 2, 8, 1, 9, 3");
    for &val in &[5, 2, 8, 1, 9, 3] {
        demo_list.insert(val, val * 100);
        print!("After inserting {}: ", val);
        for (k, _) in demo_list.iter() {
            print!("{} ", k);
        }
        println!();
    }
    println!();

    println!("Removing: 2, 8");
    for &val in &[2, 8] {
        demo_list.remove(&val);
        print!("After removing {}: ", val);
        for (k, _) in demo_list.iter() {
            print!("{} ", k);
        }
        println!();
    }
    println!();

    // Performance characteristics visualization
    println!("=== Performance Characteristics ===\n");

    let sizes = vec![10, 100, 1000, 10000];
    println!("Expected lookup time (O(log n)):");
    println!("{:>10} | {:>15}", "Size", "Avg Comparisons");
    println!("{:-<10}-+-{:-<15}", "", "");

    for size in sizes {
        let expected_comparisons = (size as f64).log2();
        println!("{:>10} | {:>15.2}", size, expected_comparisons);
    }
    println!();

    println!("With P=0.25:");
    println!("  - Average node height: 1.33 levels");
    println!("  - Average pointers per node: 1.33");
    println!("  - Space overhead: ~1.33x compared to linked list");
    println!("  - Expected search time: O(log n)");
    println!("  - Expected insert time: O(log n)");
    println!("  - Expected delete time: O(log n)");
}
