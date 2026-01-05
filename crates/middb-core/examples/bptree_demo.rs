use middb_core::BPTree;

fn main() {
    println!("B+ Tree Demo\n");

    let mut tree = BPTree::<4, _, _>::new();
    println!("Created B+ tree with FANOUT=4");
    println!("Empty: {}, Len: {}\n", tree.is_empty(), tree.len());

    println!("Inserting elements...");
    for i in [5, 2, 8, 1, 9, 3, 7, 4, 6] {
        tree.insert(i, format!("value_{}", i));
        println!("  Inserted {} => value_{}", i, i);
    }
    println!("Length: {}\n", tree.len());

    println!("\nGet operations:");
    for i in [1, 5, 9, 10] {
        match tree.get(&i) {
            Some(v) => println!("  get({}) = {}", i, v),
            None => println!("  get({}) = None", i),
        }
    }

    println!("\nUpdate existing:");
    tree.insert(5, "UPDATED".to_string());
    println!("  get(5) = {}", tree.get(&5).unwrap());
    println!("  Length: {}\n", tree.len());

    println!("Remove operations:");
    for i in [2, 5, 8] {
        match tree.remove(&i) {
            Some(v) => println!("  Removed {} => {}", i, v),
            None => println!("  Key {} not found", i),
        }
    }
    println!("Length after removals: {}\n", tree.len());

    println!("Range query [3, 8):");
    let range: Vec<_> = tree.range(&3, &8).collect();
    println!("  Result: {:?}", range);
}
