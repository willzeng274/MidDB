use middb_core::SkipList;

fn main() {
    println!("SkipList Demo\n");

    let mut list = SkipList::new();
    println!("Created empty skip list");
    println!("Length: {}, Empty: {}\n", list.len(), list.is_empty());

    // Insert some elements
    println!("Inserting elements...");
    for i in [5, 2, 8, 1, 9, 3, 7, 4, 6] {
        list.insert(i, format!("value_{}", i));
        println!("  Inserted key={}, value=value_{}", i, i);
    }
    println!("Length after inserts: {}\n", list.len());

    println!("Retrieving elements:");
    for i in [1, 5, 9, 10] {
        match list.get(&i) {
            Some(v) => println!("  get({}) = {}", i, v),
            None => println!("  get({}) = None", i),
        }
    }
    println!();

    println!("Iterating over all elements:");
    for (key, value) in list.iter() {
        println!("  {} => {}", key, value);
    }
    println!();

    println!("Range query [3, 7):");
    for (key, value) in list.range(&3, &7) {
        println!("  {} => {}", key, value);
    }
    println!();

    println!("Updating key=5 with new value...");
    list.insert(5, "UPDATED_VALUE".to_string());
    println!("  get(5) = {}", list.get(&5).unwrap());
    println!("  Length: {}\n", list.len());

    println!("Removing elements:");
    for i in [2, 5, 8] {
        match list.remove(&i) {
            Some(v) => println!("  Removed key={}, value={}", i, v),
            None => println!("  Key={} not found", i),
        }
    }
    println!("Length after removals: {}\n", list.len());

    println!("Verifying removals:");
    for i in [2, 5, 8] {
        match list.get(&i) {
            Some(v) => println!("  get({}) = {} (ERROR: should be None)", i, v),
            None => println!("  get({}) = None", i),
        }
    }
    println!();

    println!("Final list contents:");
    for (key, value) in list.iter() {
        println!("  {} => {}", key, value);
    }
}
