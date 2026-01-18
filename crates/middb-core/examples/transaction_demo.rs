use middb_core::{Config, Database};
use tempfile::TempDir;

fn main() {
    println!("Transaction Demo\n");

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).expect("Failed to open database");

    println!("Basic transaction:");
    let txn = db.begin_txn();
    db.put_txn(txn, b"alice".to_vec(), b"1000".to_vec()).unwrap();
    db.put_txn(txn, b"bob".to_vec(), b"500".to_vec()).unwrap();

    println!("  get(alice) in txn = {:?}", 
        db.get_txn(txn, &b"alice".to_vec()).unwrap().map(|v| String::from_utf8_lossy(&v).to_string()));
    println!("  get(alice) outside = {:?}", db.get(&b"alice".to_vec()).unwrap());

    db.commit_txn(txn).unwrap();
    println!("  get(alice) after commit = {:?}", 
        db.get(&b"alice".to_vec()).unwrap().map(|v| String::from_utf8_lossy(&v).to_string()));
    println!();

    println!("Abort:");
    let txn2 = db.begin_txn();
    db.put_txn(txn2, b"alice".to_vec(), b"9999".to_vec()).unwrap();
    db.abort_txn(txn2).unwrap();
    println!("  alice after abort = {:?}", 
        db.get(&b"alice".to_vec()).unwrap().map(|v| String::from_utf8_lossy(&v).to_string()));
    println!();

    println!("Delete in transaction:");
    let txn3 = db.begin_txn();
    db.delete_txn(txn3, b"bob".to_vec()).unwrap();
    db.commit_txn(txn3).unwrap();
    println!("  bob after delete = {:?}", db.get(&b"bob".to_vec()).unwrap());
    println!();

    println!("Concurrent transactions:");
    let t1 = db.begin_txn();
    let t2 = db.begin_txn();
    db.put_txn(t1, b"from_t1".to_vec(), b"hello".to_vec()).unwrap();
    db.put_txn(t2, b"from_t2".to_vec(), b"world".to_vec()).unwrap();
    db.commit_txn(t1).unwrap();
    db.commit_txn(t2).unwrap();
    println!("  from_t1 = {:?}", db.get(&b"from_t1".to_vec()).unwrap().map(|v| String::from_utf8_lossy(&v).to_string()));
    println!("  from_t2 = {:?}", db.get(&b"from_t2".to_vec()).unwrap().map(|v| String::from_utf8_lossy(&v).to_string()));

    let stats = db.stats();
    println!("\nStats: {} entries, seq={}", stats.memtable_entries, stats.sequence_number);

    db.close().expect("Failed to close");
}
