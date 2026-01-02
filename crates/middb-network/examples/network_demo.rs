use middb_core::{Config, Database};
use middb_network::{Server, Client};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    println!("Network Layer Demo\n");
    
    let temp_dir = TempDir::new().unwrap();
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).unwrap();
    
    let server = Server::new(db, "127.0.0.1:7878".to_string());
    
    tokio::spawn(async move {
        server.run().await.expect("Server failed");
    });
    
    sleep(Duration::from_millis(100)).await;
    
    println!("Connecting to server at 127.0.0.1:7878");
    let mut client = Client::connect("127.0.0.1:7878").await.expect("Failed to connect");
    
    println!("\nPing server");
    client.ping().await.expect("Ping failed");
    println!("Received pong");
    
    println!("\nPutting key1 => value1");
    client.put(b"key1", b"value1").await.expect("Put failed");
    
    println!("Getting key1");
    match client.get(b"key1").await.expect("Get failed") {
        Some(value) => println!("Received: {:?}", String::from_utf8_lossy(&value)),
        None => println!("Key not found"),
    }
    
    println!("\nPutting key2 => value2");
    client.put(b"key2", b"value2").await.expect("Put failed");
    
    println!("Deleting key1");
    client.delete(b"key1").await.expect("Delete failed");
    
    println!("Getting key1 (should be deleted)");
    match client.get(b"key1").await.expect("Get failed") {
        Some(_) => println!("ERROR: Key still exists"),
        None => println!("Key deleted successfully"),
    }
    
    println!("\nGetting key2");
    match client.get(b"key2").await.expect("Get failed") {
        Some(value) => println!("Received: {:?}", String::from_utf8_lossy(&value)),
        None => println!("Key not found"),
    }
    
    println!("\nNetwork demo complete");
}
