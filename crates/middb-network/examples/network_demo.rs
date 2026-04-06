use middb_core::{Config, Database};
use middb_network::{Client, Server};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::new(temp_dir.path());
    let db = Database::open(config).unwrap();

    let server = Server::new(db, "127.0.0.1:7878".to_string());
    tokio::spawn(async move {
        server.run().await.expect("Server failed");
    });
    sleep(Duration::from_millis(100)).await;

    let mut client = Client::connect("127.0.0.1:7878").await.expect("Failed to connect");

    client.ping().await.expect("Ping failed");
    println!("Connected and pinged server");

    // KV operations
    client.put(b"key1", b"value1").await.unwrap();
    client.put(b"key2", b"value2").await.unwrap();
    let val = client.get(b"key1").await.unwrap();
    println!("GET key1 = {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Batch operations
    let count = client.batch_put(vec![
        (b"batch_a".to_vec(), b"1".to_vec()),
        (b"batch_b".to_vec(), b"2".to_vec()),
        (b"batch_c".to_vec(), b"3".to_vec()),
    ]).await.unwrap();
    println!("Batch put {count} keys");

    let values = client.batch_get(vec![
        b"batch_a".to_vec(), b"batch_b".to_vec(), b"missing".to_vec(),
    ]).await.unwrap();
    println!("Batch get: {:?}", values.iter().map(|v| {
        v.as_ref().map(|b| String::from_utf8_lossy(b).to_string())
    }).collect::<Vec<_>>());

    // Transactions
    let txn_id = client.begin_txn().await.unwrap();
    client.txn_put(txn_id, b"txn_key", b"txn_value").await.unwrap();
    let txn_val = client.txn_get(txn_id, b"txn_key").await.unwrap();
    println!("TXN GET (before commit) = {:?}", txn_val.map(|v| String::from_utf8_lossy(&v).to_string()));
    client.commit_txn(txn_id).await.unwrap();
    let committed_val = client.get(b"txn_key").await.unwrap();
    println!("GET after commit = {:?}", committed_val.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Pipelining
    use middb_network::Request;
    let responses = client.pipeline(vec![
        Request::Put { key: b"p1".to_vec(), value: b"v1".to_vec() },
        Request::Put { key: b"p2".to_vec(), value: b"v2".to_vec() },
        Request::Get { key: b"p1".to_vec() },
        Request::Get { key: b"p2".to_vec() },
    ]).await.unwrap();
    println!("Pipeline: {} responses", responses.len());

    println!("\nDone");
}
