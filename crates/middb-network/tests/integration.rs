use middb_core::{Config, Database};
use middb_network::{Client, Request, Response, Server};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

async fn start_server(addr: &str) -> (TempDir, tokio::task::JoinHandle<()>) {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path());
    let db = Database::open(config).unwrap();
    let server = Server::new(db, addr.to_string());
    let handle = tokio::spawn(async move {
        server.run().await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;
    (dir, handle)
}

#[tokio::test]
async fn test_client_server_kv_ops() {
    let (_dir, handle) = start_server("127.0.0.1:19001").await;
    let mut client = Client::connect("127.0.0.1:19001").await.unwrap();

    client.ping().await.unwrap();

    client.put(b"key1", b"value1").await.unwrap();
    client.put(b"key2", b"value2").await.unwrap();

    assert_eq!(client.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    assert_eq!(client.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    assert_eq!(client.get(b"missing").await.unwrap(), None);

    client.delete(b"key1").await.unwrap();
    assert_eq!(client.get(b"key1").await.unwrap(), None);

    handle.abort();
}

#[tokio::test]
async fn test_batch_operations() {
    let (_dir, handle) = start_server("127.0.0.1:19002").await;
    let mut client = Client::connect("127.0.0.1:19002").await.unwrap();

    let count = client.batch_put(vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
    ]).await.unwrap();
    assert_eq!(count, 3);

    let values = client.batch_get(vec![
        b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"missing".to_vec(),
    ]).await.unwrap();
    assert_eq!(values.len(), 4);
    assert_eq!(values[0], Some(b"1".to_vec()));
    assert_eq!(values[1], Some(b"2".to_vec()));
    assert_eq!(values[2], Some(b"3".to_vec()));
    assert_eq!(values[3], None);

    handle.abort();
}

#[tokio::test]
async fn test_transaction_over_network() {
    let (_dir, handle) = start_server("127.0.0.1:19003").await;
    let mut client = Client::connect("127.0.0.1:19003").await.unwrap();

    let txn_id = client.begin_txn().await.unwrap();
    client.txn_put(txn_id, b"txn_key", b"txn_val").await.unwrap();

    let val = client.txn_get(txn_id, b"txn_key").await.unwrap();
    assert_eq!(val, Some(b"txn_val".to_vec()));

    client.commit_txn(txn_id).await.unwrap();

    let val = client.get(b"txn_key").await.unwrap();
    assert_eq!(val, Some(b"txn_val".to_vec()));

    handle.abort();
}

#[tokio::test]
async fn test_pipeline() {
    let (_dir, handle) = start_server("127.0.0.1:19004").await;
    let mut client = Client::connect("127.0.0.1:19004").await.unwrap();

    let responses = client.pipeline(vec![
        Request::Put { key: b"p1".to_vec(), value: b"v1".to_vec() },
        Request::Put { key: b"p2".to_vec(), value: b"v2".to_vec() },
        Request::Get { key: b"p1".to_vec() },
        Request::Ping,
    ]).await.unwrap();

    assert_eq!(responses.len(), 4);
    assert!(matches!(responses[0], Response::Ok));
    assert!(matches!(responses[1], Response::Ok));
    assert!(matches!(responses[3], Response::Pong));

    handle.abort();
}

#[tokio::test]
async fn test_concurrent_clients() {
    let (_dir, handle) = start_server("127.0.0.1:19005").await;

    let mut handles = vec![];
    for i in 0..5 {
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect("127.0.0.1:19005").await.unwrap();
            for j in 0..20 {
                let key = format!("client{}_{}", i, j).into_bytes();
                let value = format!("val{}_{}", i, j).into_bytes();
                client.put(&key, &value).await.unwrap();
            }
            for j in 0..20 {
                let key = format!("client{}_{}", i, j).into_bytes();
                let val = client.get(&key).await.unwrap();
                assert!(val.is_some());
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    handle.abort();
}
