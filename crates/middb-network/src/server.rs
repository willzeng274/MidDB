use crate::protocol::{Frame, FramePayload, Request, Response, MAX_FRAME_SIZE};
use middb_core::Database;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};

const MAX_CONCURRENT_REQUESTS: usize = 256;

pub struct Server {
    db: Arc<Database>,
    addr: String,
}

impl Server {
    pub fn new(db: Database, addr: String) -> Self {
        Server {
            db: Arc::new(db),
            addr,
        }
    }

    pub fn from_arc(db: Arc<Database>, addr: String) -> Self {
        Server { db, addr }
    }

    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;

        loop {
            let (socket, _addr) = listener.accept().await?;
            let db = Arc::clone(&self.db);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, db).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(socket: TcpStream, db: Arc<Database>) -> io::Result<()> {
    let (mut reader, writer) = socket.into_split();
    let mut writer = BufWriter::new(writer);
    let (tx, mut rx) = mpsc::channel::<Frame>(MAX_CONCURRENT_REQUESTS);
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));

    let write_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let data = match frame.encode() {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Encode error: {}", e);
                    continue;
                }
            };
            if writer.write_u32(data.len() as u32).await.is_err() { break; }
            if writer.write_all(&data).await.is_err() { break; }
            if writer.flush().await.is_err() { break; }
        }
    });

    loop {
        let len = match reader.read_u32().await {
            Ok(len) => len as usize,
            Err(_) => break,
        };

        if len == 0 || len > MAX_FRAME_SIZE {
            break;
        }

        let mut buf = vec![0u8; len];
        if reader.read_exact(&mut buf).await.is_err() {
            break;
        }

        let frame = match Frame::decode(&buf) {
            Ok(f) => f,
            Err(_) => break,
        };

        let request_id = frame.request_id;
        let request = match frame.payload {
            FramePayload::Request(req) => req,
            _ => continue,
        };

        let db = Arc::clone(&db);
        let tx = tx.clone();
        let permit = Arc::clone(&semaphore);

        tokio::spawn(async move {
            let _permit = permit.acquire().await;
            let response = handle_request(&db, request);
            let _ = tx.send(Frame::response(request_id, response)).await;
        });
    }

    drop(tx);
    let _ = write_task.await;
    Ok(())
}

fn handle_request(db: &Arc<Database>, request: Request) -> Response {
    match request {
        Request::Get { key } => match db.get(&key) {
            Ok(value) => Response::Value(value),
            Err(e) => Response::Error(e.to_string()),
        },
        Request::Put { key, value } => match db.put(key, value) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::Delete { key } => match db.delete(key) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::BatchGet { keys } => {
            let values: Vec<Option<Vec<u8>>> = keys
                .iter()
                .map(|k| db.get(k).unwrap_or(None))
                .collect();
            Response::Values(values)
        },
        Request::BatchPut { pairs } => {
            let count = pairs.len();
            for (key, value) in pairs {
                if let Err(e) = db.put(key, value) {
                    return Response::Error(e.to_string());
                }
            }
            Response::BatchOk { count }
        },
        Request::Query { sql } => handle_query(db, &sql),
        Request::BeginTxn => {
            let txn_id = db.begin_txn();
            Response::TxnStarted { txn_id }
        },
        Request::CommitTxn { txn_id } => match db.commit_txn(txn_id) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::AbortTxn { txn_id } => match db.abort_txn(txn_id) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::TxnGet { txn_id, key } => match db.get_txn(txn_id, &key) {
            Ok(value) => Response::Value(value),
            Err(e) => Response::Error(e.to_string()),
        },
        Request::TxnPut { txn_id, key, value } => match db.put_txn(txn_id, key, value) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::TxnDelete { txn_id, key } => match db.delete_txn(txn_id, key) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e.to_string()),
        },
        Request::Ping => Response::Pong,
    }
}

fn handle_query(db: &Arc<Database>, sql: &str) -> Response {
    use middb_query::{Executor, Planner, SqlParser};

    let logical_plan = match SqlParser::parse(sql) {
        Ok(plan) => plan,
        Err(e) => return Response::Error(format!("Parse error: {}", e)),
    };

    let planner = Planner::new();
    let physical_plan = planner.to_physical(logical_plan);

    let executor = Executor::with_database(Arc::clone(db));
    match executor.execute(physical_plan) {
        Ok(rows) => {
            let columns: Vec<String> = if let Some(first) = rows.first() {
                first.column_order().to_vec()
            } else {
                vec![]
            };

            let result_rows: Vec<Vec<Option<String>>> = rows
                .iter()
                .map(|row| {
                    columns.iter().map(|col| {
                        row.get_column(col).map(|v| format!("{:?}", v))
                    }).collect()
                })
                .collect();

            Response::QueryResult { columns, rows: result_rows }
        }
        Err(e) => Response::Error(format!("Execute error: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use middb_core::{Config, Database};
    use tempfile::TempDir;

    fn test_db() -> Arc<Database> {
        let dir = TempDir::new().unwrap();
        let config = Config::new(dir.path());
        Arc::new(Database::open(config).unwrap())
    }

    #[test]
    fn test_handle_put_get() {
        let db = test_db();
        let resp = handle_request(&db, Request::Put {
            key: b"k1".to_vec(), value: b"v1".to_vec(),
        });
        assert!(matches!(resp, Response::Ok));

        let resp = handle_request(&db, Request::Get { key: b"k1".to_vec() });
        match resp {
            Response::Value(Some(v)) => assert_eq!(v, b"v1"),
            _ => panic!("Expected value"),
        }
    }

    #[test]
    fn test_handle_batch_get() {
        let db = test_db();
        db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"b".to_vec(), b"2".to_vec()).unwrap();

        let resp = handle_request(&db, Request::BatchGet {
            keys: vec![b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()],
        });
        match resp {
            Response::Values(vals) => {
                assert_eq!(vals.len(), 3);
                assert_eq!(vals[0], Some(b"1".to_vec()));
                assert_eq!(vals[1], Some(b"2".to_vec()));
                assert_eq!(vals[2], None);
            }
            _ => panic!("Expected values"),
        }
    }

    #[test]
    fn test_handle_batch_put() {
        let db = test_db();
        let resp = handle_request(&db, Request::BatchPut {
            pairs: vec![
                (b"x".to_vec(), b"1".to_vec()),
                (b"y".to_vec(), b"2".to_vec()),
            ],
        });
        assert!(matches!(resp, Response::BatchOk { count: 2 }));
        assert_eq!(db.get(&b"x".to_vec()).unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(&b"y".to_vec()).unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_handle_txn_lifecycle() {
        let db = test_db();

        let resp = handle_request(&db, Request::BeginTxn);
        let txn_id = match resp {
            Response::TxnStarted { txn_id } => txn_id,
            _ => panic!("Expected TxnStarted"),
        };

        let resp = handle_request(&db, Request::TxnPut {
            txn_id, key: b"tk".to_vec(), value: b"tv".to_vec(),
        });
        assert!(matches!(resp, Response::Ok));

        let resp = handle_request(&db, Request::TxnGet { txn_id, key: b"tk".to_vec() });
        match resp {
            Response::Value(Some(v)) => assert_eq!(v, b"tv"),
            _ => panic!("Expected value"),
        }

        let resp = handle_request(&db, Request::CommitTxn { txn_id });
        assert!(matches!(resp, Response::Ok));

        assert_eq!(db.get(&b"tk".to_vec()).unwrap(), Some(b"tv".to_vec()));
    }

    #[test]
    fn test_handle_ping() {
        let db = test_db();
        assert!(matches!(handle_request(&db, Request::Ping), Response::Pong));
    }
}
