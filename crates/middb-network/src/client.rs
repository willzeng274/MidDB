use crate::protocol::{Frame, FramePayload, Request, Response, MAX_FRAME_SIZE};
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Client {
    stream: TcpStream,
    next_id: AtomicU32,
}

impl Client {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Client {
            stream,
            next_id: AtomicU32::new(1),
        })
    }

    fn next_request_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let resp = self.send_request(Request::Get { key: key.to_vec() }).await?;
        match resp {
            Response::Value(value) => Ok(value),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let resp = self.send_request(Request::Put {
            key: key.to_vec(), value: value.to_vec(),
        }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let resp = self.send_request(Request::Delete { key: key.to_vec() }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> io::Result<Vec<Option<Vec<u8>>>> {
        let resp = self.send_request(Request::BatchGet { keys }).await?;
        match resp {
            Response::Values(values) => Ok(values),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn batch_put(&mut self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> io::Result<usize> {
        let resp = self.send_request(Request::BatchPut { pairs }).await?;
        match resp {
            Response::BatchOk { count } => Ok(count),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn query(&mut self, sql: &str) -> io::Result<QueryResult> {
        let resp = self.send_request(Request::Query { sql: sql.to_string() }).await?;
        match resp {
            Response::QueryResult { columns, rows } => Ok(QueryResult { columns, rows }),
            Response::Ok => Ok(QueryResult { columns: vec![], rows: vec![] }),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn begin_txn(&mut self) -> io::Result<u64> {
        let resp = self.send_request(Request::BeginTxn).await?;
        match resp {
            Response::TxnStarted { txn_id } => Ok(txn_id),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn commit_txn(&mut self, txn_id: u64) -> io::Result<()> {
        let resp = self.send_request(Request::CommitTxn { txn_id }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn abort_txn(&mut self, txn_id: u64) -> io::Result<()> {
        let resp = self.send_request(Request::AbortTxn { txn_id }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn txn_get(&mut self, txn_id: u64, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let resp = self.send_request(Request::TxnGet { txn_id, key: key.to_vec() }).await?;
        match resp {
            Response::Value(value) => Ok(value),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn txn_put(&mut self, txn_id: u64, key: &[u8], value: &[u8]) -> io::Result<()> {
        let resp = self.send_request(Request::TxnPut {
            txn_id, key: key.to_vec(), value: value.to_vec(),
        }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn txn_delete(&mut self, txn_id: u64, key: &[u8]) -> io::Result<()> {
        let resp = self.send_request(Request::TxnDelete { txn_id, key: key.to_vec() }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn ping(&mut self) -> io::Result<()> {
        let resp = self.send_request(Request::Ping).await?;
        match resp {
            Response::Pong => Ok(()),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Expected pong")),
        }
    }

    pub async fn replicate_write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let resp = self.send_request(Request::ReplicateWrite {
            key: key.to_vec(), value: value.to_vec(),
        }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn replicate_delete(&mut self, key: &[u8]) -> io::Result<()> {
        let resp = self.send_request(Request::ReplicateDelete { key: key.to_vec() }).await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn heartbeat(&mut self, node_id: &str, ring_version: u64) -> io::Result<()> {
        let resp = self.send_request(Request::Heartbeat {
            node_id: node_id.to_string(), ring_version,
        }).await?;
        match resp {
            Response::HeartbeatAck => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn join_cluster(&mut self, node_addr: &str) -> io::Result<(Vec<String>, u64)> {
        let resp = self.send_request(Request::JoinCluster {
            node_addr: node_addr.to_string(),
        }).await?;
        match resp {
            Response::ClusterState { nodes, ring_version } => Ok((nodes, ring_version)),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn get_cluster_state(&mut self) -> io::Result<(Vec<String>, u64)> {
        let resp = self.send_request(Request::GetClusterState).await?;
        match resp {
            Response::ClusterState { nodes, ring_version } => Ok((nodes, ring_version)),
            Response::Error(e) => Err(io::Error::other(e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }

    pub async fn pipeline(&mut self, requests: Vec<Request>) -> io::Result<Vec<Response>> {
        let mut ids = Vec::with_capacity(requests.len());

        for req in requests {
            let id = self.next_request_id();
            ids.push(id);
            let frame = Frame::request(id, req);
            let data = frame.encode()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.stream.write_u32(data.len() as u32).await?;
            self.stream.write_all(&data).await?;
        }
        self.stream.flush().await?;

        let mut responses = Vec::with_capacity(ids.len());
        for _ in 0..ids.len() {
            let resp = self.read_response().await?;
            responses.push(resp);
        }
        Ok(responses)
    }

    async fn send_request(&mut self, request: Request) -> io::Result<Response> {
        let id = self.next_request_id();
        let frame = Frame::request(id, request);
        let data = frame.encode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.stream.write_u32(data.len() as u32).await?;
        self.stream.write_all(&data).await?;
        self.stream.flush().await?;

        self.read_response().await
    }

    async fn read_response(&mut self) -> io::Result<Response> {
        let len = self.stream.read_u32().await? as usize;
        if len > MAX_FRAME_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Response too large"));
        }

        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf).await?;

        let frame = Frame::decode(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        match frame.payload {
            FramePayload::Response(resp) => Ok(resp),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Expected response frame")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}
