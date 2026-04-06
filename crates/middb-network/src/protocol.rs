use serde::{Deserialize, Serialize};

pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024; // 64MB

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub request_id: u32,
    pub payload: FramePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FramePayload {
    Request(Request),
    Response(Response),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    BatchGet { keys: Vec<Vec<u8>> },
    BatchPut { pairs: Vec<(Vec<u8>, Vec<u8>)> },
    Query { sql: String },
    BeginTxn,
    CommitTxn { txn_id: u64 },
    AbortTxn { txn_id: u64 },
    TxnGet { txn_id: u64, key: Vec<u8> },
    TxnPut { txn_id: u64, key: Vec<u8>, value: Vec<u8> },
    TxnDelete { txn_id: u64, key: Vec<u8> },
    Ping,

    // Cluster-internal: replica write/delete (node-to-node)
    ReplicateWrite { key: Vec<u8>, value: Vec<u8> },
    ReplicateDelete { key: Vec<u8> },

    // Cluster membership
    Heartbeat { node_id: String, ring_version: u64 },
    JoinCluster { node_addr: String },
    GetClusterState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Values(Vec<Option<Vec<u8>>>),
    BatchOk { count: usize },
    TxnStarted { txn_id: u64 },
    QueryResult { columns: Vec<String>, rows: Vec<Vec<Option<String>>> },
    Error(String),
    Pong,

    // Cluster responses
    HeartbeatAck,
    ClusterState { nodes: Vec<String>, ring_version: u64 },
}

impl Frame {
    pub fn request(request_id: u32, req: Request) -> Self {
        Frame { request_id, payload: FramePayload::Request(req) }
    }

    pub fn response(request_id: u32, resp: Response) -> Self {
        Frame { request_id, payload: FramePayload::Response(resp) }
    }

    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn decode(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

impl Request {
    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn decode(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

impl Response {
    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn decode(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_roundtrip() {
        let frame = Frame::request(42, Request::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        });
        let encoded = frame.encode().unwrap();
        let decoded = Frame::decode(&encoded).unwrap();
        assert_eq!(decoded.request_id, 42);
        match decoded.payload {
            FramePayload::Request(Request::Put { key, value }) => {
                assert_eq!(key, b"test_key");
                assert_eq!(value, b"test_value");
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_batch_request() {
        let req = Request::BatchGet {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        };
        let encoded = req.encode().unwrap();
        let decoded = Request::decode(&encoded).unwrap();
        match decoded {
            Request::BatchGet { keys } => assert_eq!(keys.len(), 3),
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_query_result_response() {
        let resp = Response::QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Some("1".into()), Some("alice".into())],
                vec![Some("2".into()), None],
            ],
        };
        let encoded = resp.encode().unwrap();
        let decoded = Response::decode(&encoded).unwrap();
        match decoded {
            Response::QueryResult { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[1][1], None);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_txn_requests() {
        let req = Request::TxnPut { txn_id: 5, key: b"k".to_vec(), value: b"v".to_vec() };
        let encoded = req.encode().unwrap();
        let decoded = Request::decode(&encoded).unwrap();
        match decoded {
            Request::TxnPut { txn_id, key, value } => {
                assert_eq!(txn_id, 5);
                assert_eq!(key, b"k");
                assert_eq!(value, b"v");
            }
            _ => panic!("Wrong variant"),
        }
    }
}
