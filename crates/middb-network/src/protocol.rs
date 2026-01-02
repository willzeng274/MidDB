use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Error(String),
    Pong,
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
    fn test_request_encode_decode() {
        let req = Request::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };
        
        let encoded = req.encode().unwrap();
        let decoded = Request::decode(&encoded).unwrap();
        
        match decoded {
            Request::Put { key, value } => {
                assert_eq!(key, b"test_key");
                assert_eq!(value, b"test_value");
            }
            _ => panic!("Wrong variant"),
        }
    }
    
    #[test]
    fn test_response_encode_decode() {
        let resp = Response::Value(Some(b"data".to_vec()));
        let encoded = resp.encode().unwrap();
        let decoded = Response::decode(&encoded).unwrap();
        
        match decoded {
            Response::Value(Some(data)) => assert_eq!(data, b"data"),
            _ => panic!("Wrong variant"),
        }
    }
}
