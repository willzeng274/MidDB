use crate::protocol::{Request, Response};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Client { stream })
    }
    
    pub async fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let request = Request::Get { key: key.to_vec() };
        let response = self.send_request(request).await?;
        
        match response {
            Response::Value(value) => Ok(value),
            Response::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }
    
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let request = Request::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let response = self.send_request(request).await?;
        
        match response {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }
    
    pub async fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let request = Request::Delete { key: key.to_vec() };
        let response = self.send_request(request).await?;
        
        match response {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected response")),
        }
    }
    
    pub async fn ping(&mut self) -> io::Result<()> {
        let request = Request::Ping;
        let response = self.send_request(request).await?;
        
        match response {
            Response::Pong => Ok(()),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Expected pong")),
        }
    }
    
    async fn send_request(&mut self, request: Request) -> io::Result<Response> {
        let request_data = request.encode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        self.stream.write_u32(request_data.len() as u32).await?;
        self.stream.write_all(&request_data).await?;
        
        let len = self.stream.read_u32().await? as usize;
        
        if len > 10 * 1024 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Response too large"));
        }
        
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf).await?;
        
        Response::decode(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
