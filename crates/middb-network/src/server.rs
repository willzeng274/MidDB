use crate::protocol::{Request, Response};
use middb_core::Database;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
    
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("Server listening on {}", self.addr);
        
        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New connection from {}", addr);
            
            let db = Arc::clone(&self.db);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, db).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream, db: Arc<Database>) -> io::Result<()> {
    loop {
        let len = match socket.read_u32().await {
            Ok(len) => len as usize,
            Err(_) => return Ok(()),
        };
        
        if len == 0 || len > 10 * 1024 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid length"));
        }
        
        let mut buf = vec![0u8; len];
        socket.read_exact(&mut buf).await?;
        
        let request = Request::decode(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        let response = handle_request(&db, request);
        
        let response_data = response.encode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        socket.write_u32(response_data.len() as u32).await?;
        socket.write_all(&response_data).await?;
    }
}

fn handle_request(db: &Database, request: Request) -> Response {
    match request {
        Request::Get { key } => {
            match db.get(&key) {
                Ok(value) => Response::Value(value),
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::Put { key, value } => {
            match db.put(key, value) {
                Ok(()) => Response::Ok,
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::Delete { key } => {
            match db.delete(key) {
                Ok(()) => Response::Ok,
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::Ping => Response::Pong,
    }
}
