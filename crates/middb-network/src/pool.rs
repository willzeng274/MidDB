use crate::client::Client;
use std::io;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

pub struct ConnectionPool {
    addr: String,
    connections: Mutex<Vec<Client>>,
    semaphore: Arc<Semaphore>,
    max_size: usize,
}

impl ConnectionPool {
    pub fn new(addr: String, max_size: usize) -> Self {
        ConnectionPool {
            addr,
            connections: Mutex::new(Vec::with_capacity(max_size)),
            semaphore: Arc::new(Semaphore::new(max_size)),
            max_size,
        }
    }

    pub async fn get(&self) -> io::Result<PooledConnection<'_>> {
        let permit = self.semaphore.clone().acquire_owned().await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Pool closed"))?;

        let client = {
            let mut conns = self.connections.lock().await;
            conns.pop()
        };

        let client = match client {
            Some(c) => c,
            None => Client::connect(&self.addr).await?,
        };

        Ok(PooledConnection {
            client: Some(client),
            pool: self,
            _permit: permit,
        })
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    async fn return_connection(&self, client: Client) {
        let mut conns = self.connections.lock().await;
        if conns.len() < self.max_size {
            conns.push(client);
        }
    }
}

pub struct PooledConnection<'a> {
    client: Option<Client>,
    pool: &'a ConnectionPool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<'a> PooledConnection<'a> {
    pub fn client(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }
}

impl<'a> Drop for PooledConnection<'a> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if let Ok(mut conns) = self.pool.connections.try_lock() {
                if conns.len() < self.pool.max_size {
                    conns.push(client);
                }
            }
        }
    }
}

impl ConnectionPool {
    pub async fn release(&self, mut conn: PooledConnection<'_>) {
        if let Some(client) = conn.client.take() {
            self.return_connection(client).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_creation() {
        let pool = ConnectionPool::new("127.0.0.1:9999".to_string(), 10);
        assert_eq!(pool.max_size(), 10);
    }
}
