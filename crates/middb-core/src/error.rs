use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Serialization(String),
    KeyNotFound,
    TransactionConflict,
    StorageFull,
    Corruption(String),
    InvalidConfig(String),
    InvalidArgument(String),
    Internal(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::TransactionConflict => write!(f, "Transaction conflict"),
            Error::StorageFull => write!(f, "Storage full"),
            Error::Corruption(msg) => write!(f, "Data corruption: {}", msg),
            Error::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            Error::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}
