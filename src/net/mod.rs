pub use self::{
    connection::Connection,
    connection_pool::{ConnectionPool, PooledConnection},
    host::{Host, ToHosts},
};

mod connection;
mod connection_pool;
pub mod host;
mod parser;

type Result<T, E = NetError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("No more connections available in the pool")]
    NoMoreConnections,
    #[error("Could not open network connection")]
    FailedOpening,
    #[error("I/O related error")]
    Io(#[from] std::io::Error),
    #[error("Buffer error")]
    Buffer(#[from] crate::commands::buffer::BufferError),
    #[error("Authentication error")]
    Authenticate(#[source] Box<crate::commands::CommandError>),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseHostError {
    #[error("Invalid address string")]
    InvalidArgument,
    #[error("Invalid port number")]
    PortNumber(#[source] std::num::ParseIntError),
}
