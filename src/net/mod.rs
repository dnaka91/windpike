pub use self::{
    connection::Connection,
    host::{Host, ToHosts},
    pool::{Pool, PooledConnection},
};

mod connection;
mod host;
mod parser;
mod pool;

type Result<T, E = NetError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("no more connections available in the pool")]
    NoMoreConnections,
    #[error("could not open network connection")]
    FailedOpening,
    #[error("I/O related error")]
    Io(#[from] std::io::Error),
    #[error("buffer error")]
    Buffer(#[from] crate::commands::buffer::BufferError),
    #[error("authentication error")]
    Authenticate(#[source] Box<crate::commands::CommandError>),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseHostError {
    #[error("invalid address string")]
    InvalidArgument,
    #[error("invalid port number")]
    PortNumber(#[source] std::num::ParseIntError),
}
