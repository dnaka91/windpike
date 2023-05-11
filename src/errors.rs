//! Error and Result types for the Aerospike client.
//!
//! # Examples
//!
//! Handling an error returned by the client.
//!
//! ```rust
//! use windpike::{
//!     errors::CommandError,
//!     policy::{BasePolicy, ClientPolicy},
//!     Bins, Client, Key, ResultCode,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
//!         .await
//!         .expect("Failed to connect to cluster");
//!
//!     let key = Key::new("test", "test", "someKey");
//!     match client.get(&BasePolicy::default(), &key, Bins::None).await {
//!         Ok(record) => match record.time_to_live() {
//!             None => println!("record never expires"),
//!             Some(duration) => println!("ttl: {} secs", duration.as_secs()),
//!         },
//!         Err(CommandError::ServerError(ResultCode::KeyNotFoundError)) => {
//!             println!("No such record: {key:?}");
//!         }
//!         Err(err) => {
//!             println!("Error fetching record: {err:#?}");
//!         }
//!     }
//! }
//! ```

use crate::result_code::ResultCode;
pub use crate::{
    cluster::ClusterError,
    commands::{buffer::BufferError, CommandError, ParseParticleError},
    msgpack::MsgpackError,
    net::{NetError, ParseHostError},
    value::ParticleError,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error decoding Base64 encoded value")]
    Base64(#[from] base64::DecodeError),
    #[error("error interpreting a sequence of u8 as a UTF-8 encoded string")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("error during an I/O operation")]
    Io(#[from] std::io::Error),
    #[error("error returned from the `recv` function on an MPSC `Receiver`")]
    MpscRecv(#[from] tokio::sync::mpsc::error::TryRecvError),
    #[error("error parsing an IP or socket address")]
    ParseAddr(#[from] std::net::AddrParseError),
    #[error("error parsing an integer")]
    ParseInt(#[from] std::num::ParseIntError),
    /// The client received a server response that it was not able to process.
    #[error("bad server response: {0}")]
    BadResponse(String),
    /// The client was not able to communicate with the cluster due to some issue with the
    /// network connection.
    #[error("unable to communicate with server cluster: {0}")]
    Connection(String),
    /// One or more of the arguments passed to the client are invalid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Cluster node is invalid.
    #[error("invalid cluster node: {0}")]
    InvalidNode(String),
    /// Exceeded max. number of connections per node.
    #[error("too many connections")]
    NoMoreConnections,
    /// Server responded with a response code indicating an error condition.
    #[error("server error: {}", .0.into_string())]
    ServerError(ResultCode),
    /// Error returned when a tasked timed out before it could be completed.
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("no nodes available")]
    NoNodes,
    #[error("failed to truncate namespace or set")]
    Truncate(#[source] Box<Self>),
    #[error("error creating index")]
    CreateIndex(#[source] Box<Self>),
    #[error("network error")]
    Net(#[from] crate::net::NetError),
    #[error("command error")]
    Command(#[from] crate::commands::CommandError),
    #[error("cluster error")]
    Cluster(#[from] crate::cluster::ClusterError),
    #[error("MessagePack error")]
    Msgpack(#[from] crate::msgpack::MsgpackError),
    #[error("failed parsing host value")]
    ParseHost(#[from] crate::net::ParseHostError),
}
