// Copyright 2015-2020 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Error and Result types for the Aerospike client.
//!
//! # Examples
//!
//! Handling an error returned by the client.
//!
//! ```rust
//! use aerospike::{
//!     errors::CommandError, Bins, Client, ClientPolicy, Key, ReadPolicy, ResultCode,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = Client::new(&ClientPolicy::default(), &"localhost:3000")
//!         .await
//!         .expect("Failed to connect to cluster");
//!
//!     let key = Key::new("test", "test", "someKey");
//!     match client.get(&ReadPolicy::default(), &key, Bins::None).await {
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

#![allow(missing_docs)]

use crate::result_code::ResultCode;
pub use crate::{
    cluster::ClusterError,
    commands::{CommandError, ParseParticleError},
    msgpack::MsgpackError,
    net::ParseHostError,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error decoding Base64 encoded value")]
    Base64(#[from] base64::DecodeError),
    #[error("Error interpreting a sequence of u8 as a UTF-8 encoded string")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("Error during an I/O operation")]
    Io(#[from] std::io::Error),
    #[error("Error returned from the `recv` function on an MPSC `Receiver`")]
    MpscRecv(#[from] tokio::sync::mpsc::error::TryRecvError),
    #[error("Error parsing an IP or socket address")]
    ParseAddr(#[from] std::net::AddrParseError),
    #[error("Error parsing an integer")]
    ParseInt(#[from] std::num::ParseIntError),
    /// The client received a server response that it was not able to process.
    #[error("Bad server response: {0}")]
    BadResponse(String),
    /// The client was not able to communicate with the cluster due to some issue with the
    /// network connection.
    #[error("Unable to communicate with server cluster: {0}")]
    Connection(String),
    /// One or more of the arguments passed to the client are invalid.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    /// Cluster node is invalid.
    #[error("Invalid cluster node: {0}")]
    InvalidNode(String),
    /// Exceeded max. number of connections per node.
    #[error("Too many connections")]
    NoMoreConnections,
    /// Server responded with a response code indicating an error condition.
    #[error("Server error: {}", .0.into_string())]
    ServerError(ResultCode),
    /// Error returned when a tasked timed out before it could be completed.
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("No nodes available")]
    NoNodes,
    #[error("Failed to truncate namespace or set")]
    Truncate(#[source] Box<Self>),
    #[error("Error creating index")]
    CreateIndex(#[source] Box<Self>),
    #[error("Network error")]
    Net(#[from] crate::net::NetError),
    #[error("Command error")]
    Command(#[from] crate::commands::CommandError),
    #[error("Cluster error")]
    Cluster(#[from] crate::cluster::ClusterError),
    #[error("MessagePack error")]
    Msgpack(#[from] crate::msgpack::MsgpackError),
    #[error("Failed parsing host value")]
    ParseHost(#[from] crate::net::ParseHostError),
}
