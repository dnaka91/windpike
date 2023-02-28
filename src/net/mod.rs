// Copyright 2015-2018 Aerospike, Inc.
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
