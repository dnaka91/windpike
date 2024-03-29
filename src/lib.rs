#![forbid(unsafe_code)]
#![warn(
    rust_2018_idioms,
    clippy::all,
    clippy::pedantic,
    clippy::clone_on_ref_ptr,
    clippy::format_push_string,
    clippy::get_unwrap,
    clippy::mem_forget,
    clippy::mutex_atomic,
    clippy::rc_buffer,
    clippy::rc_mutex,
    clippy::self_named_module_files,
    clippy::str_to_string,
    clippy::wildcard_imports,
    clippy::string_add,
    clippy::string_to_string,
    clippy::try_err,
    clippy::unreachable
)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::module_name_repetitions,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::missing_errors_doc,
    clippy::manual_let_else,
    missing_docs
)]

//! A pure-rust client for the Aerospike `NoSQL` database.
//!
//! Aerospike is an enterprise-class, `NoSQL` database solution for real-time operational
//! applications, delivering predictable performance at scale, superior uptime, and high
//! availability at the lowest TCO compared to first-generation `NoSQL` and relational databases.
//! For more information please refer to <https://www.aerospike.com/>.
//!
//! # Installation
//!
//! Add this to your `Cargo.toml`:
//!
//! ```text
//! [dependencies]
//! aerospike = "1.0.0"
//! ```
//!
//! # Examples
//!
//! The following is a very simple example of CRUD operations in an Aerospike database.
//!
//! ```rust
//! use std::{sync::Arc, time::Instant};
//!
//! use windpike::{
//!     operations::scalar,
//!     policies::{BasePolicy, ClientPolicy, WritePolicy},
//!     Bin, Bins, Client, Key,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
//!         .await
//!         .expect("Failed to connect to cluster");
//!     let client = Arc::new(client);
//!
//!     let mut tasks = vec![];
//!     let now = Instant::now();
//!     for i in 0..2 {
//!         let client = client.clone();
//!         let t = tokio::spawn(async move {
//!             let rpolicy = BasePolicy::default();
//!             let wpolicy = WritePolicy::default();
//!             let key = Key::new("test", "test", i);
//!             let bins = [Bin::new("int", 123), Bin::new("str", "Hello, World!")];
//!
//!             client.put(&wpolicy, &key, &bins).await.unwrap();
//!             let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
//!             println!("Record: {rec:?}");
//!
//!             client.touch(&wpolicy, &key).await.unwrap();
//!             let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
//!             println!("Record: {rec:?}");
//!
//!             let rec = client.get(&rpolicy, &key, Bins::None).await.unwrap();
//!             println!("Record Header: {rec:?}");
//!
//!             let exists = client.exists(&wpolicy, &key).await.unwrap();
//!             println!("exists: {exists}");
//!
//!             let bin = Bin::new("int", 999);
//!             let ops = &vec![scalar::put(&bin), scalar::get()];
//!             let op_rec = client.operate(&wpolicy, &key, ops).await.unwrap();
//!             println!("operate: {op_rec:?}");
//!
//!             let existed = client.delete(&wpolicy, &key).await.unwrap();
//!             println!("existed (should be true): {existed}");
//!
//!             let existed = client.delete(&wpolicy, &key).await.unwrap();
//!             println!("existed (should be false): {existed}");
//!         });
//!
//!         tasks.push(t);
//!     }
//!
//!     for t in tasks {
//!         t.await.unwrap();
//!     }
//!
//!     println!("total time: {:?}", now.elapsed());
//! }
//! ```

pub use ordered_float;

pub use crate::{
    batch::BatchRead,
    bin::{Bin, Bins},
    client::Client,
    key::{Key, UserKey},
    net::{Host, ToHosts},
    record::{Record, RecordSet},
    result_code::ResultCode,
    value::{FloatValue, MapKey, Value},
};

mod batch;
mod bin;
mod client;
mod cluster;
mod commands;
pub mod errors;
mod key;
#[macro_use]
mod macros;
pub mod index;
mod msgpack;
mod net;
pub mod operations;
pub mod policies;
mod record;
mod result_code;
mod value;
