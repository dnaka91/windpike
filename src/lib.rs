#![warn(
    rust_2018_idioms,
    clippy::all,
    clippy::pedantic,
    clippy::clone_on_ref_ptr
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
//! use aerospike::{as_bin, operations, Bins, Client, ClientPolicy, Key, ReadPolicy, WritePolicy};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = Client::new(&ClientPolicy::default(), &"localhost:3000")
//!         .await
//!         .expect("Failed to connect to cluster");
//!     let client = Arc::new(client);
//!
//!     let mut tasks = vec![];
//!     let now = Instant::now();
//!     for i in 0..2 {
//!         let client = client.clone();
//!         let t = tokio::spawn(async move {
//!             let rpolicy = ReadPolicy::default();
//!             let wpolicy = WritePolicy::default();
//!             let key = Key::new("test", "test", i);
//!             let bins = [as_bin!("int", 123), as_bin!("str", "Hello, World!")];
//!
//!             client.put(&wpolicy, &key, &bins).await.unwrap();
//!             let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
//!             println!("Record: {rec}");
//!
//!             client.touch(&wpolicy, &key).await.unwrap();
//!             let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
//!             println!("Record: {rec}");
//!
//!             let rec = client.get(&rpolicy, &key, Bins::None).await.unwrap();
//!             println!("Record Header: {rec}");
//!
//!             let exists = client.exists(&wpolicy, &key).await.unwrap();
//!             println!("exists: {exists}");
//!
//!             let bin = as_bin!("int", 999);
//!             let ops = &vec![operations::put(&bin), operations::get()];
//!             let op_rec = client.operate(&wpolicy, &key, ops).await.unwrap();
//!             println!("operate: {op_rec}");
//!
//!             let existed = client.delete(&wpolicy, &key).await.unwrap();
//!             println!("existed (sould be true): {existed}");
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

pub use batch::BatchRead;
pub use bin::{Bin, Bins};
pub use client::Client;
pub use cluster::Node;
pub use key::{Key, UserKey};
pub use net::{Host, ToHosts};
pub use operations::{MapPolicy, MapReturnType, MapWriteMode};
pub use policy::{
    BatchPolicy, ClientPolicy, CommitLevel, Concurrency, ConsistencyLevel, Expiration,
    GenerationPolicy, Policy, Priority, ReadPolicy, RecordExistsAction, ScanPolicy, WritePolicy,
};
pub use query::{CollectionIndexType, IndexType, Recordset, Statement};
pub use record::Record;
pub use result_code::ResultCode;
pub use task::{IndexTask, Task};
pub use user::User;
pub use value::{FloatValue, Value};

mod batch;
mod bin;
mod client;
pub mod cluster;
mod commands;
pub mod errors;
mod key;
#[macro_use]
mod macros;
mod msgpack;
mod net;
pub mod operations;
pub mod policy;
pub mod query;
mod record;
mod result_code;
pub mod task;
mod user;
mod value;
