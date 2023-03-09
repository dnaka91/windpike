// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    str,
    sync::Arc,
    vec::Vec,
};

use base64::engine::{general_purpose, Engine};
use tokio::sync::RwLock;

use super::{node, ClusterError, Node, Result};
use crate::{commands::Message, net::Connection};

const REPLICAS_NAME: &str = "replicas-master";

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct PartitionTokenizer {
    buffer: Vec<u8>,
    _length: usize,
    _offset: usize,
}

impl PartitionTokenizer {
    pub async fn new(conn: &mut Connection) -> Result<Self> {
        let info_map = Message::info(conn, &[REPLICAS_NAME]).await?;
        if let Some(buf) = info_map.get(REPLICAS_NAME) {
            return Ok(Self {
                _length: info_map.len(),
                buffer: buf.as_bytes().to_owned(),
                _offset: 0,
            });
        }
        Err(ClusterError::MissingReplicas)
    }

    pub async fn update_partition(
        &self,
        nmap: Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>>,
        node: Arc<Node>,
    ) -> Result<HashMap<String, Vec<Arc<Node>>>> {
        let mut amap = nmap.read().await.clone();

        // <ns>:<base64-encoded partition map>;<ns>:<base64-encoded partition map>; ...
        let part_str = str::from_utf8(&self.buffer)?;
        let mut parts = part_str.trim_end().split(|c| c == ':' || c == ';');
        loop {
            match (parts.next(), parts.next()) {
                (Some(ns), Some(part)) => {
                    let restore_buffer = general_purpose::STANDARD.decode(part)?;
                    match amap.entry(ns.to_string()) {
                        Vacant(entry) => {
                            entry.insert(vec![Arc::clone(&node); node::PARTITIONS]);
                        }
                        Occupied(mut entry) => {
                            for (idx, item) in entry.get_mut().iter_mut().enumerate() {
                                if restore_buffer[idx >> 3] & (0x80 >> (idx & 7) as u8) != 0 {
                                    *item = Arc::clone(&node);
                                }
                            }
                        }
                    }
                }
                (None, None) => break,
                _ => return Err(ClusterError::InvalidPartitionInfo),
            }
        }

        Ok(amap)
    }
}
