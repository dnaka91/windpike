use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    sync::Arc,
    vec::Vec,
};

use tokio::sync::RwLock;

use super::{node, ClusterError, Node, Result};
use crate::{
    commands::{self, info_cmds::REPLICAS_MASTER},
    net::Connection,
};

pub async fn update(
    conn: &mut Connection,
    nmap: Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>>,
    node: Arc<Node>,
) -> Result<HashMap<String, Vec<Arc<Node>>>> {
    let replicas = commands::info_typed(conn, &[REPLICAS_MASTER])
        .await?
        .replicas_master
        .ok_or(ClusterError::MissingReplicas)?;

    let mut amap = nmap.read().await.clone();

    for (ns, buffer) in replicas {
        match amap.entry(ns) {
            Vacant(entry) => {
                entry.insert(vec![Arc::clone(&node); node::PARTITIONS]);
            }
            Occupied(mut entry) => {
                for (idx, item) in entry.get_mut().iter_mut().enumerate() {
                    if buffer[idx >> 3] & (0x80 >> (idx & 7) as u8) != 0 {
                        *item = Arc::clone(&node);
                    }
                }
            }
        }
    }

    Ok(amap)
}
