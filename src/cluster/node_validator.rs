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

use std::{net::ToSocketAddrs, str, vec::Vec};

use tracing::debug;

use crate::{
    cluster::Cluster,
    commands::Message,
    errors::{ErrorKind, Result, ResultExt},
    net::{Connection, Host},
    policy::ClientPolicy,
};

// Validates a Database server node
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone)]
pub struct NodeValidator {
    pub name: String,
    pub aliases: Vec<Host>,
    pub address: String,
    pub client_policy: ClientPolicy,
    pub use_new_info: bool,
    pub supports_float: bool,
    pub supports_batch_index: bool,
    pub supports_replicas_all: bool,
    pub supports_geo: bool,
}

// Generates a node validator
impl NodeValidator {
    pub fn new(cluster: &Cluster) -> Self {
        NodeValidator {
            name: String::new(),
            aliases: vec![],
            address: String::new(),
            client_policy: cluster.client_policy().clone(),
            use_new_info: true,
            supports_float: false,
            supports_batch_index: false,
            supports_replicas_all: false,
            supports_geo: false,
        }
    }

    pub async fn validate_node(&mut self, cluster: &Cluster, host: &Host) -> Result<()> {
        self.resolve_aliases(host)
            .chain_err(|| "Failed to resolve host aliases")?;

        let mut last_err = None;
        for alias in &self.aliases() {
            match self.validate_alias(cluster, alias).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    debug!(%alias, ?err, "Alias failed");
                    last_err = Some(err);
                }
            }
        }
        last_err.map_or_else(|| unreachable!(), Err)
    }

    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.clone()
    }

    fn resolve_aliases(&mut self, host: &Host) -> Result<()> {
        self.aliases = (host.name.as_ref(), host.port)
            .to_socket_addrs()?
            .map(|addr| Host::new(&addr.ip().to_string(), addr.port()))
            .collect();
        debug!(%host, aliases = ?self.aliases, "Resolved aliases for host");
        if self.aliases.is_empty() {
            Err(ErrorKind::Connection(format!("Failed to find addresses for {host}")).into())
        } else {
            Ok(())
        }
    }

    async fn validate_alias(&mut self, cluster: &Cluster, alias: &Host) -> Result<()> {
        let mut conn = Connection::new(&alias.address(), &self.client_policy).await?;
        let info_map = Message::info(&mut conn, &["node", "cluster-name", "features"]).await?;

        match info_map.get("node") {
            None => bail!(ErrorKind::InvalidNode(String::from("Missing node name"))),
            Some(node_name) => self.name = node_name.clone(),
        }

        if let Some(ref cluster_name) = *cluster.cluster_name() {
            match info_map.get("cluster-name") {
                None => bail!(ErrorKind::InvalidNode(String::from("Missing cluster name"))),
                Some(info_name) if info_name == cluster_name => {}
                Some(info_name) => bail!(ErrorKind::InvalidNode(format!(
                    "Cluster name mismatch: expected={cluster_name}, got={info_name}",
                ))),
            }
        }

        self.address = alias.address();

        if let Some(features) = info_map.get("features") {
            self.set_features(features);
        }

        Ok(())
    }

    fn set_features(&mut self, features: &str) {
        let features = features.split(';');
        for feature in features {
            match feature {
                "float" => self.supports_float = true,
                "batch-index" => self.supports_batch_index = true,
                "replicas-all" => self.supports_replicas_all = true,
                "geo" => self.supports_geo = true,
                _ => (),
            }
        }
    }
}
