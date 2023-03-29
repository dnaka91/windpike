use std::{net::ToSocketAddrs, str, vec::Vec};

use tracing::debug;

use super::{Cluster, NodeError, Result};
use crate::{
    commands::Message,
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
        Self {
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

    pub async fn validate_node(&mut self, cluster: &Cluster, host: &Host) -> Result<(), NodeError> {
        self.resolve_aliases(host)?;

        let mut last_err = None;
        for alias in &self.aliases() {
            match self.validate_alias(cluster, alias).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    debug!(%alias, ?err, "Alias validation failed");
                    last_err = Some(err);
                }
            }
        }

        match last_err {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    #[must_use]
    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.clone()
    }

    fn resolve_aliases(&mut self, host: &Host) -> Result<(), NodeError> {
        self.aliases = (host.name.as_ref(), host.port)
            .to_socket_addrs()?
            .map(|addr| Host::new(&addr.ip().to_string(), addr.port()))
            .collect();
        debug!(%host, aliases = ?self.aliases, "Resolved aliases for host");
        if self.aliases.is_empty() {
            Err(NodeError::NoAddress { host: host.clone() })
        } else {
            Ok(())
        }
    }

    async fn validate_alias(&mut self, cluster: &Cluster, alias: &Host) -> Result<(), NodeError> {
        let mut conn = Connection::new(&alias.address(), &self.client_policy).await?;
        let info_map = Message::info(&mut conn, &["node", "cluster-name", "features"]).await?;

        match info_map.get("node") {
            None => return Err(NodeError::MissingNodeName),
            Some(node_name) => self.name = node_name.clone(),
        }

        if let Some(cluster_name) = cluster.cluster_name() {
            match info_map.get("cluster-name") {
                None => return Err(NodeError::MissingClusterName),
                Some(info_name) if info_name == cluster_name => {}
                Some(info_name) => {
                    return Err(NodeError::NameMismatch {
                        expected: cluster_name.clone(),
                        got: info_name.clone(),
                    })
                }
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
