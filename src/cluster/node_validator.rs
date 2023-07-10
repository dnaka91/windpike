use std::{net::ToSocketAddrs, vec::Vec};

use tracing::debug;

use super::{node::FeatureSupport, Cluster, NodeError, Result};
use crate::{
    commands::Message,
    net::{Connection, Host},
    policies::ClientPolicy,
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
    pub features: FeatureSupport,
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
            features: FeatureSupport::default(),
        }
    }

    pub async fn validate_node(&mut self, cluster: &Cluster, host: &Host) -> Result<(), NodeError> {
        self.resolve_aliases(host)?;

        let mut last_err = None;
        for alias in &self.aliases() {
            match self.validate_alias(cluster, alias).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    debug!(%alias, ?err, "alias validation failed");
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
        self.aliases = host
            .to_socket_addrs()?
            .map(|addr| Host::new(addr.ip().to_string(), addr.port()))
            .collect();
        debug!(%host, aliases = ?self.aliases, "resolved aliases for host");
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
            self.features = features.as_str().into();
        }

        Ok(())
    }
}
