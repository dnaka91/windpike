use tracing::debug;

use super::{node::FeatureSupport, Cluster, NodeError, Result};
use crate::{
    commands::{
        self,
        info_cmds::{CLUSTER_NAME, FEATURES, NODE},
    },
    net::{Connection, Host},
    policies::ClientPolicy,
};

pub async fn validate(
    cluster: &Cluster,
    host: &Host,
) -> Result<(String, FeatureSupport, Vec<Host>), NodeError> {
    let aliases = resolve_aliases(host).await?;
    let mut last_err = None;

    for alias in &aliases {
        match validate_alias(cluster.client_policy(), cluster.name(), alias).await {
            Ok((name, features)) => return Ok((name, features, aliases)),
            Err(err) => {
                debug!(%alias, ?err, "alias validation failed");
                last_err = Some(err);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| NodeError::NoValidInstance { host: host.clone() }))
}

async fn resolve_aliases(host: &Host) -> Result<Vec<Host>, NodeError> {
    let aliases = host
        .to_socket_addrs()
        .await?
        .map(|addr| Host::new(addr.ip().to_string(), addr.port()))
        .collect::<Vec<_>>();
    debug!(%host, ?aliases, "resolved aliases for host");

    if aliases.is_empty() {
        Err(NodeError::NoAddress { host: host.clone() })
    } else {
        Ok(aliases)
    }
}

async fn validate_alias(
    policy: &ClientPolicy,
    cluster_name: Option<&str>,
    alias: &Host,
) -> Result<(String, FeatureSupport), NodeError> {
    let mut conn = Connection::new(&alias.address(), policy).await?;
    let info_map = commands::info_typed(&mut conn, &[NODE, CLUSTER_NAME, FEATURES]).await?;

    if let Some(cluster_name) = cluster_name {
        match info_map.cluster_name {
            None => return Err(NodeError::MissingClusterName),
            Some(info_name) if info_name == cluster_name => {}
            Some(info_name) => {
                return Err(NodeError::NameMismatch {
                    expected: cluster_name.to_owned(),
                    got: info_name,
                })
            }
        }
    }

    let node_name = match info_map.node {
        None => return Err(NodeError::MissingNodeName),
        Some(node_name) => node_name,
    };

    let features = info_map.features.unwrap_or_else(FeatureSupport::empty);

    Ok((node_name, features))
}
