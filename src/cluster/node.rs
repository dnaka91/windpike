use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::RwLock;
use tracing::error;

use super::{node_validator::NodeValidator, ClusterError, NodeError, NodeRefreshError, Result};
use crate::{
    commands::Message,
    net::{ConnectionPool, Host, NetError, PooledConnection},
    policy::ClientPolicy,
};

pub const PARTITIONS: usize = 4096;

/// The node instance holding connections and node settings.
/// Exposed for usage in the sync client interface.
#[derive(Debug)]
pub struct Node {
    client_policy: ClientPolicy,
    name: String,
    host: Host,
    aliases: RwLock<Vec<Host>>,
    address: String,

    connection_pool: ConnectionPool,
    failures: AtomicUsize,

    partition_generation: AtomicIsize,
    refresh_count: AtomicUsize,
    reference_count: AtomicUsize,
    responded: AtomicBool,
    active: AtomicBool,

    supports_float: bool,
    supports_geo: bool,
}

impl Node {
    #![allow(missing_docs)]
    #[must_use]
    pub fn new(client_policy: ClientPolicy, nv: &NodeValidator) -> Self {
        Self {
            connection_pool: ConnectionPool::new(&nv.aliases[0], &client_policy),
            client_policy,
            name: nv.name.clone(),
            aliases: RwLock::new(nv.aliases.clone()),
            address: nv.address.clone(),

            host: nv.aliases[0].clone(),
            failures: AtomicUsize::new(0),
            partition_generation: AtomicIsize::new(-1),
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),

            supports_float: nv.supports_float,
            supports_geo: nv.supports_geo,
        }
    }

    // Returns the Node address
    pub fn address(&self) -> &str {
        &self.address
    }

    // Returns the Node name
    pub fn name(&self) -> &str {
        &self.name
    }

    // Returns the active client policy
    pub const fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    // Returns true if the Node supports floats
    pub fn supports_float(&self) -> bool {
        self.supports_float
    }

    // Returns true if the Node supports geo
    pub fn supports_geo(&self) -> bool {
        self.supports_geo
    }

    // Returns the reference count
    pub fn reference_count(&self) -> usize {
        self.reference_count.load(Ordering::Relaxed)
    }

    // Refresh the node
    pub async fn refresh(
        &self,
        current_aliases: &HashMap<Host, Arc<Self>>,
    ) -> Result<Vec<Host>, NodeRefreshError> {
        self.reference_count.store(0, Ordering::Relaxed);
        self.responded.store(false, Ordering::Relaxed);
        self.refresh_count.fetch_add(1, Ordering::Relaxed);
        let commands = vec![
            "node",
            "cluster-name",
            "partition-generation",
            self.services_name(),
        ];
        let info_map = self
            .info(&commands)
            .await
            .map_err(NodeRefreshError::InfoCommandFailed)?;
        self.validate_node(&info_map)
            .map_err(NodeRefreshError::ValidationFailed)?;
        self.responded.store(true, Ordering::Relaxed);
        let friends = self
            .add_friends(current_aliases, &info_map)
            .map_err(NodeRefreshError::FailedAddingFriends)?;
        self.update_partitions(&info_map)
            .map_err(NodeRefreshError::FailedUpdatingPartitions)?;
        self.reset_failures();
        Ok(friends)
    }

    // Returns the services that the client should use for the cluster tend
    const fn services_name(&self) -> &'static str {
        if self.client_policy.use_services_alternate {
            "services-alternate"
        } else {
            "services"
        }
    }

    fn validate_node(&self, info_map: &HashMap<String, String>) -> Result<(), NodeError> {
        self.verify_node_name(info_map)?;
        self.verify_cluster_name(info_map)?;
        Ok(())
    }

    fn verify_node_name(&self, info_map: &HashMap<String, String>) -> Result<(), NodeError> {
        match info_map.get("node") {
            None => Err(NodeError::MissingNodeName),
            Some(info_name) if info_name == &self.name => Ok(()),
            Some(info_name) => {
                self.inactivate();
                Err(NodeError::NameMismatch {
                    expected: self.name.clone(),
                    got: info_name.clone(),
                })
            }
        }
    }

    fn verify_cluster_name(&self, info_map: &HashMap<String, String>) -> Result<(), NodeError> {
        self.client_policy.cluster_name.as_ref().map_or_else(
            || Ok(()),
            |expected| match info_map.get("cluster-name") {
                None => Err(NodeError::MissingClusterName),
                Some(info_name) if info_name == expected => Ok(()),
                Some(info_name) => {
                    self.inactivate();
                    Err(NodeError::NameMismatch {
                        expected: expected.clone(),
                        got: info_name.clone(),
                    })
                }
            },
        )
    }

    fn add_friends(
        &self,
        current_aliases: &HashMap<Host, Arc<Self>>,
        info_map: &HashMap<String, String>,
    ) -> Result<Vec<Host>> {
        let mut friends: Vec<Host> = vec![];

        let friend_string = match info_map.get(self.services_name()) {
            None => return Err(ClusterError::MissingServicesList),
            Some(friend_string) if friend_string.is_empty() => return Ok(friends),
            Some(friend_string) => friend_string,
        };

        let friend_names = friend_string.split(';');
        for friend in friend_names {
            let mut friend_info = friend.split(':');
            if friend_info.clone().count() != 2 {
                error!(
                    "Node info from asinfo:services is malformed. Expected HOST:PORT, but got \
                     '{friend}'",
                );
                continue;
            }

            let host = friend_info.next().unwrap();
            let port = u16::from_str(friend_info.next().unwrap())?;
            let alias = match &self.client_policy.ip_map {
                Some(ip_map) if ip_map.contains_key(host) => {
                    Host::new(ip_map.get(host).unwrap(), port)
                }
                _ => Host::new(host, port),
            };

            if current_aliases.contains_key(&alias) {
                self.reference_count.fetch_add(1, Ordering::Relaxed);
            } else if !friends.contains(&alias) {
                friends.push(alias);
            }
        }

        Ok(friends)
    }

    fn update_partitions(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get("partition-generation") {
            None => return Err(ClusterError::MissingPartitionGeneration),
            Some(gen_string) => {
                let gen = gen_string.parse::<isize>()?;
                self.partition_generation.store(gen, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    // Get a connection to the node from the connection pool
    pub async fn get_connection(&self) -> Result<PooledConnection, NetError> {
        self.connection_pool.get().await
    }

    // Amount of failures
    pub fn failures(&self) -> usize {
        self.failures.load(Ordering::Relaxed)
    }

    fn reset_failures(&self) {
        self.failures.store(0, Ordering::Relaxed);
    }

    // Adds a failure to the failure count
    pub fn increase_failures(&self) -> usize {
        self.failures.fetch_add(1, Ordering::Relaxed)
    }

    fn inactivate(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    // Returns true if the node is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    // Get a list of aliases to the node
    pub async fn aliases(&self) -> Vec<Host> {
        self.aliases.read().await.to_vec()
    }

    // Add an alias to the node
    pub async fn add_alias(&self, alias: Host) {
        let mut aliases = self.aliases.write().await;
        aliases.push(alias);
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    // Set the node inactive and close all connections in the pool
    pub async fn close(&mut self) {
        self.inactivate();
        self.connection_pool.close().await;
    }

    // Send info commands to this node
    pub async fn info(&self, commands: &[&str]) -> Result<HashMap<String, String>> {
        let mut conn = self.get_connection().await?;
        match Message::info(&mut conn, commands).await {
            Ok(info) => Ok(info),
            Err(e) => {
                conn.invalidate().await;
                Err(e.into())
            }
        }
    }

    // Get the partition generation
    pub fn partition_generation(&self) -> isize {
        self.partition_generation.load(Ordering::Relaxed)
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Node {}
