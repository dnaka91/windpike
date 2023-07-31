use std::{
    collections::{HashMap, HashSet},
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
    net::{Host, NetError, Pool, PooledConnection},
    policies::ClientPolicy,
};

pub const PARTITIONS: usize = 4096;

/// The node instance holding connections and node settings.
/// Exposed for usage in the sync client interface.
#[derive(Debug)]
pub struct Node {
    client_policy: ClientPolicy,
    name: String,
    aliases: RwLock<Vec<Host>>,

    connection_pool: Pool,
    failures: AtomicUsize,

    partition_generation: AtomicIsize,
    refresh_count: AtomicUsize,
    reference_count: AtomicUsize,
    responded: AtomicBool,
    active: AtomicBool,

    _features: FeatureSupport,
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Default)]
    pub struct FeatureSupport: u32 {
        const BATCH_ANY = 1 << 0;
        const BATCH_INDEX = 1 << 1;
        const BLOB_BITS = 1 << 2;
        const CDT_LIST = 1 << 3;
        const CDT_MAP = 1 << 4;
        const CLUSTER_STABLE = 1 << 5;
        const FLOAT = 1 << 6;
        const GEO = 1 << 7;
        const SINDEX_EXISTS = 1 << 8;
        const PEERS = 1 << 9;
        const PIPELINING = 1 << 10;
        const PQUERY = 1 << 11;
        const PSCANS = 1 << 12;
        const QUERY_SHOW = 1 << 13;
        const RELAXED_SC = 1 << 14;
        const REPLICAS = 1 << 15;
        const REPLICAS_ALL = 1 << 16;
        const REPLICAS_MASTER = 1 << 17;
        const REPLICAS_MAX = 1 << 18;
        const TRUNCATE_NAMESPACE = 1 << 19;
        const UDF = 1 << 20;
    }
}

impl From<&str> for FeatureSupport {
    fn from(value: &str) -> Self {
        let mut support = FeatureSupport::default();
        for v in value.split(';') {
            support |= match v {
                "batch-any" => Self::BATCH_ANY,
                "batch-index" => Self::BATCH_INDEX,
                "blob-bits" => Self::BLOB_BITS,
                "cdt-list" => Self::CDT_LIST,
                "cdt-map" => Self::CDT_MAP,
                "cluster-stable" => Self::CLUSTER_STABLE,
                "float" => Self::FLOAT,
                "geo" => Self::GEO,
                "sindex-exists" => Self::SINDEX_EXISTS,
                "peers" => Self::PEERS,
                "pipelining" => Self::PIPELINING,
                "pquery" => Self::PQUERY,
                "pscans" => Self::PSCANS,
                "query-show" => Self::QUERY_SHOW,
                "relaxed-sc" => Self::RELAXED_SC,
                "replicas" => Self::REPLICAS,
                "replicas-all" => Self::REPLICAS_ALL,
                "replicas-master" => Self::REPLICAS_MASTER,
                "replicas-max" => Self::REPLICAS_MAX,
                "truncate-namespace" => Self::TRUNCATE_NAMESPACE,
                "udf" => Self::UDF,
                _ => continue,
            };
        }

        support
    }
}

impl Node {
    pub async fn new(client_policy: ClientPolicy, nv: &NodeValidator) -> Result<Self, NetError> {
        Ok(Self {
            connection_pool: Pool::new(nv.aliases[0].clone(), client_policy.clone()).await?,
            client_policy,
            name: nv.name.clone(),
            aliases: RwLock::new(nv.aliases.clone()),
            failures: AtomicUsize::new(0),
            partition_generation: AtomicIsize::new(-1),
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),

            _features: nv.features,
        })
    }

    // Returns the Node name
    pub fn name(&self) -> &str {
        &self.name
    }

    // Returns the reference count
    pub fn reference_count(&self) -> usize {
        self.reference_count.load(Ordering::Relaxed)
    }

    // Refresh the node
    pub async fn refresh(
        &self,
        current_aliases: &HashMap<Host, Arc<Self>>,
    ) -> Result<HashSet<Host>, NodeRefreshError> {
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
    ) -> Result<HashSet<Host>> {
        Ok(info_map
            .get(self.services_name())
            .ok_or(ClusterError::MissingServicesList)?
            .split(';')
            .filter(|s| !s.is_empty())
            .filter_map(|friend| {
                let (host, port) = if let Some((host, port)) = friend
                    .split_once(':')
                    .and_then(|(host, port)| Some(host).zip(port.parse().ok()))
                {
                    (host, port)
                } else {
                    error!(
                        got = friend,
                        "node info from asinfo:services is malformed, expected HOST:PORT",
                    );
                    return None;
                };

                let host = self
                    .client_policy
                    .ip_map
                    .as_ref()
                    .and_then(|map| map.get(host).map(String::as_str))
                    .unwrap_or(host);

                let alias = Host::new(host, port);

                if current_aliases.contains_key(&alias) {
                    self.reference_count.fetch_add(1, Ordering::Relaxed);
                    None
                } else {
                    Some(alias)
                }
            })
            .collect())
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
    pub async fn get_connection(&self) -> Result<PooledConnection<'_>, NetError> {
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
    pub(crate) fn increase_failures(&self) -> usize {
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

    // Send info commands to this node
    pub async fn info(&self, commands: &[&str]) -> Result<HashMap<String, String>> {
        let mut conn = self.get_connection().await?;
        match Message::info(&mut conn, commands).await {
            Ok(info) => Ok(info),
            Err(e) => {
                conn.close().await;
                Err(e.into())
            }
        }
    }

    // Get the partition generation
    pub fn partition_generation(&self) -> isize {
        self.partition_generation.load(Ordering::Relaxed)
    }
}
