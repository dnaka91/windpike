use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::{RwLock, RwLockReadGuard};

use super::{ClusterError, NodeError, NodeRefreshError, Result};
use crate::{
    commands::{
        self,
        info_cmds::{CLUSTER_NAME, NODE, PARTITION_GENERATION, SERVICES, SERVICES_ALTERNATE},
        Info,
    },
    net::{Host, NetError, Pool, PooledConnection},
    policies::ClientPolicy,
};

pub const PARTITIONS: usize = 4096;

/// The node instance holding connections and node settings.
/// Exposed for usage in the sync client interface.
#[derive(Debug)]
pub struct Node {
    client_policy: Arc<ClientPolicy>,
    name: String,
    aliases: RwLock<Vec<Host>>,

    connection_pool: Pool,
    failures: AtomicUsize,

    partition_generation: AtomicIsize,
    reference_count: AtomicUsize,
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
    pub async fn new(
        client_policy: Arc<ClientPolicy>,
        name: String,
        features: FeatureSupport,
        aliases: Vec<Host>,
    ) -> Result<Self, NetError> {
        Ok(Self {
            connection_pool: Pool::new(aliases[0].clone(), Arc::clone(&client_policy)).await?,
            client_policy,
            name,
            aliases: RwLock::new(aliases),
            failures: AtomicUsize::new(0),
            partition_generation: AtomicIsize::new(-1),
            reference_count: AtomicUsize::new(0),
            active: AtomicBool::new(true),
            _features: features,
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

        let commands = vec![
            NODE,
            CLUSTER_NAME,
            PARTITION_GENERATION,
            if self.client_policy.use_services_alternate {
                SERVICES_ALTERNATE
            } else {
                SERVICES
            },
        ];

        let mut conn = self
            .get_connection()
            .await
            .map_err(|e| NodeRefreshError::InfoCommandFailed(e.into()))?;

        let mut info = match commands::info_typed(&mut conn, &commands).await {
            Ok(info) => info,
            Err(e) => {
                conn.close().await;
                return Err(NodeRefreshError::InfoCommandFailed(e.into()));
            }
        };

        self.validate_node(&mut info)
            .map_err(NodeRefreshError::ValidationFailed)?;
        let friends = self
            .add_friends(current_aliases, &mut info)
            .map_err(NodeRefreshError::FailedAddingFriends)?;
        self.update_partitions(&info)
            .map_err(NodeRefreshError::FailedUpdatingPartitions)?;
        self.reset_failures();

        Ok(friends)
    }

    fn validate_node(&self, info_map: &mut Info) -> Result<(), NodeError> {
        match info_map.node.take() {
            None => return Err(NodeError::MissingNodeName),
            Some(info_name) if info_name == self.name => {}
            Some(info_name) => {
                self.inactivate();
                return Err(NodeError::NameMismatch {
                    expected: self.name.clone(),
                    got: info_name,
                });
            }
        }

        if let Some(expected) = self.client_policy.cluster_name.as_deref() {
            match info_map.cluster_name.take() {
                None => return Err(NodeError::MissingClusterName),
                Some(info_name) if info_name == expected => {}
                Some(info_name) => {
                    self.inactivate();
                    return Err(NodeError::NameMismatch {
                        expected: expected.to_owned(),
                        got: info_name.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    fn add_friends(
        &self,
        current_aliases: &HashMap<Host, Arc<Self>>,
        info_map: &mut Info,
    ) -> Result<HashSet<Host>> {
        let friends = if self.client_policy.use_services_alternate {
            info_map.services_alternate.take()
        } else {
            info_map.services.take()
        };

        Ok(friends
            .ok_or(ClusterError::MissingServicesList)?
            .into_iter()
            .filter_map(|friend| {
                let alias = self
                    .client_policy
                    .ip_map
                    .as_ref()
                    .and_then(|map| {
                        map.get(&friend.name)
                            .map(|name| Host::new(name, friend.port))
                    })
                    .unwrap_or(friend);

                if current_aliases.contains_key(&alias) {
                    self.reference_count.fetch_add(1, Ordering::Relaxed);
                    None
                } else {
                    Some(alias)
                }
            })
            .collect())
    }

    fn update_partitions(&self, info_map: &Info) -> Result<()> {
        let gen = info_map
            .partition_generation
            .ok_or(ClusterError::MissingPartitionGeneration)?;
        self.partition_generation.store(gen, Ordering::Relaxed);

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
    pub async fn aliases(&self) -> RwLockReadGuard<'_, Vec<Host>> {
        self.aliases.read().await
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
        match commands::info_raw(&mut conn, commands).await {
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
