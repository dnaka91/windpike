pub mod node;
pub mod node_validator;
pub mod partition;
pub mod partition_tokenizer;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    vec::Vec,
};

use tokio::{
    sync::RwLock,
    task::JoinError,
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};

pub use self::node::Node;
use self::{
    node_validator::NodeValidator, partition::Partition, partition_tokenizer::PartitionTokenizer,
};
use crate::{net::Host, policy::ClientPolicy};

type Result<T, E = ClusterError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("Missing replicas information")]
    MissingReplicas,
    #[error("Error parsing partition information")]
    InvalidPartitionInfo,
    #[error("Invalid UTF-8 content discovered")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("Invalid integer")]
    InvalidInteger(#[from] std::num::ParseIntError),
    #[error("Base64 decoding error")]
    Base64(#[from] base64::DecodeError),
    #[error(
        "Failed to connect to host(s). The network connection(s) to cluster nodes may have timed \
         out, or the cluster may be in a state of flux."
    )]
    Connection,
    #[error("Networking error")]
    Network(#[from] crate::net::NetError),
    #[error("Command error")]
    Command(#[from] crate::commands::CommandError),
    #[error("Missing services list")]
    MissingServicesList,
    #[error("Missing partition generation")]
    MissingPartitionGeneration,
    #[error("Error during initial cluster tend")]
    InitialTend(#[source] JoinError),
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("No addresses for host `{host}`")]
    NoAddress { host: Host },
    #[error("Missing node name")]
    MissingNodeName,
    #[error("Missing cluster name")]
    MissingClusterName,
    #[error("Cluster name mismatch. Expected `{expected}`, but got `{got}`")]
    NameMismatch { expected: String, got: String },
    #[error("Networking error")]
    Net(#[from] crate::net::NetError),
    #[error("I/O related error")]
    Io(#[from] std::io::Error),
    #[error("Command error")]
    Command(#[from] crate::commands::CommandError),
}

#[derive(Debug, thiserror::Error)]
pub enum NodeRefreshError {
    #[error("Info command failed")]
    InfoCommandFailed(#[source] ClusterError),
    #[error("Failed to validate node")]
    ValidationFailed(#[source] NodeError),
    #[error("Failed to add friends")]
    FailedAddingFriends(#[source] ClusterError),
    #[error("Failed to update partitions")]
    FailedUpdatingPartitions(#[source] ClusterError),
}

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
#[derive(Debug)]
pub struct Cluster {
    // Initial host nodes specified by user.
    seeds: Arc<RwLock<Vec<Host>>>,

    // All aliases for all nodes in cluster.
    aliases: Arc<RwLock<HashMap<Host, Arc<Node>>>>,

    // Active nodes in cluster.
    nodes: Arc<RwLock<Vec<Arc<Node>>>>,

    // Hints for best node for a partition
    partition_write_map: Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>>,

    // Random node index.
    node_index: AtomicUsize,

    client_policy: ClientPolicy,

    closed: AtomicBool,
}

impl Cluster {
    pub async fn new(policy: ClientPolicy, hosts: &[Host]) -> Result<Arc<Self>> {
        let cluster = Arc::new(Self {
            client_policy: policy,

            seeds: Arc::new(RwLock::new(hosts.to_vec())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(vec![])),

            partition_write_map: Arc::new(RwLock::new(HashMap::new())),
            node_index: AtomicUsize::new(0),

            closed: AtomicBool::new(false),
        });
        // try to seed connections for first use
        Self::wait_till_stabilized(Arc::clone(&cluster)).await?;

        // apply policy rules
        if cluster.client_policy.fail_if_not_connected && !cluster.is_connected().await {
            return Err(ClusterError::Connection);
        }

        let cluster_for_tend = Arc::clone(&cluster);
        tokio::spawn(Self::tend_thread(cluster_for_tend));

        debug!("New cluster initialized and ready to be used...");

        Ok(cluster)
    }

    async fn tend_thread(cluster: Arc<Self>) {
        let tend_interval = cluster.client_policy.tend_interval;

        while !cluster.closed.load(Ordering::Relaxed) {
            if let Err(err) = cluster.tend().await {
                error!(error = ?err, "Error tending cluster");
            }
            tokio::time::sleep(tend_interval).await;
        }
    }

    async fn tend(&self) -> Result<()> {
        let mut nodes = self.nodes().await;

        // All node additions/deletions are performed in tend thread.
        // If active nodes don't exist, seed cluster.
        if nodes.is_empty() {
            debug!("No connections available; seeding...");
            self.seed_nodes().await;
            nodes = self.nodes().await;
        }

        let mut friend_list: Vec<Host> = vec![];
        let mut refresh_count = 0;

        // Refresh all known nodes.
        for node in nodes {
            let old_gen = node.partition_generation();
            if node.is_active() {
                match node.refresh(&self.aliases().await).await {
                    Ok(friends) => {
                        refresh_count += 1;

                        if !friends.is_empty() {
                            friend_list.extend_from_slice(&friends);
                        }

                        if old_gen != node.partition_generation() {
                            self.update_partitions(Arc::clone(&node)).await?;
                        }
                    }
                    Err(err) => {
                        node.increase_failures();
                        warn!(?node, %err, "Node refresh failed");
                    }
                }
            }
        }

        // Add nodes in a batch.
        let add_list = self.find_new_nodes_to_add(friend_list).await;
        self.add_nodes_and_aliases(&add_list).await;

        // IMPORTANT: Remove must come after add to remove aliases
        // Handle nodes changes determined from refreshes.
        // Remove nodes in a batch.
        let remove_list = self.find_nodes_to_remove(refresh_count).await;
        self.remove_nodes_and_aliases(remove_list).await;

        Ok(())
    }

    async fn wait_till_stabilized(cluster: Arc<Self>) -> Result<()> {
        let timeout = cluster
            .client_policy()
            .timeout
            .unwrap_or_else(|| Duration::from_secs(3));
        let deadline = Instant::now() + timeout;

        let handle = tokio::spawn(async move {
            let mut count: isize = -1;
            loop {
                if Instant::now() > deadline {
                    break;
                }

                if let Err(err) = cluster.tend().await {
                    error!(error = ?err, "Error during initial cluster tend");
                }

                let old_count = count;
                // unlikely that there are ever more than isize::MAX nodes
                count = cluster.node_count().await.try_into().unwrap_or(isize::MAX);
                if count == old_count {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        handle.await.map_err(ClusterError::InitialTend)
    }

    pub const fn cluster_name(&self) -> &Option<String> {
        &self.client_policy.cluster_name
    }

    pub const fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub async fn add_seeds(&self, new_seeds: &[Host]) -> Result<()> {
        let mut seeds = self.seeds.write().await;
        seeds.extend_from_slice(new_seeds);

        Ok(())
    }

    pub async fn alias_exists(&self, host: &Host) -> Result<bool> {
        let aliases = self.aliases.read().await;
        Ok(aliases.contains_key(host))
    }

    async fn set_partitions(&self, partitions: HashMap<String, Vec<Arc<Node>>>) {
        let mut partition_map = self.partition_write_map.write().await;
        *partition_map = partitions;
    }

    fn partitions(&self) -> Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>> {
        Arc::clone(&self.partition_write_map)
    }

    pub async fn node_partitions(&self, node: &Node, namespace: &str) -> Vec<u16> {
        let mut res: Vec<u16> = vec![];
        let partitions = self.partitions();
        let partitions = partitions.read().await;

        if let Some(node_array) = partitions.get(namespace) {
            for (i, tnode) in node_array.iter().enumerate() {
                if node.name() == tnode.name() {
                    res.push(i as u16);
                }
            }
        }

        res
    }

    pub async fn update_partitions(&self, node: Arc<Node>) -> Result<()> {
        let mut conn = node.get_connection().await?;
        let tokens = match PartitionTokenizer::new(&mut conn).await {
            Ok(tokens) => tokens,
            Err(e) => {
                conn.invalidate().await;
                return Err(e);
            }
        };

        let nmap = tokens.update_partition(self.partitions(), node).await?;
        self.set_partitions(nmap).await;

        Ok(())
    }

    pub async fn seed_nodes(&self) -> bool {
        let seed_array = self.seeds.read().await;

        info!(seeds_count = seed_array.len(), "Seeding the cluster");

        let mut list: Vec<Arc<Node>> = vec![];
        for seed in &*seed_array {
            let mut seed_node_validator = NodeValidator::new(self);
            if let Err(err) = seed_node_validator.validate_node(self, seed).await {
                error!(error = ?err, %seed, "Failed to validate seed host");
                continue;
            };

            for alias in &*seed_node_validator.aliases() {
                let nv = if *seed == *alias {
                    seed_node_validator.clone()
                } else {
                    let mut nv2 = NodeValidator::new(self);
                    if let Err(err) = nv2.validate_node(self, seed).await {
                        error!(error = ?err, %alias, "Seeding host failed with error");
                        continue;
                    };
                    nv2
                };

                if Self::find_node_name(&list, &nv.name) {
                    continue;
                }

                let node = self.create_node(&nv);
                let node = Arc::new(node);
                self.add_aliases(Arc::clone(&node)).await;
                list.push(node);
            }
        }

        self.add_nodes_and_aliases(&list).await;
        !list.is_empty()
    }

    fn find_node_name(list: &[Arc<Node>], name: &str) -> bool {
        list.iter().any(|node| node.name() == name)
    }

    async fn find_new_nodes_to_add(&self, hosts: Vec<Host>) -> Vec<Arc<Node>> {
        let mut list: Vec<Arc<Node>> = vec![];

        for host in hosts {
            let mut nv = NodeValidator::new(self);
            if let Err(err) = nv.validate_node(self, &host).await {
                error!(error = ?err, %host, "Adding node failed with error");
                continue;
            };

            // Duplicate node name found. This usually occurs when the server
            // services list contains both internal and external IP addresses
            // for the same node. Add new host to list of alias filters
            // and do not add new node.
            let mut dup = false;
            match self.get_node_by_name(&nv.name).await {
                Some(node) => {
                    self.add_alias(host, Arc::clone(&node)).await;
                    dup = true;
                }
                None => {
                    if let Some(node) = list.iter().find(|n| n.name() == nv.name) {
                        self.add_alias(host, Arc::clone(node)).await;
                        dup = true;
                    }
                }
            };

            if !dup {
                let node = self.create_node(&nv);
                list.push(Arc::new(node));
            }
        }

        list
    }

    fn create_node(&self, nv: &NodeValidator) -> Node {
        Node::new(self.client_policy.clone(), nv)
    }

    async fn find_nodes_to_remove(&self, refresh_count: usize) -> Vec<Arc<Node>> {
        let nodes = self.nodes().await;
        let mut remove_list: Vec<Arc<Node>> = vec![];
        let cluster_size = nodes.len();
        for node in nodes {
            let tnode = Arc::clone(&node);

            if !node.is_active() {
                remove_list.push(tnode);
                continue;
            }

            match cluster_size {
                // Single node clusters rely on whether it responded to info requests.
                1 if node.failures() > 5 => {
                    // 5 consecutive info requests failed. Try seeds.
                    if self.seed_nodes().await {
                        remove_list.push(tnode);
                    }
                }

                // Two node clusters require at least one successful refresh before removing.
                2 if refresh_count == 1 && node.reference_count() == 0 && node.failures() > 0 => {
                    remove_list.push(node);
                }

                _ => {
                    // Multi-node clusters require two successful node refreshes before removing.
                    if refresh_count >= 2 && node.reference_count() == 0 {
                        // Node is not referenced by other nodes.
                        // Check if node responded to info request.
                        if node.failures() == 0 {
                            // Node is alive, but not referenced by other nodes.  Check if mapped.
                            if !self.find_node_in_partition_map(node).await {
                                remove_list.push(tnode);
                            }
                        } else {
                            // Node not responding. Remove it.
                            remove_list.push(tnode);
                        }
                    }
                }
            }
        }

        remove_list
    }

    async fn add_nodes_and_aliases(&self, friend_list: &[Arc<Node>]) {
        for node in friend_list {
            self.add_aliases(Arc::clone(node)).await;
        }
        self.add_nodes(friend_list).await;
    }

    async fn remove_nodes_and_aliases(&self, mut nodes_to_remove: Vec<Arc<Node>>) {
        for node in &mut nodes_to_remove {
            for alias in node.aliases().await {
                self.remove_alias(&alias).await;
            }
            if let Some(node) = Arc::get_mut(node) {
                node.close().await;
            }
        }
        self.remove_nodes(&nodes_to_remove).await;
    }

    async fn add_alias(&self, host: Host, node: Arc<Node>) {
        let mut aliases = self.aliases.write().await;
        node.add_alias(host.clone()).await;
        aliases.insert(host, node);
    }

    async fn remove_alias(&self, host: &Host) {
        let mut aliases = self.aliases.write().await;
        aliases.remove(host);
    }

    async fn add_aliases(&self, node: Arc<Node>) {
        let mut aliases = self.aliases.write().await;
        for alias in node.aliases().await {
            aliases.insert(alias, Arc::clone(&node));
        }
    }

    async fn find_node_in_partition_map(&self, filter: Arc<Node>) -> bool {
        let partitions = self.partition_write_map.read().await;
        (*partitions)
            .values()
            .any(|map| map.iter().any(|node| node.name() == filter.name()))
    }

    async fn add_nodes(&self, friend_list: &[Arc<Node>]) {
        if friend_list.is_empty() {
            return;
        }

        let mut nodes = self.nodes().await;
        nodes.extend(friend_list.iter().cloned());
        self.set_nodes(nodes).await;
    }

    async fn remove_nodes(&self, nodes_to_remove: &[Arc<Node>]) {
        if nodes_to_remove.is_empty() {
            return;
        }

        let nodes = self
            .nodes()
            .await
            .into_iter()
            .filter(|node| nodes_to_remove.iter().all(|rem| rem.name() != node.name()))
            .collect();

        self.set_nodes(nodes).await;
    }

    pub async fn is_connected(&self) -> bool {
        let nodes = self.nodes().await;
        let closed = self.closed.load(Ordering::Relaxed);
        !nodes.is_empty() && !closed
    }

    pub async fn aliases(&self) -> HashMap<Host, Arc<Node>> {
        self.aliases.read().await.clone()
    }

    pub async fn nodes(&self) -> Vec<Arc<Node>> {
        self.nodes.read().await.clone()
    }

    async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    async fn set_nodes(&self, new_nodes: Vec<Arc<Node>>) {
        let mut nodes = self.nodes.write().await;
        *nodes = new_nodes;
    }

    pub async fn get_node(&self, partition: &Partition<'_>) -> Option<Arc<Node>> {
        let node = {
            let partitions = self.partitions();
            let partitions = partitions.read().await;

            partitions
                .get(partition.namespace)
                .and_then(|node_array| node_array.get(partition.partition_id))
                .cloned()
        };

        if node.is_none() {
            self.get_random_node().await
        } else {
            node
        }
    }

    pub async fn get_random_node(&self) -> Option<Arc<Node>> {
        let node_array = self.nodes().await;
        let length = node_array.len();

        (0..length)
            .find_map(|_| {
                let index = (self.node_index.fetch_add(1, Ordering::Relaxed) + 1) % length;
                node_array.get(index).filter(|node| node.is_active())
            })
            .map(Arc::clone)
    }

    pub async fn get_node_by_name(&self, node_name: &str) -> Option<Arc<Node>> {
        self.nodes()
            .await
            .iter()
            .find(|node| node.name() == node_name)
            .cloned()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}
