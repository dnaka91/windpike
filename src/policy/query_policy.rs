use crate::policy::BasePolicy;

/// `QueryPolicy` encapsulates parameters for query operations.
#[derive(Clone, Debug)]
pub struct QueryPolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// Maximum number of concurrent requests to server nodes at any point in time. If there are 16
    /// nodes in the cluster and `max_concurrent_nodes` is 8, then queries will be made to 8 nodes
    /// in parallel. When a query completes, a new query will be issued until all 16 nodes have
    /// been queried. Default (0) is to issue requests to all server nodes in parallel.
    pub max_concurrent_nodes: usize,

    /// Number of records to place in queue before blocking. Records received from multiple server
    /// nodes will be placed in a queue. A separate thread consumes these records in parallel. If
    /// the queue is full, the producer threads will block until records are consumed.
    pub record_queue_size: usize,

    /// Terminate query if cluster is in fluctuating state.
    pub fail_on_cluster_change: bool,
}

impl QueryPolicy {
    /// Create a new query policy instance with default parameters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for QueryPolicy {
    fn default() -> Self {
        Self {
            base_policy: BasePolicy::default(),
            max_concurrent_nodes: 0,
            record_queue_size: 1024,
            fail_on_cluster_change: true,
        }
    }
}

impl AsRef<BasePolicy> for QueryPolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}
