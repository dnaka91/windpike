//! Policy types encapsulate optional parameters for various client operations.

use std::{collections::HashMap, option::Option};

use tokio::time::{Duration, Instant};

use crate::commands::{self, CommandError};

/// Trait implemented by most policy types; policies that implement this trait typically encompass
/// an instance of `BasePolicy`.
pub trait Policy {
    /// Transaction priority.
    fn priority(&self) -> Priority;

    #[doc(hidden)]
    /// Deadline for current transaction based on specified timeout. For internal use only.
    fn deadline(&self) -> Option<Instant>;

    /// Total transaction timeout for both client and server. The timeout is tracked on the client
    /// and also sent to the server along with the transaction in the wire protocol. The client
    /// will most likely timeout first, but the server has the capability to timeout the
    /// transaction as well.
    ///
    /// The timeout is also used as a socket timeout. Default: 0 (no timeout).
    fn timeout(&self) -> Option<Duration>;

    /// Maximum number of retries before aborting the current transaction. A retry may be attempted
    /// when there is a network error. If `max_retries` is exceeded, the abort will occur even if
    /// the timeout has not yet been exceeded.
    fn max_retries(&self) -> Option<usize>;

    /// Time to sleep between retries. Set to zero to skip sleep. Default: 500ms.
    fn sleep_between_retries(&self) -> Option<Duration>;

    /// How replicas should be consulted in read operations to provide the desired consistency
    /// guarantee.
    fn consistency_level(&self) -> ConsistencyLevel;
}

impl<T> Policy for T
where
    T: AsRef<BasePolicy>,
{
    fn priority(&self) -> Priority {
        self.as_ref().priority()
    }

    fn deadline(&self) -> Option<Instant> {
        self.as_ref().deadline()
    }

    fn timeout(&self) -> Option<Duration> {
        self.as_ref().timeout()
    }

    fn max_retries(&self) -> Option<usize> {
        self.as_ref().max_retries()
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.as_ref().sleep_between_retries()
    }

    fn consistency_level(&self) -> ConsistencyLevel {
        self.as_ref().consistency_level()
    }
}

/// Priority of operations on database server.
#[derive(Clone, Copy, Debug, Default)]
pub enum Priority {
    /// Default determines that the server defines the priority.
    #[default]
    Default = 0,
    /// Low determines that the server should run the operation in a background thread.
    Low,
    /// Medium determines that the server should run the operation at medium priority.
    Medium,
    /// High determines that the server should run the operation at the highest priority.
    High,
}

/// `ConsistencyLevel` indicates how replicas should be consulted in a read
/// operation to provide the desired consistency guarantee.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsistencyLevel {
    /// ConsistencyOne indicates only a single replica should be consulted in
    /// the read operation.
    #[default]
    ConsistencyOne = 0,
    /// ConsistencyAll indicates that all replicas should be consulted in
    /// the read operation.
    ConsistencyAll = 1,
}

/// Common parameters shared by all policy types.
#[derive(Clone, Debug)]
pub struct BasePolicy {
    /// Priority of request relative to other transactions.
    /// Currently, only used for scans.
    /// This is deprected for Scan/Query commands and will not be sent to the server.
    pub priority: Priority,

    /// How replicas should be consulted in a read operation to provide the desired
    /// consistency guarantee. Default to allowing one replica to be used in the
    /// read operation.
    pub consistency_level: ConsistencyLevel,

    /// Timeout specifies transaction timeout.
    /// This timeout is used to set the socket timeout and is also sent to the
    /// server along with the transaction in the wire protocol.
    /// Default to no timeout (0).
    pub timeout: Option<Duration>,

    /// MaxRetries determines maximum number of retries before aborting the current transaction.
    /// A retry is attempted when there is a network error other than timeout.
    /// If maxRetries is exceeded, the abort will occur even if the timeout
    /// has not yet been exceeded.
    pub max_retries: Option<usize>,

    /// SleepBetweenReplies determines duration to sleep between retries if a
    /// transaction fails and the timeout was not exceeded.  Enter zero to skip sleep.
    pub sleep_between_retries: Option<Duration>,

    /// Send user defined key in addition to hash digest on both reads and writes.
    /// The default is to not send the user defined key.
    pub send_key: bool,
}

impl Default for BasePolicy {
    fn default() -> Self {
        Self {
            priority: Priority::default(),
            timeout: Some(Duration::new(30, 0)),
            max_retries: Some(2),
            sleep_between_retries: Some(Duration::new(0, 500_000_000)),
            consistency_level: ConsistencyLevel::default(),
            send_key: false,
        }
    }
}

impl Policy for BasePolicy {
    fn priority(&self) -> Priority {
        self.priority
    }

    fn deadline(&self) -> Option<Instant> {
        self.timeout.map(|timeout| Instant::now() + timeout)
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    fn max_retries(&self) -> Option<usize> {
        self.max_retries
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.sleep_between_retries
    }

    fn consistency_level(&self) -> ConsistencyLevel {
        self.consistency_level
    }
}

/// `BatchPolicy` encapsulates parameters for all batch operations.
#[derive(Clone, Debug)]
pub struct BatchPolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// Concurrency mode for batch requests: Sequential or Parallel (with optional max. no of
    /// parallel threads).
    pub concurrency: Concurrency,

    /// Allow batch to be processed immediately in the server's receiving thread when the server
    /// deems it to be appropriate. If false, the batch will always be processed in separate
    /// transaction threads.
    ///
    /// For batch exists or batch reads of smaller sized records (<= 1K per record), inline
    /// processing will be significantly faster on "in memory" namespaces. The server disables
    /// inline processing on disk based namespaces regardless of this policy field.
    ///
    /// Inline processing can introduce the possibility of unfairness because the server can
    /// process the entire batch before moving onto the next command.
    ///
    /// Default: true
    pub allow_inline: bool,

    /// Send set name field to server for every key in the batch. This is only necessary when
    /// authentication is enabled and security roles are defined on a per-set basis.
    ///
    /// Default: false
    pub send_set_name: bool,
}

impl BatchPolicy {
    /// Create a new batch policy instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            base_policy: BasePolicy::default(),
            concurrency: Concurrency::default(),
            allow_inline: true,
            send_set_name: false,
        }
    }
}

impl AsRef<BasePolicy> for BatchPolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}

/// Specifies whether a command, that needs to be executed on multiple cluster nodes, should be
/// executed sequentially, one node at a time, or in parallel on multiple nodes using the client's
/// thread pool.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Concurrency {
    /// Issue commands sequentially. This mode has a performance advantage for small to
    /// medium sized batch sizes because requests can be issued in the main transaction thread.
    /// This is the default.
    #[default]
    Sequential,
    /// Issue all commands in parallel threads. This mode has a performance advantage for
    /// extremely large batch sizes because each node can process the request immediately. The
    /// downside is extra threads will need to be created (or takedn from a thread pool).
    Parallel,
    /// Issue up to N commands in parallel threads. When a request completes, a new request
    /// will be issued until all threads are complete. This mode prevents too many parallel threads
    /// being created for large cluster implementations. The downside is extra threads will still
    /// need to be created (or taken from a thread pool).
    ///
    /// E.g. if there are 16 nodes/namespace combinations requested and concurrency is set to
    /// `MaxThreads(8)`, then batch requests will be made for 8 node/namespace combinations in
    /// parallel threads. When a request completes, a new request will be issued until all 16
    /// requests are complete.
    MaxThreads(usize),
}

/// `ClientPolicy` encapsulates parameters for client policy command.
#[derive(Clone, Debug)]
pub struct ClientPolicy {
    /// User authentication to cluster. Leave empty for clusters running without restricted access.
    pub user_password: Option<(String, String)>,

    /// Initial host connection timeout in milliseconds.  The timeout when opening a connection
    /// to the server host for the first time.
    pub timeout: Option<Duration>,

    /// Connection idle timeout. Every time a connection is used, its idle
    /// deadline will be extended by this duration. When this deadline is reached,
    /// the connection will be closed and discarded from the connection pool.
    pub idle_timeout: Option<Duration>,

    /// Maximum number of synchronous connections allowed per server node.
    pub max_conns_per_node: u32,

    /// Throw exception if host connection fails during addHost().
    pub fail_if_not_connected: bool,

    /// Threshold at which the buffer attached to the connection will be shrunk by deallocating
    /// memory instead of just resetting the size of the underlying vec.
    /// Should be set to a value that covers as large a percentile of payload sizes as possible,
    /// while also being small enough not to occupy a significant amount of memory for the life
    /// of the connection pool.
    pub buffer_reclaim_threshold: usize,

    /// TendInterval determines interval for checking for cluster state changes.
    /// Minimum possible interval is 10 Milliseconds.
    pub tend_interval: Duration,

    /// A IP translation table is used in cases where different clients
    /// use different server IP addresses.  This may be necessary when
    /// using clients from both inside and outside a local area
    /// network. Default is no translation.
    /// The key is the IP address returned from friend info requests to other servers.
    /// The value is the real IP address used to connect to the server.
    pub ip_map: Option<HashMap<String, String>>,

    /// UseServicesAlternate determines if the client should use "services-alternate"
    /// instead of "services" in info request during cluster tending.
    /// "services-alternate" returns server configured external IP addresses that client
    /// uses to talk to nodes.  "services-alternate" can be used in place of
    /// providing a client "ipMap".
    /// This feature is recommended instead of using the client-side IpMap above.
    ///
    /// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
    pub use_services_alternate: bool,

    /// Size of the thread pool used in scan and query commands. These commands are often sent to
    /// multiple server nodes in parallel threads. A thread pool improves performance because
    /// threads do not have to be created/destroyed for each command.
    pub thread_pool_size: usize,

    /// Expected cluster name. It not `None`, server nodes must return this cluster name in order
    /// to join the client's view of the cluster. Should only be set when connecting to servers
    /// that support the "cluster-name" info command.
    pub cluster_name: Option<String>,
}

impl Default for ClientPolicy {
    fn default() -> Self {
        Self {
            user_password: None,
            timeout: Some(Duration::new(30, 0)),
            idle_timeout: Some(Duration::new(5, 0)),
            max_conns_per_node: 256,
            fail_if_not_connected: true,
            tend_interval: Duration::new(1, 0),
            ip_map: None,
            use_services_alternate: false,
            thread_pool_size: 128,
            cluster_name: None,
            buffer_reclaim_threshold: 65536,
        }
    }
}

impl ClientPolicy {
    /// Set username and password to use when authenticating to the cluster.
    pub fn set_user_password(
        &mut self,
        username: String,
        password: &str,
    ) -> Result<(), CommandError> {
        let password = commands::hash_password(password)?;
        self.user_password = Some((username, password));
        Ok(())
    }
}

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

/// `ScanPolicy` encapsulates optional parameters used in scan operations.
#[derive(Clone, Debug)]
pub struct ScanPolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// Percent of data to scan. Valid integer range is 1 to 100. Default is 100.
    /// This is deprected and won't be sent to the server.
    pub scan_percent: u8,

    /// Maximum number of concurrent requests to server nodes at any point in time. If there are 16
    /// nodes in the cluster and `max_concurrent_nodes` is 8, then scan requests will be made to 8
    /// nodes in parallel. When a scan completes, a new scan request will be issued until all 16
    /// nodes have been scanned. Default (0) is to issue requests to all server nodes in parallel.
    pub max_concurrent_nodes: usize,

    /// Number of records to place in queue before blocking. Records received from multiple server
    /// nodes will be placed in a queue. A separate thread consumes these records in parallel. If
    /// the queue is full, the producer threads will block until records are consumed.
    pub record_queue_size: usize,

    /// Terminate scan if cluster is in fluctuating state.
    /// This is deprected and won't be sent to the server.
    pub fail_on_cluster_change: bool,

    /// Maximum time in milliseconds to wait when polling socket for availability prior to
    /// performing an operation on the socket on the server side. Zero means there is no socket
    /// timeout. Default: 10,000 ms.
    pub socket_timeout: Duration,
}

impl ScanPolicy {
    /// Create a new scan policy instance with default parameters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ScanPolicy {
    fn default() -> Self {
        Self {
            base_policy: BasePolicy::default(),
            scan_percent: 100,
            max_concurrent_nodes: 0,
            record_queue_size: 1024,
            fail_on_cluster_change: true,
            socket_timeout: Duration::from_secs(10),
        }
    }
}

impl AsRef<BasePolicy> for ScanPolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}

/// `WritePolicy` encapsulates parameters for all write operations.
#[derive(Clone, Debug, Default)]
pub struct WritePolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// RecordExistsAction qualifies how to handle writes where the record already exists.
    pub record_exists_action: RecordExistsAction,

    /// GenerationPolicy qualifies how to handle record writes based on record generation.
    /// The default (NONE) indicates that the generation is not used to restrict writes.
    pub generation_policy: GenerationPolicy,

    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    pub commit_level: CommitLevel,

    /// Generation determines expected generation.
    /// Generation is the number of times a record has been
    /// modified (including creation) on the server.
    /// If a write operation is creating a record, the expected generation would be 0.
    pub generation: u32,

    /// Expiration determimes record expiration in seconds. Also known as TTL (Time-To-Live).
    /// Seconds record will live before being removed by the server.
    pub expiration: Expiration,

    /// For Client::operate() method, return a result for every operation.
    /// Some list operations do not return results by default (`operations::list::clear()` for
    /// example). This can sometimes make it difficult to determine the desired result offset in
    /// the returned bin's result list.
    ///
    /// Setting RespondPerEachOp to true makes it easier to identify the desired result offset
    /// (result offset equals bin's operate sequence). This only makes sense when multiple list
    /// operations are used in one operate call and some of those operations do not return results
    /// by default.
    pub respond_per_each_op: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures.  Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,
}

impl WritePolicy {
    /// Create a new write policy instance with the specified generation and expiration parameters.
    #[must_use]
    pub fn new(gen: u32, exp: Expiration) -> Self {
        Self {
            generation: gen,
            expiration: exp,
            ..Self::default()
        }
    }
}

impl AsRef<BasePolicy> for WritePolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}

/// `RecordExistsAction` determines how to handle record writes based on record generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum RecordExistsAction {
    /// Update means: Create or update record.
    /// Merge write command bins with existing bins.
    #[default]
    Update = 0,
    /// UpdateOnly means: Update record only. Fail if record does not exist.
    /// Merge write command bins with existing bins.
    UpdateOnly,
    /// Replace means: Create or replace record.
    /// Delete existing bins not referenced by write command bins.
    /// Supported by Aerospike 2 server versions >= 2.7.5 and
    /// Aerospike 3 server versions >= 3.1.6.
    Replace,
    /// ReplaceOnly means: Replace record only. Fail if record does not exist.
    /// Delete existing bins not referenced by write command bins.
    /// Supported by Aerospike 2 server versions >= 2.7.5 and
    /// Aerospike 3 server versions >= 3.1.6.
    ReplaceOnly,
    /// CreateOnly means: Create only. Fail if record exists.
    CreateOnly,
}

/// `GenerationPolicy` determines how to handle record writes based on record generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum GenerationPolicy {
    /// None means: Do not use record generation to restrict writes.
    #[default]
    None = 0,
    /// ExpectGenEqual means: Update/delete record if expected generation is equal to server
    /// generation. Otherwise, fail.
    ExpectGenEqual,
    /// ExpectGenGreater means: Update/delete record if expected generation greater than the server
    /// generation. Otherwise, fail. This is useful for restore after backup.
    ExpectGenGreater,
}

/// `CommitLevel` determines how to handle record writes based on record generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CommitLevel {
    /// CommitAll indicates the server should wait until successfully committing master and all
    /// replicas.
    #[default]
    CommitAll = 0,
    /// CommitMaster indicates the server should wait until successfully committing master only.
    CommitMaster,
}

/// Record expiration, also known as time-to-live (TTL).
#[derive(Clone, Copy, Debug, Default)]
pub enum Expiration {
    /// Set the record to expire X seconds from now
    Seconds(u32),
    /// Set the record's expiry time using the default time-to-live (TTL) value for the namespace
    #[default]
    NamespaceDefault,
    /// Set the record to never expire. Requires Aerospike 2 server version 2.7.2 or later or
    /// Aerospike 3 server version 3.1.4 or later. Do not use with older servers.
    Never,
    /// Do not change the record's expiry time when updating the record; requires Aerospike server
    /// version 3.10.1 or later.
    DontUpdate,
}

impl From<Expiration> for u32 {
    fn from(value: Expiration) -> Self {
        match value {
            Expiration::Seconds(secs) => secs,
            Expiration::NamespaceDefault => 0,
            Expiration::Never => u32::MAX,
            Expiration::DontUpdate => u32::MAX - 1,
        }
    }
}
