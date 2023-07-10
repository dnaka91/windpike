//! Policies that allow to adjust the behavior of various operations.

use std::{collections::HashMap, option::Option};

use tokio::time::{Duration, Instant};

use crate::commands::{self, CommandError};

/// Common parameters used for read operations and acts as base for most of the other policies.
#[derive(Clone, Debug)]
pub struct BasePolicy {
    /// Level of consistency guarantee for read operations that determines how many replicas are
    /// required to contain the same data set.
    pub consistency_level: ConsistencyLevel,
    /// The duration after which the transaction is cancelled (including retries).
    ///
    /// This value is sent to the server as well, so it will have an effect on both sides of the
    /// connection (in case the client doesn't properly cancel the operation).
    pub timeout: Duration,
    /// How many times to retry the operation, in case the transaction failed.
    pub max_retries: Option<usize>,
    /// The duration to sleep between retry attempts. Use a _zero_ duration to disable sleeping.
    pub sleep_between_retries: Duration,
    /// Send the user key on read and write operations. By default, only the hashed version is sent
    /// to reduce the amount of data transferred.
    pub send_key: bool,
}

impl BasePolicy {
    /// Default value for the [`Self::max_retries`] parameter.
    pub const DEFAULT_MAX_RETRIES: usize = 2;
    /// Default value for the [`Self::send_key`] parameter.
    pub const DEFAULT_SEND_KEY: bool = false;
    /// Default value for the [`Self::sleep_between_retries`] parameter.
    pub const DEFAULT_SLEEP_BETWEEN_RETRIES: Duration = Duration::from_millis(500);
    /// Default value for the [`Self::timeout`] parameter.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Deadline for current transaction based on specified timeout.
    #[must_use]
    pub(crate) fn deadline(&self) -> Option<Instant> {
        (!self.timeout.is_zero()).then(|| Instant::now() + self.timeout)
    }
}

impl Default for BasePolicy {
    fn default() -> Self {
        Self {
            timeout: Self::DEFAULT_TIMEOUT,
            max_retries: Some(Self::DEFAULT_MAX_RETRIES),
            sleep_between_retries: Self::DEFAULT_SLEEP_BETWEEN_RETRIES,
            consistency_level: ConsistencyLevel::default(),
            send_key: Self::DEFAULT_SEND_KEY,
        }
    }
}

impl AsRef<Self> for BasePolicy {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Level which defines the amount of replicas to contact on read operations to ensure the
/// consistency of the retrieved data.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsistencyLevel {
    /// Contact only a single replica to retrieve the data. **This is the default**.
    #[default]
    One = 0,
    /// Contact all replicas to ensure they contain the same data set.
    All = 1,
}

/// Parameters for all batch operations.
#[derive(Clone, Debug)]
pub struct BatchPolicy {
    /// The base policy that this one extends.
    pub base_policy: BasePolicy,
    /// Way in which nodes in the cluster are contacted to perform the batch operation. This has
    /// only an effect if the request actually requires to contact multiple nodes.
    pub concurrency: Concurrency,
    /// Allow the server to process the batch request immediately on its receiving thread. If
    /// disabled, processing is always scheduled and done on separate transaction threads.
    ///
    /// This setting can improve performance for small sized records, but can possibly introduce
    /// unfair processing of received commands.
    pub allow_inline: bool,
    /// For every key in the batch, send the set name as well.
    ///
    /// This is only required when authentication is enabled and per-set security roles are
    /// defined.
    pub send_set_name: bool,
}

impl BatchPolicy {
    /// Default value for the [`Self::allow_inline`] parameter.
    pub const DEFAULT_ALLOW_INLINE: bool = true;
    /// Default value for the [`Self::send_set_name`] parameter.
    pub const DEFAULT_SEND_SET_NAME: bool = false;
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            base_policy: BasePolicy::default(),
            concurrency: Concurrency::default(),
            allow_inline: Self::DEFAULT_ALLOW_INLINE,
            send_set_name: Self::DEFAULT_SEND_SET_NAME,
        }
    }
}

impl AsRef<BasePolicy> for BatchPolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}

/// Defines how a batch command should be executed, if it requires to be sent to multiple cluster
/// nodes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Concurrency {
    /// Execute the command one node at a time in sequence. **This is the default**.
    #[default]
    Sequential,
    /// Execute the command on all nodes concurrently, limited to the set amount. Using `0`
    /// disables the limit.
    Parallel(usize),
}

/// Parameters for creating new [`Client`](crate::Client) instances.
#[derive(Clone, Debug)]
pub struct ClientPolicy {
    /// Username and password pair to authenticate against the cluster. A value of [`None`]
    /// disabled the authentication altogether.
    pub user_password: Option<(String, String)>,
    /// Initial timeout when creating a new connection to the server.
    pub timeout: Option<Duration>,
    /// Idling time after which unused connections are closed.
    pub idle_timeout: Option<Duration>,
    /// Maximum amount of socket connections per node in the cluster.
    pub max_conns_per_node: u32,
    /// Return an error if the client is not initially connected to any nodes after creating a new
    /// instance.
    pub fail_if_not_connected: bool,
    /// Threshold after which the data buffer for each node connection will be shrunk to only the
    /// currently used memory size.
    ///
    /// Each buffer will grow over time, depending on the amount of raw response data and re-use
    /// any allocated memory for future operations. This setting allows to reduce the used memory
    /// by shrinking the buffer again after it has passed the threshold.
    pub buffer_reclaim_threshold: usize,
    /// Interval at which to check for changes in the cluster (like addition or removal of nodes).
    pub tend_interval: Duration,
    /// Translation table for cluster node IPs that allows to remap advertised nodes from info
    /// commands to their real IP.
    ///
    /// This setting is relevant when a mix of clients from both and internal and external network
    /// access the cluster, as IPs can be different.
    pub ip_map: Option<HashMap<String, String>>,
    /// Alternative to the [`Self::ip_map`], which instead uses the cluster servers' own configured
    /// external IP addresses, to determine the proper address for each server.
    pub use_services_alternate: bool,
    /// Expected name of the cluster. If set, all nodes must return this name to be allowed to join
    /// the list of nodes on the client side.
    ///
    /// This should only be set if all servers support the `cluster-name` info command.
    pub cluster_name: Option<String>,
}

impl ClientPolicy {
    /// Default value for the [`Self::buffer_reclaim_threshold`] parameter.
    pub const DEFAULT_BUFFER_RECLAIM_THRESHOLD: usize = 65536;
    /// Default value for the [`Self::fail_if_not_connected`] parameter.
    pub const DEFAULT_FAIL_IF_NOT_CONNECTED: bool = true;
    /// Default value for the [`Self::idle_timeout`] parameter.
    pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
    /// Default value for the [`Self::max_conns_per_node`] parameter.
    pub const DEFAULT_MAX_CONNS_PER_NODE: u32 = 256;
    /// Default value for the [`Self::tend_interval`] parameter.
    pub const DEFAULT_TEND_INTERVAL: Duration = Duration::from_secs(1);
    /// Default value for the [`Self::timeout`] parameter.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
    /// Default value for the [`Self::use_services_alternate`] parameter.
    pub const DEFAULT_USE_SERVICES_ALTERNATE: bool = false;

    /// Enable authentication and use the given username and password as credentials.
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

impl Default for ClientPolicy {
    fn default() -> Self {
        Self {
            user_password: None,
            timeout: Some(Self::DEFAULT_TIMEOUT),
            idle_timeout: Some(Self::DEFAULT_IDLE_TIMEOUT),
            max_conns_per_node: Self::DEFAULT_MAX_CONNS_PER_NODE,
            fail_if_not_connected: Self::DEFAULT_FAIL_IF_NOT_CONNECTED,
            buffer_reclaim_threshold: Self::DEFAULT_BUFFER_RECLAIM_THRESHOLD,
            tend_interval: Self::DEFAULT_TEND_INTERVAL,
            ip_map: None,
            use_services_alternate: Self::DEFAULT_USE_SERVICES_ALTERNATE,
            cluster_name: None,
        }
    }
}

/// Parameters for all scan operations.
#[derive(Clone, Debug)]
pub struct ScanPolicy {
    /// The base policy that this one extends.
    pub base_policy: BasePolicy,
    /// Maximum amount of time to wait before the scan operation is cancelled (on the server side).
    /// A duration of _zero_ can be used to disable the timeout.
    pub socket_timeout: Duration,
}

impl ScanPolicy {
    /// Default value for the [`Self::socket_timeout`] parameter.
    pub const DEFAULT_SOCKET_TIMEOUT: Duration = Duration::from_secs(10);
}

impl Default for ScanPolicy {
    fn default() -> Self {
        Self {
            base_policy: BasePolicy::default(),
            socket_timeout: Self::DEFAULT_SOCKET_TIMEOUT,
        }
    }
}

impl AsRef<BasePolicy> for ScanPolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}

/// Parameters for all write operations.
#[derive(Clone, Debug, Default)]
pub struct WritePolicy {
    /// The base policy that this one extends.
    pub base_policy: BasePolicy,
    /// Action to perform if an existing record was found on the server.
    pub record_exists_action: RecordExistsAction,
    /// Policy to limit the write of a record based on its generation.
    pub generation_policy: GenerationPolicy,
    /// Consistency level of the write operation.
    pub commit_level: CommitLevel,
    /// The expected generation, which defines how many times the record has been modified on the
    /// server. Only effective if the [`Self::generation_policy`] is set to any other value than
    /// [`GenerationPolicy::None`].
    pub generation: u32,
    /// Amount of time the record will exist until it is auto-deleted by the server.
    pub expiration: Expiration,
    /// When sending multiple operations at once, define whether a result should be returned for
    /// each operation. Note that some operations might not return a result at all.
    pub respond_per_each_op: bool,
    /// Create a tombstone for deleted records, which prevents them from re-appearing after a node
    /// in the cluster failed.
    pub durable_delete: bool,
}

impl WritePolicy {
    /// Create a new write policy with given generation and expiration.
    ///
    /// This is a shorthand for common operations. Alternatively the write policy can be created
    /// manually, allowing to set additional parameters at once.
    #[must_use]
    pub fn new(generation: u32, expiration: Expiration) -> Self {
        Self {
            generation,
            expiration,
            ..Self::default()
        }
    }
}

impl AsRef<BasePolicy> for WritePolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}

/// Action that is to be performed when a record write operation encounters an already existing
/// entry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum RecordExistsAction {
    /// Update the existing record, or create it if it doesn't exist yet. Existing bins will be
    /// merged with new ones. **This is the default**.
    #[default]
    Update = 0,
    /// Only update the record if it exists, and fail if it's missing. Existing bins will be merged
    /// with new ones like with [`Self::Update`].
    UpdateOnly,
    /// Fully replace an existing record, or create it if it doesn't exist yet. Existing bins will
    /// be deleted and replaced.
    Replace,
    /// Only replace an existing record if it exists, and fail if it's missing. Existing bins will
    /// be deleted and replaced, like with [`Self::Replace`].
    ReplaceOnly,
    /// Only create a new record if it doesn't exist yet, and fail otherwise.
    CreateOnly,
}

/// Policy that defines how to limit record update and delete operations by its generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum GenerationPolicy {
    /// Don't limit the record write by generation. **This is the default**.
    #[default]
    None = 0,
    /// Only update or delete the record, if the set generation matches the server's.
    ExpectGenEqual,
    /// Only update or delete the record, if the set generation is greater than the server's.
    ExpectGenGreater,
}

/// Level that defines at what point a record edit operation is considered complete.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CommitLevel {
    /// Wait until the record is written to the master node as well as all replicas. **This is the
    /// default**.
    #[default]
    All = 0,
    /// Wait until the record is only written on the master node.
    Master,
}

/// Record expiration, also known as time-to-live (usually abbreviated as TTL).
#[derive(Clone, Copy, Debug, Default)]
pub enum Expiration {
    /// Amount of seconds (counted from now) until a record expires.
    Seconds(u32),
    /// Use the namespace's default TTL that the record is saved in. **This is the default**.
    #[default]
    NamespaceDefault,
    /// Never delete the record by time.
    Never,
    /// When updating the record, don't update its existing TTL.
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
