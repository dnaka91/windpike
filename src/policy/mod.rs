//! Policy types encapsulate optional parameters for various client operations.
#![allow(clippy::missing_errors_doc)]

mod admin_policy;
mod batch_policy;
mod client_policy;
mod commit_level;
mod concurrency;
mod consistency_level;
mod expiration;
mod generation_policy;
mod priority;
mod query_policy;
mod read_policy;
mod record_exists_action;
mod scan_policy;
mod write_policy;

use std::option::Option;

use tokio::time::{Duration, Instant};

pub use self::{
    admin_policy::AdminPolicy, batch_policy::BatchPolicy, client_policy::ClientPolicy,
    commit_level::CommitLevel, concurrency::Concurrency, consistency_level::ConsistencyLevel,
    expiration::Expiration, generation_policy::GenerationPolicy, priority::Priority,
    query_policy::QueryPolicy, read_policy::ReadPolicy, record_exists_action::RecordExistsAction,
    scan_policy::ScanPolicy, write_policy::WritePolicy,
};

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

/// Common parameters shared by all policy types.
#[derive(Debug, Clone)]
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
