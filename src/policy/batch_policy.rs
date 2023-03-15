use crate::policy::{BasePolicy, Concurrency};

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
