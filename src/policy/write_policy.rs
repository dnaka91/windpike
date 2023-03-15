use crate::{policy::BasePolicy, CommitLevel, Expiration, GenerationPolicy, RecordExistsAction};

/// `WritePolicy` encapsulates parameters for all write operations.
#[derive(Debug, Clone)]
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

impl Default for WritePolicy {
    fn default() -> Self {
        Self {
            base_policy: BasePolicy::default(),
            record_exists_action: RecordExistsAction::default(),
            generation_policy: GenerationPolicy::default(),
            commit_level: CommitLevel::default(),
            generation: 0,
            expiration: Expiration::default(),
            respond_per_each_op: false,
            durable_delete: false,
        }
    }
}

impl AsRef<BasePolicy> for WritePolicy {
    fn as_ref(&self) -> &BasePolicy {
        &self.base_policy
    }
}
