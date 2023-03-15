use std::time::Duration;

use crate::{policy::BasePolicy, ConsistencyLevel, Priority};

/// `ReadPolicy` excapsulates parameters for transaction policy attributes
/// used in all database operation calls.
pub type ReadPolicy = BasePolicy;

impl Default for ReadPolicy {
    fn default() -> Self {
        ReadPolicy {
            priority: Priority::default(),
            timeout: Some(Duration::new(30, 0)),
            max_retries: Some(2),
            sleep_between_retries: Some(Duration::new(0, 500_000_000)),
            consistency_level: ConsistencyLevel::default(),
            send_key: false,
        }
    }
}
