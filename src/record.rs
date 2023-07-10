use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use rand::Rng;
use tokio::sync::mpsc;

use crate::{commands::CommandError, Key, Value};

/// A single, uniquely identifiable database entry.
#[derive(Clone, Debug)]
pub struct Record {
    /// Identifier for the record, by which it can be found in the database.
    ///
    /// When reading a record the key is usually not set, unless the
    /// [`BasePolicy::send_key`](crate::policies::BasePolicy::send_key) parameter is set to `true`.
    pub key: Option<Key>,
    /// Content of the record, which is categories in named bins. Each entry can contain simple
    /// values, lists, or even maps to create nested structures within.
    pub bins: HashMap<String, Value>,
    /// Modification count of the record. This counter is increased on the server side for each
    /// modification (including the initial creation).
    ///
    /// In write operations, the generation can be used to create conditional writes by utilizing
    /// the [`WritePolicy::generation_policy`](crate::policies::WritePolicy::generation_policy).
    pub generation: u32,
    /// Seconds from the _Citrusleaf epoch time_ (Jan 01 2010, 00:00:00 UTC) after which this
    /// record will expire.
    expiration: u32,
}

impl Record {
    /// Construct a new record.
    #[must_use]
    pub(crate) const fn new(
        key: Option<Key>,
        bins: HashMap<String, Value>,
        generation: u32,
        expiration: u32,
    ) -> Self {
        Self {
            key,
            bins,
            generation,
            expiration,
        }
    }

    /// Returns the remaining time-to-live (usually appreviated as TTL, or known as expiration time)
    /// for the record. If the record never expires, [`None`] is returned.
    #[must_use]
    pub fn time_to_live(&self) -> Option<Duration> {
        (self.expiration > 0).then(|| {
            let expiration = citrusleaf_epoch() + Duration::from_secs(u64::from(self.expiration));
            expiration
                .duration_since(SystemTime::now())
                .ok()
                .unwrap_or(Duration::from_secs(1))
        })
    }
}

/// Aerospike's own epoch time, which is `Fri Jan  1 00:00:00 UTC 2010`.
#[inline]
fn citrusleaf_epoch() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_262_304_000)
}

/// Set of records retrieved through queries and scans.
///
/// During a query/scan, multiple tasks will load the record from the cluster nodes and queue them
/// up for consumption through this set.
pub struct RecordSet {
    queue: mpsc::Receiver<Result<Record, CommandError>>,
    task_id: u64,
}

impl RecordSet {
    #[must_use]
    pub(crate) fn new(queue: mpsc::Receiver<Result<Record, CommandError>>) -> Self {
        Self {
            queue,
            task_id: rand::thread_rng().gen(),
        }
    }

    /// Returns the task ID for the scan/query.
    pub(crate) fn task_id(&self) -> u64 {
        self.task_id
    }

    /// Get the next record in the set, potentially wait for it if not available yet. Once [`None`]
    /// is returned, the set is considered resumed and subsequent calls will always return [`None`]
    /// immediately.
    pub async fn next(&mut self) -> Option<Result<Record, CommandError>> {
        self.queue.recv().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime},
    };

    use super::{citrusleaf_epoch, Record};

    #[test]
    fn ttl_expiration_future() {
        let expiration = SystemTime::now() + Duration::from_secs(1000);
        let secs_since_epoch = expiration
            .duration_since(citrusleaf_epoch())
            .unwrap()
            .as_secs();
        let record = Record::new(None, HashMap::new(), 0, secs_since_epoch as u32);
        let ttl = record.time_to_live();
        assert!(ttl.is_some());
        assert!(1000 - ttl.unwrap().as_secs() <= 1);
    }

    #[test]
    fn ttl_expiration_past() {
        let record = Record::new(None, HashMap::new(), 0, 0x0d00_d21c);
        assert_eq!(record.time_to_live(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn ttl_never_expires() {
        let record = Record::new(None, HashMap::new(), 0, 0);
        assert_eq!(record.time_to_live(), None);
    }
}
