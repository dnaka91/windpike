use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use tokio::sync::mpsc;

use crate::{commands::CommandError, Key, Value};

/// Container object for a database record.
#[derive(Clone, Debug)]
pub struct Record {
    /// Record key. When reading a record from the database, the key is not set in the returned
    /// Record struct.
    pub key: Option<Key>,

    /// Map of named record bins.
    pub bins: HashMap<String, Value>,

    /// Record modification count.
    pub generation: u32,

    /// Date record will expire, in seconds from Jan 01 2010, 00:00:00 UTC.
    expiration: u32,
}

impl Record {
    /// Construct a new Record. For internal use only.
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

    /// Returns the remaining time-to-live (TTL, a.k.a. expiration time) for the record or `None`
    /// if the record never expires.
    #[must_use]
    pub fn time_to_live(&self) -> Option<Duration> {
        (self.expiration > 0).then(|| {
            let expiration = citrusleaf_epoch() + Duration::new(u64::from(self.expiration), 0);
            expiration
                .duration_since(SystemTime::now())
                .ok()
                .unwrap_or(Duration::new(1, 0))
        })
    }
}

/// Aerospike's own epoch time, which is `Fri Jan  1 00:00:00 UTC 2010`.
#[inline]
fn citrusleaf_epoch() -> SystemTime {
    UNIX_EPOCH + Duration::new(1_262_304_000, 0)
}

/// Virtual collection of records retrieved through queries and scans. During a query/scan,
/// multiple threads will retrieve records from the server nodes and put these records on an
/// internal queue managed by the recordset. The single user thread consumes these records from the
/// queue.
pub struct Recordset {
    queue: mpsc::Receiver<Result<Record, CommandError>>,
    task_id: u64,
}

impl Recordset {
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
        let expiration = SystemTime::now() + Duration::new(1000, 0);
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
        assert_eq!(record.time_to_live(), Some(Duration::new(1u64, 0)));
    }

    #[test]
    fn ttl_never_expires() {
        let record = Record::new(None, HashMap::new(), 0, 0);
        assert_eq!(record.time_to_live(), None);
    }
}
