use rand::Rng;
use tokio::sync::mpsc;

use crate::{commands::CommandError, errors::Result, Record};

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
