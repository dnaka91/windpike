use tokio::time::{Duration, Instant};

use crate::errors::{Error, Result};

/// Status of task
#[derive(Clone, Copy, Debug)]
pub enum Status {
    /// long running task not found
    NotFound,
    /// long running task in progress
    InProgress,
    /// long running task completed
    Complete,
}

static POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Base task interface
#[async_trait::async_trait]
pub trait Task {
    /// interface for query specific task status
    async fn query_status(&self) -> Result<Status>;

    /// Wait until query status is complete, an error occurs, or the timeout has elapsed.
    async fn wait_till_complete(&self, timeout: Option<Duration>) -> Result<Status> {
        let now = Instant::now();
        let timeout_elapsed = |deadline| now.elapsed() + POLL_INTERVAL > deadline;

        loop {
            // Sleep first to give task a chance to complete and help avoid case where task hasn't
            // started yet.
            tokio::time::sleep(POLL_INTERVAL).await;

            match self.query_status().await {
                Ok(Status::NotFound) => {
                    return Err(Error::BadResponse("task status not found".to_string()))
                }
                Ok(Status::InProgress) => {} // do nothing and wait
                error_or_complete => return error_or_complete,
            }

            if timeout.map_or(false, timeout_elapsed) {
                return Err(Error::Timeout("Task timeout reached".to_string()));
            }
        }
    }
}
