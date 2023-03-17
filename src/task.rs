//! Types and methods used for long running status queries.

use std::sync::Arc;

use tokio::time::{Duration, Instant};

use crate::{
    cluster::Cluster,
    errors::{Error, Result},
};

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

/// Struct for querying index creation status
#[derive(Clone, Debug)]
pub struct IndexTask {
    cluster: Arc<Cluster>,
    namespace: String,
    index_name: String,
}

static SUCCESS_PATTERN: &str = "load_pct=";
static FAIL_PATTERN_201: &str = "FAIL:201";
static FAIL_PATTERN_203: &str = "FAIL:203";
static DELMITER: &str = ";";

impl IndexTask {
    /// Initializes `IndexTask` from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, namespace: String, index_name: String) -> Self {
        Self {
            cluster,
            namespace,
            index_name,
        }
    }

    fn build_command(namespace: &str, index_name: &str) -> String {
        format!("sindex/{namespace}/{index_name}")
    }

    fn parse_response(response: &str) -> Result<Status> {
        match response.find(SUCCESS_PATTERN) {
            None => {
                if response.contains(FAIL_PATTERN_201) || response.contains(FAIL_PATTERN_203) {
                    Ok(Status::NotFound)
                } else {
                    Err(Error::BadResponse(format!(
                        "Code 201 and 203 missing. Response: {response}"
                    )))
                }
            }
            Some(pattern_index) => {
                let percent_begin = pattern_index + SUCCESS_PATTERN.len();

                let percent_end = match response[percent_begin..].find(DELMITER) {
                    None => {
                        return Err(Error::BadResponse(format!(
                            "delimiter missing in response. Response: {response}"
                        )))
                    }
                    Some(percent_end) => percent_end,
                };
                let percent_str = &response[percent_begin..percent_begin + percent_end];
                match percent_str.parse::<isize>() {
                    Ok(100) => Ok(Status::Complete),
                    Ok(_) => Ok(Status::InProgress),
                    Err(_) => Err(Error::BadResponse(
                        "Unexpected load_pct value from server".to_string(),
                    )),
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Task for IndexTask {
    /// Query the status of index creation across all nodes
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_string()));
        }

        for node in &nodes {
            let command = &Self::build_command(&self.namespace, &self.index_name);
            let response = node.info(&[&command[..]]).await?;

            if !response.contains_key(command) {
                return Ok(Status::NotFound);
            }

            match Self::parse_response(&response[command]) {
                Ok(Status::Complete) => {}
                in_progress_or_error => return in_progress_or_error,
            }
        }
        Ok(Status::Complete)
    }
}
