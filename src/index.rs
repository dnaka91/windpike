use std::{
    fmt::{self, Display},
    sync::Arc,
    time::{Duration, Instant},
};

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
                        "code 201 and 203 missing. Response: {response}"
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
                        "unexpected load_pct value from server".to_owned(),
                    )),
                }
            }
        }
    }

    /// Query the status of index creation across all nodes
    pub async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_owned()));
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

    /// Wait until query status is complete, an error occurs, or the timeout has elapsed.
    pub async fn wait_till_complete(&self, timeout: Option<Duration>) -> Result<Status> {
        const POLL_INTERVAL: Duration = Duration::from_secs(1);

        let now = Instant::now();
        let timeout_elapsed = |deadline| now.elapsed() + POLL_INTERVAL > deadline;

        loop {
            // Sleep first to give task a chance to complete and help avoid case where task hasn't
            // started yet.
            tokio::time::sleep(POLL_INTERVAL).await;

            match self.query_status().await {
                Ok(Status::NotFound) => {
                    return Err(Error::BadResponse("task status not found".to_owned()))
                }
                Ok(Status::InProgress) => {} // do nothing and wait
                error_or_complete => return error_or_complete,
            }

            if timeout.map_or(false, timeout_elapsed) {
                return Err(Error::Timeout("task timeout reached".to_owned()));
            }
        }
    }
}

/// Underlying data type of secondary index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexType {
    /// Numeric index.
    Numeric,
    /// String index.
    String,
    /// 2-dimensional spherical geospatial index.
    Geo2DSphere,
}

impl Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Numeric => "NUMERIC",
            Self::String => "STRING",
            Self::Geo2DSphere => "GEO2DSPHERE",
        })
    }
}

/// Secondary index collection type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionIndexType {
    /// Index list elements.
    List,
    /// Index map keys.
    MapKeys,
    /// Index map values.
    MapValues,
}

impl Display for CollectionIndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::List => "LIST",
            Self::MapKeys => "MAPKEYS",
            Self::MapValues => "MAPVALUES",
        })
    }
}
