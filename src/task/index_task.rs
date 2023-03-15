use std::sync::Arc;

use crate::{
    cluster::Cluster,
    errors::{Error, Result},
    task::{Status, Task},
};

/// Struct for querying index creation status
#[derive(Debug, Clone)]
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
