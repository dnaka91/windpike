use std::{
    fmt::{self, Display},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    cluster::Cluster,
    errors::{Error, Result},
};

/// Current status of an indexing task, as reported by the [`CreateIndex::query_status`] and
/// [`CreateIndex::wait_till_complete`] methods.
#[derive(Clone, Copy, Debug)]
pub enum Status {
    /// Task for the index operation not found.
    NotFound,
    /// Operation is still in progress.
    InProgress,
    /// Successfully completed indexing operation.
    Complete,
}

/// Struct for querying index creation status
#[derive(Clone, Debug)]
pub struct CreateIndex {
    cluster: Arc<Cluster>,
    namespace: String,
    index_name: String,
}

impl CreateIndex {
    pub(crate) fn new(cluster: Arc<Cluster>, namespace: String, index_name: String) -> Self {
        Self {
            cluster,
            namespace,
            index_name,
        }
    }

    fn build_command(namespace: &str, index_name: &str) -> String {
        format!("sindex/{namespace}/{index_name}")
    }

    /// Parse the raw string response, trying to extract the status of an indexing operation.
    ///
    /// Operations can immediately complete, or take time, depending on the size of data they're
    /// built upon. The progress is indicated by the `load_pct` value, which is encoded in a list
    /// of key-values. Each list item is separated by `;` and the key and value are separated by
    /// `=` each.
    fn parse_response(response: &str) -> Result<Status> {
        const ERROR_NOT_FOUND: &str = "FAIL:201";
        const ERROR_NOT_READABLE: &str = "FAIL:203";

        let load_pct = response
            .split(';')
            .filter_map(|pair| pair.split_once('='))
            .find_map(|(key, value)| (key == "load_pct").then_some(value));

        if let Some(percentage) = load_pct {
            match percentage.parse::<i32>() {
                Ok(100) => Ok(Status::Complete),
                Ok(i) if (0..100).contains(&i) => Ok(Status::InProgress),
                Ok(_) | Err(_) => Err(Error::BadResponse(format!(
                    "invalid load percentage `{percentage}`"
                ))),
            }
        } else if response.contains(ERROR_NOT_FOUND) || response.contains(ERROR_NOT_READABLE) {
            Ok(Status::NotFound)
        } else {
            Err(Error::BadResponse(format!(
                "no load percentage found, but no error reported either (response: {response})"
            )))
        }
    }

    pub async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_owned()));
        }

        let command = Self::build_command(&self.namespace, &self.index_name);

        for node in nodes {
            let response = node
                .info(&[&command])
                .await?
                .get(&command)
                .map(|r| Self::parse_response(r));

            match response {
                Some(Ok(Status::Complete)) => {}
                Some(other) => return other,
                None => return Ok(Status::NotFound),
            }
        }

        Ok(Status::Complete)
    }

    pub async fn wait_till_complete(&self, timeout: Option<Duration>) -> Result<()> {
        const POLL_INTERVAL: Duration = Duration::from_secs(1);

        let now = Instant::now();
        let timeout_reached = |deadline| now.elapsed() + POLL_INTERVAL > deadline;

        loop {
            // Sleep first to give task a chance to complete
            tokio::time::sleep(POLL_INTERVAL).await;

            match self.query_status().await {
                Ok(Status::NotFound) => {
                    return Err(Error::BadResponse("task status not found".to_owned()))
                }
                Ok(Status::InProgress) => {} // do nothing and wait
                Ok(Status::Complete) => return Ok(()),
                Err(e) => return Err(e),
            }

            if timeout.map_or(false, timeout_reached) {
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
