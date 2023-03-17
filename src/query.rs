use std::fmt;

use rand::Rng;
use tokio::sync::mpsc;

use crate::{commands::CommandError, Bins, Record};

/// Query statement parameters.
pub struct Statement {
    /// Namespace
    pub namespace: String,
    /// Set name
    pub set_name: String,
    /// Optional index name
    pub index_name: Option<String>,
    /// Optional list of bin names to return in query.
    pub bins: Bins,
}

impl Statement {
    /// Create a new query statement with the given namespace, set name and optional list of bin
    /// names.
    ///
    /// # Examples
    ///
    /// Create a new statement to query the namespace "foo" and set "bar" and return the "name" and
    /// "age" bins for each matching record.
    ///
    /// ```rust
    /// use aerospike::*;
    ///
    /// let stmt = Statement::new("foo", "bar", Bins::from(["name", "age"]));
    /// ```
    #[must_use]
    pub fn new(namespace: &str, set_name: &str, bins: Bins) -> Self {
        Self {
            namespace: namespace.to_owned(),
            set_name: set_name.to_owned(),
            bins,
            index_name: None,
        }
    }
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

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
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

impl fmt::Display for CollectionIndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(match self {
            Self::List => "LIST",
            Self::MapKeys => "MAPKEYS",
            Self::MapValues => "MAPVALUES",
        })
    }
}
