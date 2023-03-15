use crate::Bins;

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
