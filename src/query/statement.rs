// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

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
