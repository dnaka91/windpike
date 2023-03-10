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

use std::convert::From;

use crate::value::Value;

/// Container object for a record bin, comprising a name and a value.
#[derive(Clone)]
pub struct Bin<'a> {
    /// Bin name
    pub name: &'a str,

    /// Bin value
    pub value: Value,
}

impl<'a> Bin<'a> {
    /// Construct a new bin given a name and a value.
    #[must_use]
    pub const fn new(name: &'a str, val: Value) -> Self {
        Bin { name, value: val }
    }
}

impl<'a> AsRef<Bin<'a>> for Bin<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Specify which, if any, bins to return in read operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Bins {
    /// Read all bins.
    All,
    /// Read record header (generation, expiration) only.
    None,
    /// Read specified bin names only.
    Some(Vec<String>),
}

impl<'a> From<&'a [&'a str]> for Bins {
    fn from(bins: &'a [&'a str]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

impl<'a> From<[&'a str; 1]> for Bins {
    fn from(bins: [&'a str; 1]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

impl<'a> From<[&'a str; 2]> for Bins {
    fn from(bins: [&'a str; 2]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

impl<'a> From<[&'a str; 3]> for Bins {
    fn from(bins: [&'a str; 3]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

impl<'a> From<[&'a str; 4]> for Bins {
    fn from(bins: [&'a str; 4]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

impl<'a> From<[&'a str; 5]> for Bins {
    fn from(bins: [&'a str; 5]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

impl<'a> From<[&'a str; 6]> for Bins {
    fn from(bins: [&'a str; 6]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

#[cfg(test)]
mod tests {
    use super::{Bins, From};

    #[test]
    fn into_bins() {
        let bin_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let expected = Bins::Some(bin_names);

        assert_eq!(expected, Bins::from(["a", "b", "c"]));
    }
}
