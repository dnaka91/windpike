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

use std::fmt;

/// Underlying data type of secondary index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexType {
    /// Numeric index.
    Numeric,
    /// String index.
    String,
    /// 2-dimensional spherical geospatial index.
    Geo2DSphere,
}

/// Secondary index collection type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionIndexType {
    /// Normal, scalar index.
    Default = 0,
    /// Index list elements.
    List,
    /// Index map keys.
    MapKeys,
    /// Index map values.
    MapValues,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            Self::Numeric => "NUMERIC".fmt(f),
            Self::String => "STRING".fmt(f),
            Self::Geo2DSphere => "GEO2DSPHERE".fmt(f),
        }
    }
}

impl fmt::Display for CollectionIndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            Self::Default => panic!("Unknown IndexCollectionType value `Default`"),
            Self::List => "LIST".fmt(f),
            Self::MapKeys => "MAPKEYS".fmt(f),
            Self::MapValues => "MAPVALUES".fmt(f),
        }
    }
}
