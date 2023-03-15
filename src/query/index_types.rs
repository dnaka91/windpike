use std::fmt;

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
