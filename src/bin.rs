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

impl<'a, const N: usize> From<[&'a str; N]> for Bins {
    fn from(bins: [&'a str; N]) -> Self {
        let bins = bins.iter().copied().map(String::from).collect();
        Self::Some(bins)
    }
}

#[cfg(test)]
mod tests {
    use super::{Bins, From};

    #[test]
    fn into_bins() {
        let bin_names = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let expected = Bins::Some(bin_names);

        assert_eq!(expected, Bins::from(["a", "b", "c"]));
    }
}
