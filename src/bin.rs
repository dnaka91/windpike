use std::{borrow::Cow, convert::From};

use crate::value::Value;

/// Container object for a record bin, comprising a name and a value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bin<'a> {
    /// Bin name
    pub name: &'a str,
    /// Bin value
    pub value: Value,
}

impl<'a> Bin<'a> {
    /// Construct a new bin given a name and a value.
    #[inline]
    #[must_use]
    pub fn new(name: &'a str, value: impl Into<Value>) -> Self {
        Bin {
            name,
            value: value.into(),
        }
    }
}

impl<'a, T> From<(&'a str, T)> for Bin<'a>
where
    T: Into<Value>,
{
    fn from((name, value): (&'a str, T)) -> Self {
        Bin::new(name, value)
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
    Some(Vec<Cow<'static, str>>),
}

impl<I, T> From<I> for Bins
where
    I: IntoIterator<Item = T>,
    T: Into<Cow<'static, str>>,
{
    fn from(value: I) -> Self {
        Self::Some(value.into_iter().map(T::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{Bins, Cow, From};

    #[test]
    fn into_bins() {
        let bin_names = vec![
            Cow::Owned("a".into()),
            Cow::Owned("b".into()),
            Cow::Owned("c".into()),
        ];
        let expected = Bins::Some(bin_names);

        assert_eq!(expected, Bins::from(["a", "b", "c"]));
    }
}
