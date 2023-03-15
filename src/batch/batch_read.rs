use crate::{Bins, Key, Record};

/// Key and bin names used in batch read commands where variable bins are needed for each key.
#[derive(Clone, Debug)]
pub struct BatchRead {
    /// Key.
    pub key: Key,

    /// Bins to retrieve for this key.
    pub bins: Bins,

    /// Will contain the record after the batch read operation.
    pub record: Option<Record>,
}

impl BatchRead {
    /// Create a new `BatchRead` instance for the given key and bin selector.
    #[must_use]
    pub const fn new(key: Key, bins: Bins) -> Self {
        Self {
            key,
            bins,
            record: None,
        }
    }

    #[must_use]
    pub(crate) fn match_header(&self, other: &Self, match_set: bool) -> bool {
        let key = &self.key;
        let other_key = &other.key;
        (key.namespace == other_key.namespace)
            && (match_set && (key.set_name == other_key.set_name))
            && (self.bins == other.bins)
    }
}
