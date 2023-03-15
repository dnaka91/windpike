/// `RecordExistsAction` determines how to handle record writes based on record generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum RecordExistsAction {
    /// Update means: Create or update record.
    /// Merge write command bins with existing bins.
    #[default]
    Update = 0,
    /// UpdateOnly means: Update record only. Fail if record does not exist.
    /// Merge write command bins with existing bins.
    UpdateOnly,
    /// Replace means: Create or replace record.
    /// Delete existing bins not referenced by write command bins.
    /// Supported by Aerospike 2 server versions >= 2.7.5 and
    /// Aerospike 3 server versions >= 3.1.6.
    Replace,
    /// ReplaceOnly means: Replace record only. Fail if record does not exist.
    /// Delete existing bins not referenced by write command bins.
    /// Supported by Aerospike 2 server versions >= 2.7.5 and
    /// Aerospike 3 server versions >= 3.1.6.
    ReplaceOnly,
    /// CreateOnly means: Create only. Fail if record exists.
    CreateOnly,
}
