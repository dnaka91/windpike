/// `GenerationPolicy` determines how to handle record writes based on record generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum GenerationPolicy {
    /// None means: Do not use record generation to restrict writes.
    #[default]
    None = 0,
    /// ExpectGenEqual means: Update/delete record if expected generation is equal to server
    /// generation. Otherwise, fail.
    ExpectGenEqual,
    /// ExpectGenGreater means: Update/delete record if expected generation greater than the server
    /// generation. Otherwise, fail. This is useful for restore after backup.
    ExpectGenGreater,
}
