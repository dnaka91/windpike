/// `CommitLevel` determines how to handle record writes based on record generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CommitLevel {
    /// CommitAll indicates the server should wait until successfully committing master and all
    /// replicas.
    #[default]
    CommitAll = 0,
    /// CommitMaster indicates the server should wait until successfully committing master only.
    CommitMaster,
}
