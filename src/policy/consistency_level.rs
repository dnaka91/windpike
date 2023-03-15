/// `ConsistencyLevel` indicates how replicas should be consulted in a read
/// operation to provide the desired consistency guarantee.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsistencyLevel {
    /// ConsistencyOne indicates only a single replica should be consulted in
    /// the read operation.
    #[default]
    ConsistencyOne = 0,
    /// ConsistencyAll indicates that all replicas should be consulted in
    /// the read operation.
    ConsistencyAll = 1,
}
