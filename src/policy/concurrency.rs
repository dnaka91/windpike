/// Specifies whether a command, that needs to be executed on multiple cluster nodes, should be
/// executed sequentially, one node at a time, or in parallel on multiple nodes using the client's
/// thread pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Concurrency {
    /// Issue commands sequentially. This mode has a performance advantage for small to
    /// medium sized batch sizes because requests can be issued in the main transaction thread.
    /// This is the default.
    Sequential,
    /// Issue all commands in parallel threads. This mode has a performance advantage for
    /// extremely large batch sizes because each node can process the request immediately. The
    /// downside is extra threads will need to be created (or takedn from a thread pool).
    Parallel,
    /// Issue up to N commands in parallel threads. When a request completes, a new request
    /// will be issued until all threads are complete. This mode prevents too many parallel threads
    /// being created for large cluster implementations. The downside is extra threads will still
    /// need to be created (or taken from a thread pool).
    ///
    /// E.g. if there are 16 nodes/namespace combinations requested and concurrency is set to
    /// `MaxThreads(8)`, then batch requests will be made for 8 node/namespace combinations in
    /// parallel threads. When a request completes, a new request will be issued until all 16
    /// requests are complete.
    MaxThreads(usize),
}
