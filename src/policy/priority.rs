/// Priority of operations on database server.
#[derive(Clone, Copy, Debug, Default)]
pub enum Priority {
    /// Default determines that the server defines the priority.
    #[default]
    Default = 0,
    /// Low determines that the server should run the operation in a background thread.
    Low,
    /// Medium determines that the server should run the operation at medium priority.
    Medium,
    /// High determines that the server should run the operation at the highest priority.
    High,
}
