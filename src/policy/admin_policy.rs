use std::time::Duration;

/// Policy attributes used for user administration commands.
#[derive(Debug, Clone, Copy)]
pub struct AdminPolicy {
    /// Total transaction timeout for both client and server.
    pub timeout: Duration,
}
