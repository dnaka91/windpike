use std::time::Duration;

/// Policy attributes used for user administration commands.
#[derive(Clone, Copy, Debug)]
pub struct AdminPolicy {
    /// Total transaction timeout for both client and server.
    pub timeout: Duration,
}
