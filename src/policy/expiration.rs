use std::u32;

const NAMESPACE_DEFAULT: u32 = 0;
const DONT_EXPIRE: u32 = u32::MAX; // -1 as i32
const DONT_UPDATE: u32 = u32::MAX - 1; // -2 as i32

/// Record expiration, also known as time-to-live (TTL).
#[derive(Clone, Copy, Debug, Default)]
pub enum Expiration {
    /// Set the record to expire X seconds from now
    Seconds(u32),
    /// Set the record's expiry time using the default time-to-live (TTL) value for the namespace
    #[default]
    NamespaceDefault,
    /// Set the record to never expire. Requires Aerospike 2 server version 2.7.2 or later or
    /// Aerospike 3 server version 3.1.4 or later. Do not use with older servers.
    Never,
    /// Do not change the record's expiry time when updating the record; requires Aerospike server
    /// version 3.10.1 or later.
    DontUpdate,
}

impl From<Expiration> for u32 {
    fn from(exp: Expiration) -> Self {
        match exp {
            Expiration::Seconds(secs) => secs,
            Expiration::NamespaceDefault => NAMESPACE_DEFAULT,
            Expiration::Never => DONT_EXPIRE,
            Expiration::DontUpdate => DONT_UPDATE,
        }
    }
}
