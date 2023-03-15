use crate::{cluster::node, Key};

// Validates a Database server node
#[derive(Debug, Clone)]
pub struct Partition<'a> {
    pub namespace: &'a str,
    pub partition_id: usize,
}

impl<'a> Partition<'a> {
    #[must_use]
    pub const fn new(namespace: &'a str, partition_id: usize) -> Self {
        Partition {
            namespace,
            partition_id,
        }
    }

    #[must_use]
    pub fn new_by_key(key: &'a Key) -> Self {
        Partition {
            namespace: &key.namespace,

            // CAN'T USE MOD directly - mod will give negative numbers.
            // First AND makes positive and negative correctly, then mod.
            // For any x, y : x % 2^y = x & (2^y - 1); the second method is twice as fast
            partition_id: {
                let mut buf = [0; 4];
                buf.copy_from_slice(&key.digest()[0..4]);

                u32::from_le_bytes(buf) as usize & (node::PARTITIONS - 1)
            },
        }
    }
}

impl<'a> PartialEq for Partition<'a> {
    fn eq(&self, other: &Partition<'_>) -> bool {
        self.namespace == other.namespace && self.partition_id == other.partition_id
    }
}
