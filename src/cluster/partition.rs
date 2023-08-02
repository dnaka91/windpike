use crate::{cluster::node, Key};

// Validates a Database server node
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Partition<'a> {
    pub namespace: &'a str,
    pub id: u32,
}

impl<'a> From<&'a Key> for Partition<'a> {
    fn from(value: &'a Key) -> Self {
        Self {
            namespace: &value.namespace,
            id: {
                let mut buf = [0; 4];
                buf.copy_from_slice(&value.digest()[0..4]);
                u32::from_le_bytes(buf) % node::PARTITIONS
            },
        }
    }
}
