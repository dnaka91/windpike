// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
