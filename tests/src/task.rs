// Copyright 2015-2020 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use aerospike::{
    as_bin,
    task::{Status, Task},
    IndexType, Key, WritePolicy,
};

use crate::common;

// If creating index is successful, querying IndexTask will return Status::Complete
#[tokio::test]
async fn index_task_test() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let bin_name = common::rand_str(10);
    let index_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..2_i64 {
        let key = Key::new(namespace, &set_name, i).unwrap();
        let wbin = as_bin!(&bin_name, i);
        let bins = vec![wbin];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    let index_task = client
        .create_index(
            namespace,
            &set_name,
            &bin_name,
            &index_name,
            IndexType::Numeric,
        )
        .await
        .unwrap();

    assert!(matches!(
        index_task.wait_till_complete(None).await,
        Ok(Status::Complete)
    ));

    client.close().await.unwrap();
}
