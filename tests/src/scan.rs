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

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use aerospike::*;
use tokio::sync::Mutex;

use crate::common;

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace().to_owned();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = Key::new(namespace.to_owned(), set_name.clone(), i);
        let wbin = as_bin!("bin", i);
        let bins = vec![wbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[tokio::test]
async fn scan_single_consumer() {
    common::init_logger();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let spolicy = ScanPolicy::default();
    let mut rs = client
        .scan(&spolicy, namespace, &set_name, Bins::All)
        .await
        .unwrap();

    let count = count_results(&mut rs).await;
    assert_eq!(count, EXPECTED);

    client.close().await.unwrap();
}

#[tokio::test]
async fn scan_multi_consumer() {
    common::init_logger();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let spolicy = ScanPolicy {
        record_queue_size: 4096,
        ..ScanPolicy::default()
    };
    let rs = client
        .scan(&spolicy, namespace, &set_name, Bins::All)
        .await
        .unwrap();
    let rs = Arc::new(Mutex::new(rs));

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(tokio::spawn(async move {
            let ok = count_results(&mut *rs.lock().await).await;
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.await.expect("Cannot join thread");
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);

    client.close().await.unwrap();
}

#[tokio::test]
async fn scan_node() {
    common::init_logger();

    let client = Arc::new(common::client().await);
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for node in client.nodes().await {
        let client = client.clone();
        let count = count.clone();
        let set_name = set_name.clone();
        threads.push(tokio::spawn(async move {
            let spolicy = ScanPolicy::default();
            let mut rs = client
                .scan_node(&spolicy, node, namespace, &set_name, Bins::All)
                .await
                .unwrap();
            let ok = count_results(&mut rs).await;
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.await.unwrap();
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);

    client.close().await.unwrap();
}

async fn count_results(rs: &mut Recordset) -> usize {
    let mut count = 0;
    while let Some(Ok(_)) = rs.next().await {
        count += 1;
    }

    count
}
