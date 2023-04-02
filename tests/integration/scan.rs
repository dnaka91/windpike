use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use aerospike::{
    self, as_bin,
    policy::{ScanPolicy, WritePolicy},
    query::Recordset,
    Bins, Client, Key,
};
use tokio::sync::Mutex;

use crate::common::{self, NAMESPACE};

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = Key::new(NAMESPACE, set_name.clone(), i);
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
    let set_name = create_test_set(&client, EXPECTED).await;

    let spolicy = ScanPolicy::default();
    let mut rs = client
        .scan(&spolicy, NAMESPACE, &set_name, Bins::All)
        .await
        .unwrap();

    let count = count_results(&mut rs).await;
    assert_eq!(count, EXPECTED);

    client.close();
}

#[tokio::test]
async fn scan_multi_consumer() {
    common::init_logger();

    let client = common::client().await;
    let set_name = create_test_set(&client, EXPECTED).await;

    let spolicy = ScanPolicy {
        record_queue_size: 4096,
        ..ScanPolicy::default()
    };
    let rs = client
        .scan(&spolicy, NAMESPACE, &set_name, Bins::All)
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

    client.close();
}

async fn count_results(rs: &mut Recordset) -> usize {
    let mut count = 0;
    while let Some(Ok(_)) = rs.next().await {
        count += 1;
    }

    count
}
