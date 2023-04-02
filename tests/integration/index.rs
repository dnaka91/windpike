use std::time::Duration;

use aerospike::{
    as_bin,
    index::{IndexType, Status},
    policy::WritePolicy,
    Client, Key,
};

use crate::common::{self, NAMESPACE};

const EXPECTED: usize = 100;

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
#[should_panic(expected = "IndexAlreadyExists")]
async fn recreate_index() {
    common::init_logger();

    let client = common::client().await;
    let set = create_test_set(&client, EXPECTED).await;
    let bin = "bin";
    let index = format!("{NAMESPACE}_{}_{}", set, bin);

    let _ = client.drop_index(NAMESPACE, &set, &index).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let task = client
        .create_index(NAMESPACE, &set, bin, &index, IndexType::Numeric)
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    let task = client
        .create_index(NAMESPACE, &set, bin, &index, IndexType::Numeric)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    client.close();
}

// If creating index is successful, querying IndexTask will return Status::Complete
#[tokio::test]
async fn index_task_test() {
    let client = common::client().await;
    let set_name = common::rand_str(10);
    let bin_name = common::rand_str(10);
    let index_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..2_i64 {
        let key = Key::new(NAMESPACE, set_name.clone(), i);
        let wbin = as_bin!(&bin_name, i);
        let bins = vec![wbin];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    let index_task = client
        .create_index(
            NAMESPACE,
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

    client.close();
}
