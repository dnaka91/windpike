use windpike::{index::IndexType, policies::WritePolicy, Bin, Client, Key};

use crate::common::{self, NAMESPACE};

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();

    for i in 0..no_records as i64 {
        let key = Key::new(NAMESPACE, set_name.clone(), i);
        let wbin = Bin::new("bin", i);
        let bins = vec![wbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[tokio::test]
async fn recreate_index() {
    let client = common::client().await;
    let set = create_test_set(&client, EXPECTED).await;
    let bin = "bin";
    let index = format!("{NAMESPACE}_{set}_{bin}");

    let _ = client.drop_index(NAMESPACE, &set, &index).await;

    client
        .create_index(NAMESPACE, &set, bin, &index, IndexType::Numeric)
        .await
        .expect("failed to create index")
        .wait_till_complete(None)
        .await
        .unwrap();

    client
        .create_index(NAMESPACE, &set, bin, &index, IndexType::Numeric)
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();

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
        let wbin = Bin::new(&bin_name, i);
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

    assert!(index_task.wait_till_complete(None).await.is_ok());

    client.close();
}
