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
    let namespace = common::namespace().to_owned();
    let set_name = common::rand_str(10);
    let bin_name = common::rand_str(10);
    let index_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..2_i64 {
        let key = Key::new(namespace.clone(), set_name.clone(), i);
        let wbin = as_bin!(&bin_name, i);
        let bins = vec![wbin];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    let index_task = client
        .create_index(
            &namespace,
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
