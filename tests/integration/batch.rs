use windpike::{
    policy::{BatchPolicy, Concurrency, WritePolicy},
    BatchRead, Bin, Bins, Key,
};

use crate::common::{self, NAMESPACE};

#[tokio::test]
async fn batch_get() {
    let client = common::client().await;
    let set_name = common::rand_str(10);
    let bpolicy = BatchPolicy {
        concurrency: Concurrency::Parallel(0),
        ..BatchPolicy::default()
    };
    let wpolicy = WritePolicy::default();

    let bin1 = Bin::new("a", "a value");
    let bin2 = Bin::new("b", "another value");
    let bin3 = Bin::new("c", 42);
    let bins = [bin1, bin2, bin3];
    let key1 = Key::new(NAMESPACE, set_name.clone(), 1);
    client.put(&wpolicy, &key1, &bins).await.unwrap();

    let key2 = Key::new(NAMESPACE, set_name.clone(), 2);
    client.put(&wpolicy, &key2, &bins).await.unwrap();

    let key3 = Key::new(NAMESPACE, set_name.clone(), 3);
    client.put(&wpolicy, &key3, &bins).await.unwrap();

    let key4 = Key::new(NAMESPACE, set_name, -1);
    // key does not exist

    let selected = Bins::from(["a"]);
    let all = Bins::All;
    let none = Bins::None;

    let batch = vec![
        BatchRead::new(key1.clone(), selected),
        BatchRead::new(key2.clone(), all),
        BatchRead::new(key3.clone(), none.clone()),
        BatchRead::new(key4.clone(), none),
    ];
    let mut results = client.batch_get(&bpolicy, batch).await.unwrap();

    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 1);

    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 3);

    let result = results.remove(0);
    assert_eq!(result.key, key3);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 0);

    let result = results.remove(0);
    assert_eq!(result.key, key4);
    let record = result.record;
    assert!(record.is_none());
    client.close();
}
