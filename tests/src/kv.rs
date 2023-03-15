use aerospike::{as_bin, as_list, as_map, operations, Bins, Key, ReadPolicy, Value, WritePolicy};

use crate::common;

#[tokio::test]
async fn connect() {
    common::init_logger();

    let client = common::client().await;
    let namespace = common::namespace().to_owned();
    let set_name = common::rand_str(10);
    let policy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = Key::new(namespace, set_name, -1);

    client.delete(&wpolicy, &key).await.unwrap();

    let bins = [
        as_bin!("bin999", "test string"),
        as_bin!("bin vec![int]", as_list![1u32, 2u32, 3u32]),
        as_bin!("bin vec![u8]", Value::from(vec![1u8, 2u8, 3u8])),
        as_bin!("bin map", as_map!(1 => 1, 2 => 2, 3 => "hi!")),
        as_bin!("bin f64", 1.64f64),
        as_bin!("bin Nil", None), // Writing None erases the bin!
        as_bin!(
            "bin Geo",
            Value::GeoJson(
                r#"{ "type": "Point", "coordinates": [17.119381, 19.45612] }"#.to_owned(),
            )
        ),
        as_bin!("bin-name-len-15", "max. bin name length is 15 chars"),
    ];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let record = client.get(&policy, &key, Bins::All).await.unwrap();
    let bins = record.bins;
    assert_eq!(bins.len(), 7);
    assert_eq!(bins.get("bin999"), Some(&Value::from("test string")));
    assert_eq!(bins.get("bin vec![int]"), Some(&as_list![1u32, 2u32, 3u32]));
    assert_eq!(
        bins.get("bin vec![u8]"),
        Some(&Value::from(vec![1u8, 2u8, 3u8]))
    );
    assert_eq!(
        bins.get("bin map"),
        Some(&as_map!(1 => 1, 2 => 2, 3 => "hi!"))
    );
    assert_eq!(bins.get("bin f64"), Some(&Value::from(1.64f64)));
    assert_eq!(
        bins.get("bin Geo"),
        Some(&Value::GeoJson(
            r#"{ "type": "Point", "coordinates": [17.119381, 19.45612] }"#.to_owned()
        ))
    );
    assert_eq!(
        bins.get("bin-name-len-15"),
        Some(&Value::from("max. bin name length is 15 chars"))
    );

    client.touch(&wpolicy, &key).await.unwrap();

    let bins = Bins::from(["bin999", "bin f64"]);
    let record = client.get(&policy, &key, bins).await.unwrap();
    assert_eq!(record.bins.len(), 2);

    let record = client.get(&policy, &key, Bins::None).await.unwrap();
    assert_eq!(record.bins.len(), 0);

    let exists = client.exists(&wpolicy, &key).await.unwrap();
    assert!(exists);

    let bin = as_bin!("bin999", "test string");
    let ops = &vec![operations::put(&bin), operations::get()];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(existed);

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(!existed);

    client.close().await.unwrap();
}
