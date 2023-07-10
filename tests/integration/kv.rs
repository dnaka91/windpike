use windpike::{
    operations::scalar,
    policies::{BasePolicy, WritePolicy},
    Bin, Bins, Key, Value,
};

use crate::common::{self, NAMESPACE};

#[tokio::test]
async fn connect() {
    let client = common::client().await;
    let policy = BasePolicy::default();
    let wpolicy = WritePolicy::default();
    let key = Key::new(NAMESPACE, common::rand_str(10), -1);

    client.delete(&wpolicy, &key).await.unwrap();

    let bins = [
        Bin::new("bin999", "test string"),
        Bin::new("bin vec![int]", windpike::list![1u32, 2u32, 3u32]),
        Bin::new("bin vec![u8]", Value::from(vec![1u8, 2u8, 3u8])),
        Bin::new("bin map", windpike::map!(1 => 1, 2 => 2, 3 => "hi!")),
        Bin::new("bin f64", 1.64f64),
        Bin::new("bin Nil", Value::Nil), // Writing None erases the bin!
        Bin::new(
            "bin Geo",
            Value::GeoJson(
                r#"{ "type": "Point", "coordinates": [17.119381, 19.45612] }"#.to_owned(),
            ),
        ),
        Bin::new("bin-name-len-15", "max. bin name length is 15 chars"),
    ];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let record = client.get(&policy, &key, Bins::All).await.unwrap();
    let bins = record.bins;
    assert_eq!(bins.len(), 7);
    assert_eq!(bins.get("bin999"), Some(&Value::from("test string")));
    assert_eq!(
        bins.get("bin vec![int]"),
        Some(&windpike::list![1u32, 2u32, 3u32])
    );
    assert_eq!(
        bins.get("bin vec![u8]"),
        Some(&Value::from(vec![1u8, 2u8, 3u8]))
    );
    assert_eq!(
        bins.get("bin map"),
        Some(&windpike::map!(1 => 1, 2 => 2, 3 => "hi!"))
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

    let bin = Bin::new("bin999", "test string");
    let ops = &vec![scalar::put(&bin), scalar::get()];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(existed);

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(!existed);

    client.close();
}
