use windpike::{
    operations::{hll, hll::Policy},
    policies::{BasePolicy, WritePolicy},
    Bins, Key, Value,
};

use crate::common::{self, NAMESPACE};

#[tokio::test]
async fn hll() {
    let client = common::client().await;

    let key = Key::new(NAMESPACE, common::rand_str(10), "test");

    let hpolicy = Policy::default();
    let wpolicy = WritePolicy::default();
    let rpolicy = BasePolicy::default();

    let ops = &vec![hll::init(hpolicy, "bin", 4)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let v = vec![Value::from("asd123")];
    let ops = &vec![hll::add(hpolicy, "bin", &v)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Int(1),
        "register update did not match"
    );

    let ops = &vec![hll::get_count("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Int(1),
        "HLL Count did not match"
    );

    let ops = &vec![hll::init_with_min_hash(hpolicy, "bin2", 8, 0)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let ops = &vec![hll::fold("bin2", 6)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let v2 = vec![Value::from("123asd")];
    let ops = &vec![hll::add(hpolicy, "bin2", &v2)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin2").unwrap(),
        Value::Int(1),
        "register update did not match"
    );

    let ops = &vec![hll::describe("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        windpike::list!(4, 0),
        "index bits did not match"
    );

    let rec = client
        .get(&rpolicy, &key, Bins::from(["bin2"]))
        .await
        .unwrap();
    let bin2val = vec![rec.bins.get("bin2").unwrap().clone()];

    let ops = &vec![hll::get_intersect_count("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(0),
        "intersect Count is wrong"
    );

    let ops = &vec![hll::get_union_count("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(2),
        "union Count is wrong"
    );

    let ops = &vec![hll::get_union("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    let val = Value::Hll(vec![
        0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    assert_eq!(*rec.bins.get("bin").unwrap(), val, "union does not match");

    let ops = &vec![hll::refresh_count("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Int(1),
        "HLL refresh count did not match"
    );

    let ops = &vec![
        hll::set_union(hpolicy, "bin", &bin2val),
        hll::get_count("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(2),
        "written union count does not match"
    );

    let ops = &vec![hll::get_similarity("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(0.5),
        "similarity failed"
    );

    client.close();
}
