use windpike::{
    as_list, as_values,
    operations::{list, scalar},
    policies::{BasePolicy, WritePolicy},
    Bin, Bins, Key, Value,
};

use crate::common::{self, NAMESPACE};

#[tokio::test]
async fn cdt_list() {
    let client = common::client().await;

    let policy = BasePolicy::default();

    let wpolicy = WritePolicy::default();
    let key = Key::new(NAMESPACE, common::rand_str(10), -1);
    let val = as_list!("0", 1, 2.1f64);
    let wbin = Bin::new("bin", val.clone());
    let bins = vec![wbin];
    let lpolicy = list::Policy::default();

    client.delete(&wpolicy, &key).await.unwrap();

    client.put(&wpolicy, &key, &bins).await.unwrap();
    let rec = client.get(&policy, &key, Bins::All).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), val);

    let ops = &vec![list::size("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(3));

    let values = vec![9.into(), 8.into(), 7.into()];
    let ops = &vec![
        list::insert_items(lpolicy, "bin", 1, &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(6, as_list!("0", 9, 8, 7, 1, 2.1f64))
    );

    let ops = &vec![list::pop("bin", 0), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!("0", as_list!(9, 8, 7, 1, 2.1f64))
    );

    let ops = &vec![list::pop_range("bin", 0, 2), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(9, 8, as_list!(7, 1, 2.1f64))
    );

    let ops = &vec![list::pop_range_from("bin", 1), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(1, 2.1f64, as_list!(7))
    );

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(6, as_list!("0", 9, 8, 7, 1, 2.1f64))
    );

    let ops = &vec![list::increment(lpolicy, "bin", 1, 4)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(13));

    let ops = &vec![list::remove("bin", 1), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(1, as_list!("0", 8, 7, 1, 2.1f64))
    );

    let ops = &vec![list::remove_range("bin", 1, 2), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(2, as_list!("0", 1, 2.1f64))
    );

    let ops = &vec![list::remove_range_from("bin", -1), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(1, as_list!("0", 1)));

    let v = Value::from(2);
    let ops = &vec![list::set("bin", -1, &v).unwrap(), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![list::trim("bin", 1, 1), scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(6, as_list!(9)));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![list::get("bin", 1)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(9));

    let ops = &vec![list::get_range("bin", 1, -1)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(9, 8, 7, 1, 2.1f64, -1)
    );

    let ops = &vec![list::get_range_from("bin", 2)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7, 1, 2.1f64, -1));

    let rval = Value::from(9);
    let ops = &vec![list::remove_by_value("bin", &rval, list::ReturnType::Count)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(1));

    let rval = vec![Value::from(8), Value::from(7)];
    let ops = &vec![list::remove_by_value_list(
        "bin",
        &rval,
        list::ReturnType::Count,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let beg = Value::from(7);
    let end = Value::from(9);
    let ops = &vec![list::remove_by_value_range(
        "bin",
        list::ReturnType::Count,
        &beg,
        &end,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![list::sort("bin", list::SortFlags::empty())];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let ops = &vec![scalar::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(-1, 1, 7, 8, 9, "0", 2.1f64)
    );

    let ops = &vec![list::remove_by_index("bin", 1, list::ReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(1));

    let ops = &vec![list::remove_by_index_range(
        "bin",
        4,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 2.1f64));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![list::remove_by_index_range_count(
        "bin",
        0,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 9));

    let ops = &vec![list::remove_by_rank("bin", 2, list::ReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(7));

    let ops = &vec![list::remove_by_rank_range(
        "bin",
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 2.1f64));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![list::remove_by_rank_range_count(
        "bin",
        2,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let val = Value::from(1);
    let ops = &vec![list::remove_by_value_relative_rank_range(
        "bin",
        list::ReturnType::Values,
        &val,
        1,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, 8, 9, "0", 2.1f64)
    );

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let val = Value::from(1);
    let ops = &vec![list::remove_by_value_relative_rank_range_count(
        "bin",
        list::ReturnType::Values,
        &val,
        1,
        2,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let val = Value::from(1);
    let ops = &vec![list::get_by_value_relative_rank_range_count(
        "bin",
        &val,
        2,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 9));

    let val = Value::from(1);
    let ops = &vec![list::get_by_value("bin", &val, list::ReturnType::Count)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(1));

    let val = vec![Value::from(1), Value::from("0")];
    let ops = &vec![list::get_by_value_list(
        "bin",
        &val,
        list::ReturnType::Count,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(2));

    let beg = Value::from(1);
    let end = Value::from(9);
    let ops = &vec![list::get_by_value_range(
        "bin",
        &beg,
        &end,
        list::ReturnType::Count,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(3));

    let ops = &vec![list::get_by_index("bin", 3, list::ReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(7));

    let ops = &vec![list::get_by_index_range("bin", 3, list::ReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(7, 1, 2.1f64, -1));

    let ops = &vec![list::get_by_index_range_count(
        "bin",
        0,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 9));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        list::clear("bin"),
        list::append_items(lpolicy, "bin", &values).unwrap(),
        scalar::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![list::get_by_rank("bin", 2, list::ReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(7));

    let ops = &vec![list::get_by_rank_range("bin", 4, list::ReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(9, "0", 2.1f64));

    let ops = &vec![list::get_by_rank_range_count(
        "bin",
        2,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7));

    let val = Value::from(1);
    let ops = &vec![list::get_by_value_relative_rank_range(
        "bin",
        &val,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 9, "0", 2.1f64));

    let val = Value::from(1);
    let ops = &vec![list::get_by_value_relative_rank_range_count(
        "bin",
        &val,
        2,
        2,
        list::ReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 9));
    client.close();
}
