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

use std::collections::HashMap;

use aerospike::{
    as_bin, as_list, as_map,
    operations::{
        cdt_context::{ctx_map_key, ctx_map_key_create},
        maps, MapOrder,
    },
    Bins, Key, MapPolicy, MapReturnType, ReadPolicy, Value, WritePolicy,
};

use crate::common;

#[tokio::test]
async fn map_operations() {
    common::init_logger();

    let client = common::client().await;
    let namespace = common::namespace().to_owned();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();
    let rpolicy = ReadPolicy::default();

    let key = common::rand_str(10);
    let key = Key::new(namespace, set_name, &key).unwrap();

    client.delete(&wpolicy, &key).await.unwrap();

    let val = as_map!("a" => 1, "b" => 2);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![bin];

    client.put(&wpolicy, &key, &bins).await.unwrap();

    let (k, v) = (Value::from("c"), Value::from(3));
    let op = maps::put(&mpolicy, bin_name, &k, &v);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(3));

    let op = maps::size(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(3));

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(
        *rec.bins.get(bin_name).unwrap(),
        as_map!("a" => 1, "b" => 2, "c" => 3)
    );

    let mut items = HashMap::new();
    items.insert(Value::from("d"), Value::from(4));
    items.insert(Value::from("e"), Value::from(5));
    let op = maps::put_items(&mpolicy, bin_name, &items);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(5));

    let k = Value::from("e");
    let op = maps::remove_by_key(bin_name, &k, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(5));

    let (k, i) = (Value::from("a"), Value::from(19));
    let op = maps::increment_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns value of the key after increment
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(20));

    let (k, i) = (Value::from("a"), Value::from(10));
    let op = maps::decrement_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns value of the key after decrement
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(10));

    let (k, i) = (Value::from("a"), Value::from(5));
    let dec = maps::decrement_value(&mpolicy, bin_name, &k, &i);
    let (k, i) = (Value::from("a"), Value::from(7));
    let inc = maps::increment_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[dec, inc]).await.unwrap();
    // returns values from multiple ops returned as list
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5, 12));

    let op = maps::clear(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // map_clear returns no result
    assert!(rec.bins.get(bin_name).is_none());

    client.delete(&wpolicy, &key).await.unwrap();

    let val = as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![bin];

    client.put(&wpolicy, &key, bins.as_slice()).await.unwrap();

    let op = maps::get_by_index(bin_name, 0, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(1));

    let op = maps::get_by_index_range(bin_name, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3));

    let op = maps::get_by_index_range_from(bin_name, 3, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    let val = Value::from(5);
    let op = maps::get_by_value(bin_name, &val, MapReturnType::Index);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4));

    let beg = Value::from(3);
    let end = Value::from(5);
    let op = maps::get_by_value_range(bin_name, &beg, &end, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let op = maps::get_by_rank(bin_name, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(3));

    let op = maps::get_by_rank_range(bin_name, 2, 3, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4, 5));

    let op = maps::get_by_rank_range_from(bin_name, 2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(3));

    let mkey = Value::from("b");
    let op = maps::get_by_key(bin_name, &mkey, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let mkey = Value::from("b");
    let mkey2 = Value::from("d");
    let op = maps::get_by_key_range(bin_name, &mkey, &mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let mkey = vec![Value::from("b"), Value::from("d")];
    let op = maps::get_by_key_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let mkey = vec![Value::from(2), Value::from(3)];
    let op = maps::get_by_value_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let mkey = vec![Value::from("b"), Value::from("d")];
    let op = maps::remove_by_key_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let mkey = Value::from("a");
    let mkey2 = Value::from("c");
    let op = maps::remove_by_key_range(bin_name, &mkey, &mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(1));

    let mkey = Value::from(5);
    let op = maps::remove_by_value(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(1));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = vec![Value::from(4), Value::from(5)];
    let op = maps::remove_by_value_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let mkey = Value::from(1);
    let mkey2 = Value::from(3);
    let op = maps::remove_by_value_range(bin_name, &mkey, &mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let op = maps::remove_by_index(bin_name, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let op = maps::remove_by_index_range(bin_name, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    let op = maps::remove_by_index_range_from(bin_name, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let op = maps::remove_by_rank(bin_name, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(2));

    let op = maps::remove_by_rank_range(bin_name, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let op = maps::remove_by_rank_range_from(bin_name, 3, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = Value::from("b");
    let op = maps::remove_by_key_relative_index_range(bin_name, &mkey, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    let mkey = Value::from("c");
    let op =
        maps::remove_by_key_relative_index_range_count(bin_name, &mkey, 0, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = Value::from(3);
    let op = maps::remove_by_value_relative_rank_range_count(
        bin_name,
        &mkey,
        2,
        2,
        MapReturnType::Value,
    );
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5));

    let mkey = Value::from(2);
    let op = maps::remove_by_value_relative_rank_range(bin_name, &mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = Value::from("a");
    let op = maps::get_by_key_relative_index_range(bin_name, &mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3, 4, 5));

    let mkey = Value::from("a");
    let op =
        maps::get_by_key_relative_index_range_count(bin_name, &mkey, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3));

    let mkey = Value::from(2);
    let op = maps::get_by_value_relative_rank_range(bin_name, &mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4, 5));

    let mkey = Value::from(2);
    let op =
        maps::get_by_value_relative_rank_range_count(bin_name, &mkey, 1, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3));

    let mkey = Value::from("ctxtest");
    let mval = as_map!("x" => 7, "y" => 8, "z" => 9);
    let op = maps::put(&mpolicy, bin_name, &mkey, &mval);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let ctx = &vec![ctx_map_key(mkey)];
    let xkey = Value::from("y");
    let op = maps::get_by_key(bin_name, &xkey, MapReturnType::Value).set_context(ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(8));

    let mkey = Value::from("ctxtest2");
    let ctx = &vec![ctx_map_key_create(mkey.clone(), MapOrder::KeyOrdered)];
    let xkey = Value::from("y");
    let xval = Value::from(8);
    let op = [maps::put(&mpolicy, bin_name, &xkey, &xval).set_context(ctx)];
    client.operate(&wpolicy, &key, &op).await.unwrap();
    let op = [maps::get_by_key(bin_name, &xkey, MapReturnType::Value).set_context(ctx)];
    let rec = client.operate(&wpolicy, &key, &op).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(8));

    let mkey2 = Value::from("ctxtest3");
    let ctx = &vec![
        ctx_map_key(mkey),
        ctx_map_key_create(mkey2, MapOrder::Unordered),
    ];
    let xkey = Value::from("c");
    let xval = Value::from(9);
    let op = [maps::put(&mpolicy, bin_name, &xkey, &xval).set_context(ctx)];
    client.operate(&wpolicy, &key, &op).await.unwrap();
    let op = [maps::get_by_key(bin_name, &xkey, MapReturnType::Value).set_context(ctx)];
    let rec = client.operate(&wpolicy, &key, &op).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), Value::from(9));

    client.close().await.unwrap();
}
