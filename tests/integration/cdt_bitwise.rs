use aerospike::{
    operations::{
        bitwise,
        bitwise::{BitPolicy, BitwiseOverflowActions},
    },
    policy::WritePolicy,
    Key, Value,
};

use crate::common::{self, NAMESPACE};

#[tokio::test]
async fn cdt_bitwise() {
    common::init_logger();

    let client = common::client().await;

    let wpolicy = WritePolicy::default();
    let key = Key::new(NAMESPACE, common::rand_str(10), -1);
    let val = Value::Blob(vec![
        0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101,
    ]);
    let bpolicy = BitPolicy::default();

    client.delete(&wpolicy, &key).await.unwrap();

    // Verify the insert and Get Command
    let ops = &vec![
        bitwise::insert("bin", 0, &val, &bpolicy),
        bitwise::get("bin", 9, 5),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b10000000]));

    // Verify the Count command
    let ops = &vec![bitwise::count("bin", 20, 4)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(2));

    // Verify the set command
    let val = Value::Blob(vec![0b11100000]);
    let ops = &vec![
        bitwise::set("bin", 13, 3, &val, &bpolicy),
        bitwise::get("bin", 0, 40),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![
            0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101
        ])
    );

    // Verify Remove command
    let ops = &vec![
        bitwise::remove("bin", 0, 1, &bpolicy),
        bitwise::get("bin", 0, 8),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b01000111]));

    // Verify OR command
    let val = Value::Blob(vec![0b10101010]);
    let ops = &vec![
        bitwise::or("bin", 0, 8, &val, &bpolicy),
        bitwise::get("bin", 0, 8),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b11101111]));

    // Verify XOR command
    let val = Value::Blob(vec![0b10101100]);
    let ops = &vec![
        bitwise::xor("bin", 0, 8, &val, &bpolicy),
        bitwise::get("bin", 0, 8),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b01000011]));

    // Verify AND command
    let val = Value::Blob(vec![0b01011010]);
    let ops = &vec![
        bitwise::and("bin", 0, 8, &val, &bpolicy),
        bitwise::get("bin", 0, 8),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b01000010]));

    // Verify NOT command
    let ops = &vec![
        bitwise::not("bin", 0, 8, &bpolicy),
        bitwise::get("bin", 0, 8),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b10111101]));

    // Verify LSHIFT command
    let ops = &vec![
        bitwise::lshift("bin", 24, 8, 3, &bpolicy),
        bitwise::get("bin", 24, 8),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b00101000]));

    // Verify RSHIFT command
    let ops = &vec![
        bitwise::rshift("bin", 0, 9, 1, &bpolicy),
        bitwise::get("bin", 0, 16),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b01011110, 0b10000011])
    );

    // Verify Add command
    let ops = &vec![
        bitwise::add(
            "bin",
            0,
            8,
            128,
            false,
            BitwiseOverflowActions::Fail,
            &bpolicy,
        ),
        bitwise::get("bin", 0, 32),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b11011110, 0b10000011, 0b00000100, 0b00101000])
    );

    // Verify Subtract command
    let ops = &vec![
        bitwise::subtract(
            "bin",
            0,
            8,
            128,
            false,
            BitwiseOverflowActions::Fail,
            &bpolicy,
        ),
        bitwise::get("bin", 0, 32),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b01011110, 0b10000011, 0b00000100, 0b00101000])
    );

    // Verify the set int command
    let ops = &vec![
        bitwise::set_int("bin", 8, 8, 255, &bpolicy),
        bitwise::get("bin", 0, 32),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b01011110, 0b11111111, 0b00000100, 0b00101000])
    );

    // Verify the get int command
    let ops = &vec![bitwise::get_int("bin", 8, 8, false)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(255));

    // Verify the LSCAN command
    let ops = &vec![bitwise::lscan("bin", 19, 8, true)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(2));

    // Verify the RSCAN command
    let ops = &vec![bitwise::rscan("bin", 19, 8, true)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(7));
    client.close();
}
