use crate::common;

#[tokio::test]
async fn truncate() {
    common::init_logger();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let result = client.truncate(namespace, &set_name, 0).await;
    assert!(result.is_ok());

    client.close().await.unwrap();
}
