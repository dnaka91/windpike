use crate::common::{self, NAMESPACE};

#[tokio::test]
async fn truncate() {
    let client = common::client().await;

    let result = common::client()
        .await
        .truncate(NAMESPACE, &common::rand_str(10), 0)
        .await;
    assert!(result.is_ok());

    client.close();
}
