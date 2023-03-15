use aerospike::{cluster::ClusterError, errors::Error, Client};

mod common;

#[tokio::test]
async fn cluster_name() {
    let policy = &mut common::client_policy().clone();
    policy.cluster_name = Some(String::from("notTheRealClusterName"));
    let err = Client::new(policy, &common::hosts()).await.unwrap_err();
    eprintln!("{err:?}");
    assert!(matches!(err, Error::Cluster(ClusterError::Connection)));
}

#[tokio::test]
async fn node_names() {
    let client = common::client().await;
    let names = client.node_names().await;
    assert!(!names.is_empty());
    client.close().await.unwrap();
}

#[tokio::test]
async fn nodes() {
    let client = common::client().await;
    let nodes = client.nodes().await;
    assert!(!nodes.is_empty());
    client.close().await.unwrap();
}

#[tokio::test]
async fn get_node() {
    let client = common::client().await;
    for name in client.node_names().await {
        let node = client.get_node(&name).await;
        assert!(node.is_some());
    }
    client.close().await.unwrap();
}

#[tokio::test]
async fn close() {
    let client = Client::new(common::client_policy(), &common::hosts())
        .await
        .unwrap();
    assert!(client.is_connected().await, "The client is not connected");

    if let Ok(()) = client.close().await {
        assert!(
            !client.is_connected().await,
            "The client did not disconnect"
        );
    } else {
        panic!("Failed to close client");
    }
}
