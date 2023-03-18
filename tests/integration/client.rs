use aerospike::{cluster::ClusterError, errors::Error, policy::ClientPolicy, Client};

use crate::common::{self, HOSTS};

#[tokio::test]
async fn cluster_name() {
    let policy = ClientPolicy {
        cluster_name: Some("notTheRealClusterName".into()),
        ..ClientPolicy::default()
    };
    let err = Client::new(&policy, HOSTS).await.unwrap_err();
    assert!(matches!(err, Error::Cluster(ClusterError::Connection)));
}

#[tokio::test]
async fn node_names() {
    let client = common::client().await;
    let names = client.node_names().await;
    assert!(!names.is_empty());
    client.close();
}

#[tokio::test]
async fn nodes() {
    let client = common::client().await;
    let nodes = client.nodes().await;
    assert!(!nodes.is_empty());
    client.close();
}

#[tokio::test]
async fn get_node() {
    let client = common::client().await;
    for name in client.node_names().await {
        let node = client.get_node(&name).await;
        assert!(node.is_some());
    }
    client.close();
}

#[tokio::test]
async fn close() {
    let client = common::client().await;
    assert!(client.is_connected().await, "The client is not connected");

    client.close();
    assert!(
        !client.is_connected().await,
        "The client did not disconnect"
    );
}
