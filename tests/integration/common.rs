use std::env;

use aerospike::{Client, ClientPolicy};
use once_cell::sync::Lazy;
use rand::{distributions::Alphanumeric, Rng};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn hosts() -> &'static str {
    static AEROSPIKE_HOSTS: Lazy<String> =
        Lazy::new(|| env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1")));

    &AEROSPIKE_HOSTS
}

pub fn namespace() -> &'static str {
    static AEROSPIKE_NAMESPACE: Lazy<String> =
        Lazy::new(|| env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| String::from("test")));

    &AEROSPIKE_NAMESPACE
}

pub fn client_policy() -> &'static ClientPolicy {
    static GLOBAL_CLIENT_POLICY: Lazy<ClientPolicy> = Lazy::new(|| {
        let mut policy = ClientPolicy::default();
        if let Ok(user) = env::var("AEROSPIKE_USER") {
            let password = env::var("AEROSPIKE_PASSWORD").unwrap_or_default();
            policy.set_user_password(user, &password).unwrap();
        }
        policy.cluster_name = env::var("AEROSPIKE_CLUSTER").ok();
        policy
    });

    &GLOBAL_CLIENT_POLICY
}

pub async fn client() -> Client {
    Client::new(client_policy(), &hosts()).await.unwrap()
}

pub fn rand_str(sz: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..sz).map(|_| rng.sample(Alphanumeric) as char).collect()
}

pub fn init_logger() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}
