use rand::{distributions::Alphanumeric, Rng};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use windpike::{policies::ClientPolicy, Client};

pub const HOSTS: &str = "127.0.0.1";
pub const NAMESPACE: &str = "test";

pub async fn client() -> Client {
    init_logger();
    Client::new(&ClientPolicy::default(), HOSTS).await.unwrap()
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
