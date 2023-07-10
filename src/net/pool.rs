use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use async_trait::async_trait;
use bb8::{ManageConnection, RunError};

use super::{Connection, Host, NetError, Result};
use crate::policies::ClientPolicy;

struct NodeConnectionManager {
    host: Host,
    policy: ClientPolicy,
}

#[async_trait]
impl ManageConnection for NodeConnectionManager {
    type Connection = Connection;
    type Error = NetError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Connection::new(&self.host.address(), &self.policy).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        if conn.active() {
            Ok(())
        } else {
            Err(NetError::NoMoreConnections)
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.active()
    }
}

#[derive(Debug)]
pub struct Pool(bb8::Pool<NodeConnectionManager>);

impl Pool {
    pub async fn new(host: Host, policy: ClientPolicy) -> Result<Self> {
        bb8::Builder::new()
            .max_size(policy.max_conns_per_node)
            .idle_timeout(policy.idle_timeout)
            .connection_timeout(policy.timeout.unwrap_or(Duration::from_secs(5)))
            .build(NodeConnectionManager { host, policy })
            .await
            .map(Self)
    }

    pub async fn get(&self) -> Result<PooledConnection<'_>> {
        self.0
            .get()
            .await
            .map(PooledConnection)
            .map_err(|e| match e {
                RunError::User(e) => e,
                RunError::TimedOut => NetError::NoMoreConnections,
            })
    }
}

pub struct PooledConnection<'a>(bb8::PooledConnection<'a, NodeConnectionManager>);

impl<'a> Deref for PooledConnection<'a> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self.0
    }
}

impl<'a> DerefMut for PooledConnection<'a> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.0
    }
}
