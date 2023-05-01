use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::{Command, CommandError, Result, SingleCommand};
use crate::{
    cluster::{Cluster, Node},
    net::Connection,
    policy::WritePolicy,
    Key, ResultCode,
};

pub struct ExistsCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    pub exists: bool,
}

impl<'a> ExistsCommand<'a> {
    pub fn new(policy: &'a WritePolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        ExistsCommand {
            single_command: SingleCommand::new(cluster, key),
            policy,
            exists: false,
        }
    }

    pub async fn execute(&mut self) -> Result<(), CommandError> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait]
impl<'a> Command for ExistsCommand<'a> {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer()
            .set_exists(self.policy, self.single_command.key)
            .map_err(Into::into)
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let header = conn.read_header().await.map_err(|err| {
            warn!(%err, "Parse result error");
            err
        })?;

        if !matches!(
            header.result_code,
            ResultCode::Ok | ResultCode::KeyNotFoundError
        ) {
            return Err(CommandError::ServerError(header.result_code));
        }

        self.exists = header.result_code == ResultCode::Ok;

        SingleCommand::empty_socket(conn, header.size).await
    }
}
