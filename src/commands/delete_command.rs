use std::sync::Arc;

use tracing::warn;

use super::{Command, CommandError, Result, SingleCommand};
use crate::{
    cluster::{Cluster, Node},
    net::Connection,
    policy::WritePolicy,
    Key, ResultCode,
};

pub struct DeleteCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    pub existed: bool,
}

impl<'a> DeleteCommand<'a> {
    pub fn new(policy: &'a WritePolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        DeleteCommand {
            single_command: SingleCommand::new(cluster, key),
            policy,
            existed: false,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl<'a> Command for DeleteCommand<'a> {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<(), CommandError> {
        conn.buffer()
            .set_delete(self.policy, self.single_command.key)
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

        self.existed = header.result_code == ResultCode::Ok;

        SingleCommand::empty_socket(conn, header.size).await
    }
}
