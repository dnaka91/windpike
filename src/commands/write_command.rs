use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::{Command, CommandError, Result, SingleCommand};
use crate::{
    cluster::{Cluster, Node},
    net::Connection,
    operations::OperationType,
    policy::WritePolicy,
    Bin, Key, ResultCode,
};

pub(crate) struct WriteCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    bins: &'a [Bin<'a>],
    operation: OperationType,
}

impl<'a, 'b> WriteCommand<'a> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        bins: &'a [Bin<'b>],
        operation: OperationType,
    ) -> Self {
        WriteCommand {
            single_command: SingleCommand::new(cluster, key),
            bins,
            policy,
            operation,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait]
impl<'a> Command for WriteCommand<'a> {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer()
            .set_write(
                self.policy,
                self.operation,
                self.single_command.key,
                self.bins,
            )
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

        if header.result_code != ResultCode::Ok {
            return Err(CommandError::ServerError(header.result_code));
        }

        SingleCommand::empty_socket(conn, header.size).await
    }
}
