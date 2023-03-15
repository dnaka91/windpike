use std::{sync::Arc, time::Duration};

use tracing::warn;

use super::{buffer, Command, CommandError, Result, SingleCommand};
use crate::{
    cluster::{Cluster, Node},
    net::Connection,
    policy::WritePolicy,
    Key, ResultCode,
};

pub struct TouchCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
}

impl<'a> TouchCommand<'a> {
    pub fn new(policy: &'a WritePolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        TouchCommand {
            single_command: SingleCommand::new(cluster, key),
            policy,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl<'a> Command for TouchCommand<'a> {
    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer().write_timeout(timeout);
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await.map_err(Into::into)
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer()
            .set_touch(self.policy, self.single_command.key)
            .map_err(Into::into)
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        // Read header.
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!(%err, "Parse result error");
            return Err(err.into());
        }

        conn.buffer().reset_offset();

        let result_code = ResultCode::from(conn.buffer().read_u8(Some(13)));
        if result_code != ResultCode::Ok {
            return Err(CommandError::ServerError(result_code));
        }

        SingleCommand::empty_socket(conn).await
    }
}
