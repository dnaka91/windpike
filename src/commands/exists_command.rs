use std::sync::Arc;

use tracing::warn;

use super::{buffer, Command, CommandError, Result, SingleCommand};
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

#[async_trait::async_trait]
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
        // Read header.
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!(%err, "Parse result error");
            return Err(err.into());
        }

        conn.buffer().reset_offset();

        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!
        let result_code = ResultCode::from(conn.buffer().read_u8(Some(13)));

        if result_code != ResultCode::Ok && result_code != ResultCode::KeyNotFoundError {
            return Err(CommandError::ServerError(result_code));
        }

        self.exists = result_code == ResultCode::Ok;

        SingleCommand::empty_socket(conn).await
    }
}
