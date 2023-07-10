use std::sync::Arc;

use async_trait::async_trait;

use super::{Command, ReadCommand, Result, SingleCommand};
use crate::{
    cluster::{Cluster, Node},
    net::Connection,
    operations::Operation,
    policies::WritePolicy,
    Bins, Key,
};

pub struct OperateCommand<'a> {
    pub read_command: ReadCommand<'a>,
    policy: &'a WritePolicy,
    operations: &'a [Operation<'a>],
}

impl<'a> OperateCommand<'a> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        operations: &'a [Operation<'a>],
    ) -> Self {
        OperateCommand {
            read_command: ReadCommand::new(&policy.base_policy, cluster, key, Bins::All),
            policy,
            operations,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait]
impl<'a> Command for OperateCommand<'a> {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer()
            .set_operate(
                self.policy,
                self.read_command.single_command.key,
                self.operations,
            )
            .map_err(Into::into)
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        self.read_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        self.read_command.parse_result(conn).await
    }
}
