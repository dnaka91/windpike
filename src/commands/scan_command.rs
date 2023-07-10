use std::{str, sync::Arc};

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{Command, Result, SingleCommand, StreamCommand};
use crate::{cluster::Node, net::Connection, policies::ScanPolicy, Bins, Record};

pub struct ScanCommand<'a> {
    stream_command: StreamCommand,
    policy: &'a ScanPolicy,
    namespace: &'a str,
    set_name: &'a str,
    bins: Bins,
    partitions: Vec<u16>,
}

impl<'a> ScanCommand<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        policy: &'a ScanPolicy,
        node: Arc<Node>,
        namespace: &'a str,
        set_name: &'a str,
        bins: Bins,
        tx: mpsc::Sender<Result<Record<'static>>>,
        task_id: u64,
        partitions: Vec<u16>,
    ) -> Self {
        ScanCommand {
            stream_command: StreamCommand::new(node, tx, task_id),
            policy,
            namespace,
            set_name,
            bins,
            partitions,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait]
impl<'a> Command for ScanCommand<'a> {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer()
            .set_scan(
                self.policy,
                self.namespace,
                self.set_name,
                &self.bins,
                self.stream_command.task_id(),
                &self.partitions,
            )
            .map_err(Into::into)
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        self.stream_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        StreamCommand::parse_result(&mut self.stream_command, conn).await
    }
}
