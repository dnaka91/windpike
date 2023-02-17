// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{sync::Arc, time::Duration};

use tracing::warn;

use crate::{
    cluster::{Cluster, Node},
    commands::{buffer, Command, SingleCommand},
    errors::{ErrorKind, Result},
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
    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_delete(self.policy, self.single_command.key)
    }

    async fn get_node(&self) -> Result<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        // Read header.
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!(%err, "Parse result error");
            return Err(err);
        }

        conn.buffer.reset_offset();

        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));

        if result_code != ResultCode::Ok && result_code != ResultCode::KeyNotFoundError {
            bail!(ErrorKind::ServerError(result_code));
        }

        self.existed = result_code == ResultCode::Ok;

        SingleCommand::empty_socket(conn).await
    }
}
