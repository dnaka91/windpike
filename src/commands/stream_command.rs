// Copyright 2015-2020 Aerospike, Inc.
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

use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tracing::warn;

use super::{buffer, field_type::FieldType, Command, CommandError, Result};
use crate::{
    cluster::Node, net::Connection, value::bytes_to_particle, Key, Record, ResultCode, Value,
};

pub struct StreamCommand {
    node: Arc<Node>,
    tx: mpsc::Sender<Result<Record>>,
    task_id: u64,
}

impl StreamCommand {
    pub fn new(node: Arc<Node>, tx: mpsc::Sender<Result<Record>>, task_id: u64) -> Self {
        Self { node, tx, task_id }
    }

    async fn parse_record(conn: &mut Connection, size: usize) -> Result<(Option<Record>, bool)> {
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(5)));
        if result_code != ResultCode::Ok {
            if conn.bytes_read() < size {
                let remaining = size - conn.bytes_read();
                conn.read_buffer(remaining).await?;
            }

            return match result_code {
                ResultCode::KeyNotFoundError => Ok((None, false)),
                _ => Err(CommandError::ServerError(result_code)),
            };
        }

        // if cmd is the end marker of the response, do not proceed further
        let info3 = conn.buffer.read_u8(Some(3));
        if info3 & buffer::INFO3_LAST == buffer::INFO3_LAST {
            return Ok((None, false));
        }

        conn.buffer.skip(6);
        let generation = conn.buffer.read_u32(None);
        let expiration = conn.buffer.read_u32(None);
        conn.buffer.skip(4);
        let field_count = conn.buffer.read_u16(None) as usize; // almost certainly 0
        let op_count = conn.buffer.read_u16(None) as usize;

        let key = Self::parse_key(conn, field_count).await?;

        // Partition is done, don't go further
        if info3 & buffer::_INFO3_PARTITION_DONE != 0 {
            return Ok((None, true));
        }

        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        for _ in 0..op_count {
            conn.read_buffer(8).await?;
            let op_size = conn.buffer.read_u32(None) as usize;
            conn.buffer.skip(1);
            let particle_type = conn.buffer.read_u8(None);
            conn.buffer.skip(1);
            let name_size = conn.buffer.read_u8(None) as usize;
            conn.read_buffer(name_size).await?;
            let name: String = conn.buffer.read_str(name_size)?;

            let particle_bytes_size = op_size - (4 + name_size);
            conn.read_buffer(particle_bytes_size).await?;
            let value = bytes_to_particle(particle_type, &mut conn.buffer, particle_bytes_size)?;

            bins.insert(name, value);
        }

        let record = Record::new(Some(key), bins, generation, expiration);
        Ok((Some(record), true))
    }

    async fn parse_stream(&mut self, conn: &mut Connection, size: usize) -> Result<bool> {
        while !self.tx.is_closed() && conn.bytes_read() < size {
            // Read header.
            if let Err(err) = conn
                .read_buffer(buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await
            {
                warn!(%err, "Parse result error");
                return Err(err.into());
            }

            let res = Self::parse_record(conn, size).await;
            match res {
                Ok((Some(rec), _)) => {
                    if self.tx.send(Ok(rec)).await.is_err() {
                        break;
                    }
                }
                Ok((None, cont)) => return Ok(cont),
                Err(err) => {
                    self.tx.send(Err(err)).await.ok();
                    return Ok(false);
                }
            };
        }

        Ok(true)
    }

    pub async fn parse_key(conn: &mut Connection, field_count: usize) -> Result<Key> {
        let mut digest = [0; 20];
        let mut namespace = String::new();
        let mut set_name = String::new();
        let mut orig_key = None;

        for _ in 0..field_count {
            conn.read_buffer(4).await?;
            let field_len = conn.buffer.read_u32(None) as usize;
            conn.read_buffer(field_len).await?;
            let field_type = conn.buffer.read_u8(None);

            match field_type {
                x if x == FieldType::DigestRipe as u8 => {
                    digest.copy_from_slice(conn.buffer.read_slice(field_len - 1));
                }
                x if x == FieldType::Namespace as u8 => {
                    namespace = conn.buffer.read_str(field_len - 1)?;
                }
                x if x == FieldType::Table as u8 => {
                    set_name = conn.buffer.read_str(field_len - 1)?;
                }
                x if x == FieldType::Key as u8 => {
                    let particle_type = conn.buffer.read_u8(None);
                    let particle_bytes_size = field_len - 2;
                    orig_key = Some(bytes_to_particle(
                        particle_type,
                        &mut conn.buffer,
                        particle_bytes_size,
                    )?);
                }
                _ => unreachable!(),
            }
        }

        Ok(Key {
            namespace,
            set_name,
            user_key: orig_key,
            digest,
        })
    }

    pub(super) fn task_id(&self) -> u64 {
        self.task_id
    }
}

#[async_trait::async_trait]
impl Command for StreamCommand {
    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await.map_err(Into::into)
    }

    #[allow(unused_variables)]
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        // should be implemented downstream
        unreachable!()
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        Some(self.node.clone())
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let mut status = true;

        while status {
            conn.read_buffer(8).await?;
            let size = conn.buffer.read_msg_size(None);
            conn.bookmark();

            status = false;
            if size > 0 {
                status = self.parse_stream(conn, size).await?;
            }
        }

        Ok(())
    }
}
