use std::{collections::HashMap, sync::Arc};

use tokio::sync::mpsc;
use tracing::warn;

use super::{
    buffer::{InfoAttr, MessageHeader},
    field_type::FieldType,
    Command, CommandError, Result,
};
use crate::{
    cluster::Node, net::Connection, value::bytes_to_particle, Key, Record, ResultCode, UserKey,
    Value,
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
        conn.buffer().advance(3);
        let info3 = InfoAttr::from_bits_truncate(conn.buffer().read_u8());

        conn.buffer().advance(1);
        let result_code = ResultCode::from(conn.buffer().read_u8());
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
        if info3.contains(InfoAttr::LAST) {
            return Ok((None, false));
        }

        let generation = conn.buffer().read_u32();
        let expiration = conn.buffer().read_u32();
        conn.buffer().advance(4);
        let field_count = conn.buffer().read_u16() as usize; // almost certainly 0
        let op_count = conn.buffer().read_u16() as usize;

        let key = Self::parse_key(conn, field_count).await?;

        // Partition is done, don't go further
        if info3.contains(InfoAttr::PARTITION_DONE) {
            return Ok((None, true));
        }

        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        for _ in 0..op_count {
            conn.read_buffer(8).await?;
            let op_size = conn.buffer().read_u32() as usize;
            conn.buffer().advance(1);
            let particle_type = conn.buffer().read_u8();
            conn.buffer().advance(1);
            let name_size = conn.buffer().read_u8() as usize;
            conn.read_buffer(name_size).await?;
            let name = conn.buffer().read_str(name_size)?;

            let particle_bytes_size = op_size - (4 + name_size);
            conn.read_buffer(particle_bytes_size).await?;
            let value = bytes_to_particle(particle_type, conn.buffer(), particle_bytes_size)?;

            bins.insert(name, value);
        }

        let record = Record::new(Some(key), bins, generation, expiration);
        Ok((Some(record), true))
    }

    async fn parse_stream(&mut self, conn: &mut Connection, size: usize) -> Result<bool> {
        while !self.tx.is_closed() && conn.bytes_read() < size {
            // Read header.
            if let Err(err) = conn.read_buffer(MessageHeader::SIZE).await {
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
            let field_len = conn.buffer().read_u32() as usize;
            conn.read_buffer(field_len).await?;
            let field_type = conn.buffer().read_u8();

            match field_type {
                x if x == FieldType::DigestRipe as u8 => {
                    digest.copy_from_slice(conn.buffer().read_slice(field_len - 1));
                }
                x if x == FieldType::Namespace as u8 => {
                    namespace = conn.buffer().read_str(field_len - 1)?;
                }
                x if x == FieldType::Table as u8 => {
                    set_name = conn.buffer().read_str(field_len - 1)?;
                }
                x if x == FieldType::Key as u8 => {
                    let particle_type = conn.buffer().read_u8();
                    let particle_bytes_size = field_len - 2;
                    orig_key = Some(UserKey::read_from(
                        particle_type,
                        conn.buffer(),
                        particle_bytes_size,
                    )?);
                }
                _ => unreachable!(),
            }
        }

        Ok(Key {
            namespace: namespace.into(),
            set_name: set_name.into(),
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
    fn prepare_buffer(&mut self, _conn: &mut Connection) -> Result<()> {
        // should be implemented downstream
        unreachable!()
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        Some(Arc::clone(&self.node))
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let mut status = true;

        while status {
            conn.read_buffer(8).await?;
            let size = conn.buffer().read_msg_size();
            conn.bookmark();

            status = false;
            if size > 0 {
                status = self.parse_stream(conn, size).await?;
            }
        }

        Ok(())
    }
}
