use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{
    buffer::{InfoAttr, ProtoHeader},
    field_type::FieldType,
    Command, CommandError, Result,
};
use crate::{
    cluster::Node, net::Connection, value::bytes_to_particle, Key, Record, ResultCode, UserKey,
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

    async fn parse_stream(&mut self, conn: &mut Connection, header: ProtoHeader) -> Result<bool> {
        while !self.tx.is_closed() && conn.bytes_read() < header.size {
            let res = Self::parse_record(conn, header).await;
            match res {
                Ok((Some(rec), _)) => {
                    if self.tx.send(Ok(rec)).await.is_err() {
                        return Ok(false);
                    }
                }
                Ok((None, false)) => return Ok(false),
                Ok((None, true)) => continue,
                Err(err) => {
                    self.tx.send(Err(err)).await.ok();
                    return Ok(false);
                }
            };
        }

        Ok(true)
    }

    async fn parse_record(
        conn: &mut Connection,
        proto: ProtoHeader,
    ) -> Result<(Option<Record>, bool)> {
        let header = conn.read_stream_message_header(proto).await?;

        if header.result_code != ResultCode::Ok {
            if conn.bytes_read() < proto.size {
                let remaining = proto.size - conn.bytes_read();
                conn.read_buffer(remaining).await?;
            }

            return match header.result_code {
                ResultCode::KeyNotFoundError => Ok((None, false)),
                _ => Err(CommandError::ServerError(header.result_code)),
            };
        }

        // if cmd is the end marker of the response, do not proceed further
        if header.info_attr.contains(InfoAttr::LAST) {
            return Ok((None, false));
        }

        let key = Self::parse_key(conn, header.field_count).await?;

        // Partition is done, don't go further
        if header.info_attr.contains(InfoAttr::PARTITION_DONE) {
            return Ok((None, true));
        }

        let mut bins = HashMap::with_capacity(header.operation_count.into());

        for _ in 0..header.operation_count {
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

        let record = Record::new(Some(key), bins, header.generation, header.expiration);
        Ok((Some(record), true))
    }

    pub async fn parse_key(conn: &mut Connection, field_count: u16) -> Result<Key> {
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
                _ => panic!("invalid field type `{field_type}`"),
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

#[async_trait]
impl Command for StreamCommand {
    fn prepare_buffer(&mut self, _conn: &mut Connection) -> Result<()> {
        panic!("stream command doesn't write the buffer itself")
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        Some(Arc::clone(&self.node))
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        loop {
            let header = conn.read_proto_header().await?;
            if header.size == 0 {
                break;
            }

            conn.bookmark();

            if !self.parse_stream(conn, header).await? {
                break;
            }
        }

        Ok(())
    }
}
