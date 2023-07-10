use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    sync::Arc,
};

use async_trait::async_trait;
use tracing::warn;

use super::{Command, CommandError, Result, SingleCommand};
use crate::{
    as_list,
    cluster::{Cluster, Node},
    msgpack::Read,
    net::Connection,
    policies::BasePolicy,
    Bins, Key, Record, ResultCode, Value,
};

pub struct ReadCommand<'a> {
    pub single_command: SingleCommand<'a>,
    pub record: Option<Record<'a>>,
    policy: &'a BasePolicy,
    bins: Bins,
}

impl<'a> ReadCommand<'a> {
    pub fn new(
        policy: &'a BasePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key<'a>,
        bins: Bins,
    ) -> Self {
        ReadCommand {
            single_command: SingleCommand::new(cluster, key),
            bins,
            policy,
            record: None,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }

    fn parse_record(
        conn: &mut Connection,
        op_count: u16,
        field_count: u16,
        generation: u32,
        expiration: u32,
    ) -> Result<Record<'static>> {
        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count.into());

        // There can be fields in the response (setname etc). For now, ignore them. Expose them to
        // the API if needed in the future.
        for _ in 0..field_count {
            let field_size = conn.buffer().read_u32() as usize;
            conn.buffer().advance(4 + field_size);
        }

        for _ in 0..op_count {
            let op_size = conn.buffer().read_u32() as usize;
            conn.buffer().advance(1);
            let particle_type = conn.buffer().read_u8();
            conn.buffer().advance(1);
            let name_size = conn.buffer().read_u8() as usize;
            let name = conn.buffer().read_str(name_size)?;

            let particle_bytes_size = op_size - (4 + name_size);
            let value = Value::read_from(conn.buffer(), particle_type, particle_bytes_size)?;

            if value != Value::Nil {
                // list/map operations may return multiple values for the same bin.
                match bins.entry(name) {
                    Vacant(entry) => {
                        entry.insert(value);
                    }
                    Occupied(entry) => match entry.into_mut() {
                        Value::List(list) => list.push(value),
                        prev => {
                            *prev = as_list!(prev.clone(), value);
                        }
                    },
                }
            }
        }

        Ok(Record::new(None, bins, generation, expiration))
    }
}

#[async_trait]
impl<'a> Command for ReadCommand<'a> {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer()
            .set_read(self.policy, self.single_command.key, &self.bins)
            .map_err(Into::into)
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let header = conn.read_header().await.map_err(|err| {
            warn!(%err, "failed to read message header");
            err
        })?;

        if header.result_code != ResultCode::Ok {
            return Err(CommandError::ServerError(header.result_code));
        }

        // Read remaining message bytes
        if header.size > 0 {
            if let Err(err) = conn.read_buffer(header.size).await {
                warn!(%err, "failed to read message body");
                return Err(err.into());
            }
        }

        match header.result_code {
            ResultCode::Ok => {
                let record = if self.bins == Bins::None {
                    Record::new(None, HashMap::new(), header.generation, header.expiration)
                } else {
                    Self::parse_record(
                        conn,
                        header.operation_count,
                        header.field_count,
                        header.generation,
                        header.expiration,
                    )?
                };
                self.record = Some(record);
                Ok(())
            }
            rc => Err(CommandError::ServerError(rc)),
        }
    }
}
