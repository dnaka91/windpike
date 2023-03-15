use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    sync::Arc,
    time::Duration,
};

use tracing::warn;

use super::{buffer, Command, CommandError, Result, SingleCommand};
use crate::{
    as_list,
    cluster::{Cluster, Node},
    net::Connection,
    policy::ReadPolicy,
    value::bytes_to_particle,
    Bins, Key, Record, ResultCode, Value,
};

pub struct ReadCommand<'a> {
    pub single_command: SingleCommand<'a>,
    pub record: Option<Record>,
    policy: &'a ReadPolicy,
    bins: Bins,
}

impl<'a> ReadCommand<'a> {
    pub fn new(policy: &'a ReadPolicy, cluster: Arc<Cluster>, key: &'a Key, bins: Bins) -> Self {
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
        op_count: usize,
        field_count: usize,
        generation: u32,
        expiration: u32,
    ) -> Result<Record> {
        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        // There can be fields in the response (setname etc). For now, ignore them. Expose them to
        // the API if needed in the future.
        for _ in 0..field_count {
            let field_size = conn.buffer().read_u32(None) as usize;
            conn.buffer().skip(4 + field_size);
        }

        for _ in 0..op_count {
            let op_size = conn.buffer().read_u32(None) as usize;
            conn.buffer().skip(1);
            let particle_type = conn.buffer().read_u8(None);
            conn.buffer().skip(1);
            let name_size = conn.buffer().read_u8(None) as usize;
            let name = conn.buffer().read_str(name_size)?;

            let particle_bytes_size = op_size - (4 + name_size);
            let value = bytes_to_particle(particle_type, conn.buffer(), particle_bytes_size)?;

            if value != Value::Nil {
                // list/map operations may return multiple values for the same bin.
                match bins.entry(name) {
                    Vacant(entry) => {
                        entry.insert(value);
                    }
                    Occupied(entry) => match *entry.into_mut() {
                        Value::List(ref mut list) => list.push(value),
                        ref mut prev => {
                            *prev = as_list!(prev.clone(), value);
                        }
                    },
                }
            }
        }

        Ok(Record::new(None, bins, generation, expiration))
    }
}

#[async_trait::async_trait]
impl<'a> Command for ReadCommand<'a> {
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
            .set_read(self.policy, self.single_command.key, &self.bins)
            .map_err(Into::into)
    }

    async fn get_node(&self) -> Option<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!(%err, "Parse result error");
            return Err(err.into());
        }

        conn.buffer().reset_offset();
        let sz = conn.buffer().read_u64(Some(0));
        let header_length = conn.buffer().read_u8(Some(8));
        let result_code = conn.buffer().read_u8(Some(13));
        let generation = conn.buffer().read_u32(Some(14));
        let expiration = conn.buffer().read_u32(Some(18));
        let field_count = conn.buffer().read_u16(Some(26)) as usize; // almost certainly 0
        let op_count = conn.buffer().read_u16(Some(28)) as usize;
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - u64::from(header_length)) as usize;

        // Read remaining message bytes
        if receive_size > 0 {
            if let Err(err) = conn.read_buffer(receive_size).await {
                warn!(%err, "Parse result error");
                return Err(err.into());
            }
        }

        match ResultCode::from(result_code) {
            ResultCode::Ok => {
                let record = if self.bins == Bins::None {
                    Record::new(None, HashMap::new(), generation, expiration)
                } else {
                    Self::parse_record(conn, op_count, field_count, generation, expiration)?
                };
                self.record = Some(record);
                Ok(())
            }
            rc => Err(CommandError::ServerError(rc)),
        }
    }
}
