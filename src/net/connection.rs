use std::ops::Add;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, Instant},
};

use super::{NetError, Result};
use crate::{
    commands::{
        buffer::{Buffer, MessageHeader, ProtoHeader, StreamMessageHeader, TOTAL_HEADER_SIZE},
        AdminCommand,
    },
    policies::ClientPolicy,
};

#[derive(Debug)]
pub struct Connection {
    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    conn: TcpStream,
    active: bool,

    bytes_read: usize,

    buffer: Buffer,
}

impl Connection {
    pub async fn new(addr: &str, policy: &ClientPolicy) -> Result<Self> {
        let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr)).await;
        if stream.is_err() {
            return Err(NetError::FailedOpening);
        }
        let mut conn = Self {
            buffer: Buffer::new(policy.buffer_reclaim_threshold),
            bytes_read: 0,
            conn: stream.unwrap()?,
            active: true,
            idle_timeout: policy.idle_timeout,
            idle_deadline: policy.idle_timeout.map(|timeout| Instant::now() + timeout),
        };
        conn.authenticate(&policy.user_password).await?;
        conn.refresh();
        Ok(conn)
    }

    pub(super) fn active(&self) -> bool {
        self.active
    }

    pub async fn close(&mut self) {
        self.active = false;
        self.conn.shutdown().await.ok();
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.conn.write_all(self.buffer.as_ref()).await?;
        self.refresh();
        Ok(())
    }

    pub async fn read_buffer(&mut self, size: usize) -> Result<()> {
        self.buffer.resize(size)?;
        self.conn.read_exact(self.buffer.as_mut()).await?;
        self.bytes_read += size;
        self.refresh();
        Ok(())
    }

    pub async fn read_proto_header(&mut self) -> Result<ProtoHeader> {
        self.read_buffer(ProtoHeader::SIZE).await?;
        Ok(self.buffer.read_proto_header())
    }

    pub async fn read_stream_message_header(
        &mut self,
        proto: ProtoHeader,
    ) -> Result<StreamMessageHeader> {
        self.read_buffer(StreamMessageHeader::SIZE).await?;
        Ok(self.buffer.read_stream_message_header(proto))
    }

    pub async fn read_header(&mut self) -> Result<MessageHeader> {
        self.read_buffer(TOTAL_HEADER_SIZE).await?;
        Ok(self.buffer.read_header())
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to));
        };
    }

    async fn authenticate(&mut self, user_password: &Option<(String, String)>) -> Result<()> {
        if let Some((user, password)) = user_password {
            return match AdminCommand::authenticate(self, user, password).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    self.close().await;
                    Err(NetError::Authenticate(Box::new(err)))
                }
            };
        }

        Ok(())
    }

    pub fn bookmark(&mut self) {
        self.bytes_read = 0;
    }

    pub const fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    pub fn buffer(&mut self) -> &mut Buffer {
        &mut self.buffer
    }
}
