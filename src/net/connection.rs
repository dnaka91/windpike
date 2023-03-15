use std::ops::Add;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, Instant},
};

use super::{NetError, Result};
use crate::{
    commands::{buffer::Buffer, AdminCommand},
    policy::ClientPolicy,
};

#[derive(Debug)]
pub struct Connection {
    _timeout: Option<Duration>,

    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    conn: TcpStream,

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
            _timeout: policy.timeout,
            conn: stream.unwrap()?,
            idle_timeout: policy.idle_timeout,
            idle_deadline: policy.idle_timeout.map(|timeout| Instant::now() + timeout),
        };
        conn.authenticate(&policy.user_password).await?;
        conn.refresh();
        Ok(conn)
    }

    pub async fn close(&mut self) {
        let _s = self.conn.shutdown().await;
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.conn.write_all(&self.buffer.buffer).await?;
        self.refresh();
        Ok(())
    }

    pub async fn read_buffer(&mut self, size: usize) -> Result<()> {
        self.buffer.resize_buffer(size)?;
        self.conn.read_exact(&mut self.buffer.buffer).await?;
        self.bytes_read += size;
        self.buffer.reset_offset();
        self.refresh();
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.conn.write_all(buf).await?;
        self.refresh();
        Ok(())
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<()> {
        self.conn.read_exact(buf).await?;
        self.bytes_read += buf.len();
        self.refresh();
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        self.idle_deadline
            .map_or(false, |idle_dl| Instant::now() >= idle_dl)
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to));
        };
    }

    async fn authenticate(&mut self, user_password: &Option<(String, String)>) -> Result<()> {
        if let Some((ref user, ref password)) = *user_password {
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
