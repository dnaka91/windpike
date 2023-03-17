use std::{collections::HashMap, io::Write, str};

use tracing::debug;

use super::{CommandError, Result};
use crate::net::Connection;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers.
const MAX_BUFFER_SIZE: usize = 1024 * 1024 + 8; // 1 MB + header

#[derive(Clone, Debug)]
pub struct Message {
    buf: Vec<u8>,
}

impl Message {
    pub async fn info(conn: &mut Connection, commands: &[&str]) -> Result<HashMap<String, String>> {
        let cmd = {
            let mut cmd = commands.join("\n");
            cmd.push('\n');
            cmd
        };
        let mut msg = Self::new(&cmd.into_bytes())?;

        msg.send(conn).await?;
        msg.parse_response()
    }

    fn new(data: &[u8]) -> Result<Self> {
        let len = data.len().to_be_bytes();
        let mut buf = Vec::with_capacity(1024);
        buf.push(2); // version
        buf.push(1); // msg_type
        buf.write_all(&len[2..8])?;
        buf.write_all(data)?;

        Ok(Self { buf })
    }

    fn data_len(&self) -> u64 {
        let mut lbuf = [0; 8];
        lbuf[2..8].clone_from_slice(&self.buf[2..8]);
        u64::from_be_bytes(lbuf)
    }

    async fn send(&mut self, conn: &mut Connection) -> Result<()> {
        conn.write(&self.buf).await?;

        // read the header
        conn.read(self.buf[..8].as_mut()).await?;

        // figure our message size and grow the buffer if necessary
        let data_len = self.data_len() as usize;

        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if data_len > MAX_BUFFER_SIZE {
            return Err(CommandError::BufferSize {
                size: data_len,
                max: MAX_BUFFER_SIZE,
            });
        }
        self.buf.resize(data_len, 0);

        // read the message content
        conn.read(self.buf.as_mut()).await?;

        Ok(())
    }

    fn parse_response(&self) -> Result<HashMap<String, String>> {
        let response = str::from_utf8(&self.buf)?;
        let response = response.trim_matches('\n');

        debug!(?response, "response from server for info command");
        let mut result: HashMap<String, String> = HashMap::new();

        for tuple in response.split('\n') {
            let mut kv = tuple.split('\t');
            let key = kv.next();
            let val = kv.next();

            match (key, val) {
                (Some(key), Some(val)) => result.insert(key.to_owned(), val.to_owned()),
                (Some(key), None) => result.insert(key.to_owned(), String::new()),
                _ => return Err(CommandError::Parse("Parsing Info command failed")),
            };
        }

        Ok(result)
    }
}
