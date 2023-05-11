use std::{collections::HashMap, str};

use tracing::debug;

use super::{buffer::Buffer, CommandError, Result};
use crate::net::Connection;

#[derive(Debug)]
pub struct Message {}

impl Message {
    pub async fn info(conn: &mut Connection, commands: &[&str]) -> Result<HashMap<String, String>> {
        conn.buffer().set_info(commands)?;
        conn.flush().await?;

        let size = conn.read_proto_header().await?.size;
        conn.read_buffer(size).await?;

        parse_response(conn.buffer())
    }
}

fn parse_response(buf: &mut Buffer) -> Result<HashMap<String, String>> {
    let response = str::from_utf8(buf.as_ref())?;

    debug!(?response, "response from server for info command");

    response
        .lines()
        .map(|tuple| {
            let (key, value) = tuple
                .split_once('\t')
                .ok_or_else(|| CommandError::Parse("failed parsing info command"))?;

            Ok((key.to_owned(), value.to_owned()))
        })
        .collect()
}
