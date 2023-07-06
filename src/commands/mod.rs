mod admin_command;
mod batch_read_command;
pub(crate) mod buffer;
mod delete_command;
mod exists_command;
mod info_command;
mod operate_command;
mod particle_type;
mod read_command;
mod scan_command;
mod single_command;
mod stream_command;
mod touch_command;
mod write_command;

mod field_type;

use std::sync::Arc;

use async_trait::async_trait;

pub use self::particle_type::ParseParticleError;
pub(crate) use self::{
    admin_command::{hash_password, AdminCommand},
    batch_read_command::BatchReadCommand,
    delete_command::DeleteCommand,
    exists_command::ExistsCommand,
    info_command::Message,
    operate_command::OperateCommand,
    particle_type::ParticleType,
    read_command::ReadCommand,
    scan_command::ScanCommand,
    single_command::SingleCommand,
    stream_command::StreamCommand,
    touch_command::TouchCommand,
    write_command::WriteCommand,
};
use crate::{cluster::Node, net::Connection, ResultCode};

pub type Result<T, E = CommandError> = crate::errors::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("failed to prepare send buffer")]
    PrepareBuffer(#[source] Box<Self>),
    #[error("invalid size for buffer: {size} (max {max})")]
    BufferSize { size: usize, max: usize },
    #[error("timeout")]
    Timeout,
    #[error("server error: {}", .0.into_string())]
    ServerError(ResultCode),
    #[error("invalid UTF-8 content encountered")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("I/O related error")]
    Io(#[from] std::io::Error),
    #[error("failed hashing password")]
    Hashing(#[from] bcrypt::BcryptError),
    #[error("network error")]
    Network(#[from] crate::net::NetError),
    #[error("buffer error")]
    Buffer(#[from] self::buffer::BufferError),
    #[error("particle error")]
    Particle(#[from] crate::value::ParticleError),
    #[error("no connections available")]
    NoConnection,
    #[error("parsing failed: {0}")]
    Parse(&'static str),
    #[error("other error")]
    Other(#[source] Box<crate::errors::Error>),
}

// Command interface describes all commands available
#[async_trait]
trait Command {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    async fn get_node(&self) -> Option<Arc<Node>>;
    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()>;
}

#[must_use]
pub const fn keep_connection(err: &CommandError) -> bool {
    matches!(err, CommandError::ServerError(ResultCode::KeyNotFoundError))
}
