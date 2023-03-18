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

pub use self::particle_type::ParseParticleError;
pub(crate) use self::{
    admin_command::AdminCommand, batch_read_command::BatchReadCommand,
    delete_command::DeleteCommand, exists_command::ExistsCommand, info_command::Message,
    operate_command::OperateCommand, particle_type::ParticleType, read_command::ReadCommand,
    scan_command::ScanCommand, single_command::SingleCommand, stream_command::StreamCommand,
    touch_command::TouchCommand, write_command::WriteCommand,
};
use crate::{cluster::Node, net::Connection, ResultCode};

pub type Result<T, E = CommandError> = crate::errors::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("Failed to prepare send buffer")]
    PrepareBuffer(#[source] Box<Self>),
    #[error("Invalid size for buffer: {size} (max {max})")]
    BufferSize { size: usize, max: usize },
    #[error("Timeout")]
    Timeout,
    #[error("Server error: {}", .0.into_string())]
    ServerError(ResultCode),
    #[error("Invalid UTF-8 content ecountered")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("I/O related error")]
    Io(#[from] std::io::Error),
    #[error("Failed hashing password")]
    Hashing(#[from] bcrypt::BcryptError),
    #[error("Network error")]
    Network(#[from] crate::net::NetError),
    #[error("Buffer error")]
    Buffer(#[from] self::buffer::BufferError),
    #[error("Particle error")]
    Particle(#[from] crate::value::ParticleError),
    #[error("No connections available")]
    NoConnection,
    #[error("Parsing failed: {0}")]
    Parse(&'static str),
    #[error("Other error")]
    Other(#[source] Box<crate::errors::Error>),
}

// Command interface describes all commands available
#[async_trait::async_trait]
trait Command {
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    async fn get_node(&self) -> Option<Arc<Node>>;
    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()>;
}

#[must_use]
pub const fn keep_connection(err: &CommandError) -> bool {
    matches!(err, CommandError::ServerError(ResultCode::KeyNotFoundError))
}
