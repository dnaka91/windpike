#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use crate::commands::ParseParticleError;

pub(crate) mod decoder;
pub(crate) mod encoder;

pub(crate) type Result<T, E = MsgpackError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum MsgpackError {
    #[error("Particle type not recognized")]
    UnrecognizedParticle(#[from] ParseParticleError),
    #[error("Type header with code `{0}` not recognized")]
    UnrecognizedCode(u8),
    #[error("Error unpacking value of type `{0:x}`")]
    InvalidValueType(u8),
    #[error("Buffer error")]
    Buffer(#[from] crate::commands::buffer::BufferError),
}

pub(crate) trait Write {
    fn write_u8(&mut self, v: u8) -> usize;
    fn write_u16(&mut self, v: u16) -> usize;
    fn write_u32(&mut self, v: u32) -> usize;
    fn write_u64(&mut self, v: u64) -> usize;
    fn write_i8(&mut self, v: i8) -> usize;
    fn write_i16(&mut self, v: i16) -> usize;
    fn write_i32(&mut self, v: i32) -> usize;
    fn write_i64(&mut self, v: i64) -> usize;
    fn write_f32(&mut self, v: f32) -> usize;
    fn write_f64(&mut self, v: f64) -> usize;

    fn write_bytes(&mut self, v: &[u8]) -> usize;
    fn write_str(&mut self, v: &str) -> usize;

    fn write_bool(&mut self, v: bool) -> usize;
    fn write_geo(&mut self, v: &str) -> usize;
}

pub(crate) struct Sink;

impl Write for Sink {
    fn write_u8(&mut self, _: u8) -> usize {
        std::mem::size_of::<u8>()
    }

    fn write_u16(&mut self, _: u16) -> usize {
        std::mem::size_of::<u16>()
    }

    fn write_u32(&mut self, _: u32) -> usize {
        std::mem::size_of::<u32>()
    }

    fn write_u64(&mut self, _: u64) -> usize {
        std::mem::size_of::<u64>()
    }

    fn write_i8(&mut self, _: i8) -> usize {
        std::mem::size_of::<i8>()
    }

    fn write_i16(&mut self, _: i16) -> usize {
        std::mem::size_of::<i16>()
    }

    fn write_i32(&mut self, _: i32) -> usize {
        std::mem::size_of::<i32>()
    }

    fn write_i64(&mut self, _: i64) -> usize {
        std::mem::size_of::<i64>()
    }

    fn write_f32(&mut self, _: f32) -> usize {
        std::mem::size_of::<f32>()
    }

    fn write_f64(&mut self, _: f64) -> usize {
        std::mem::size_of::<f64>()
    }

    fn write_bytes(&mut self, v: &[u8]) -> usize {
        v.len()
    }

    fn write_str(&mut self, v: &str) -> usize {
        v.as_bytes().len()
    }

    fn write_bool(&mut self, _: bool) -> usize {
        std::mem::size_of::<i64>()
    }

    fn write_geo(&mut self, v: &str) -> usize {
        3 + v.len()
    }
}
