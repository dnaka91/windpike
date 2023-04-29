#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use bytes::BufMut;

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
    #[error("Buffer error")]
    Buffer(#[from] crate::commands::buffer::BufferError),
    #[error("the marker `{0}` isn't valid for the data type")]
    InvalidMarker(u8),
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
    #[inline]
    fn write_u8(&mut self, _: u8) -> usize {
        std::mem::size_of::<u8>()
    }

    #[inline]
    fn write_u16(&mut self, _: u16) -> usize {
        std::mem::size_of::<u16>()
    }

    #[inline]
    fn write_u32(&mut self, _: u32) -> usize {
        std::mem::size_of::<u32>()
    }

    #[inline]
    fn write_u64(&mut self, _: u64) -> usize {
        std::mem::size_of::<u64>()
    }

    #[inline]
    fn write_i8(&mut self, _: i8) -> usize {
        std::mem::size_of::<i8>()
    }

    #[inline]
    fn write_i16(&mut self, _: i16) -> usize {
        std::mem::size_of::<i16>()
    }

    #[inline]
    fn write_i32(&mut self, _: i32) -> usize {
        std::mem::size_of::<i32>()
    }

    #[inline]
    fn write_i64(&mut self, _: i64) -> usize {
        std::mem::size_of::<i64>()
    }

    #[inline]
    fn write_f32(&mut self, _: f32) -> usize {
        std::mem::size_of::<f32>()
    }

    #[inline]
    fn write_f64(&mut self, _: f64) -> usize {
        std::mem::size_of::<f64>()
    }

    #[inline]
    fn write_bytes(&mut self, v: &[u8]) -> usize {
        v.len()
    }

    #[inline]
    fn write_str(&mut self, v: &str) -> usize {
        v.as_bytes().len()
    }

    #[inline]
    fn write_bool(&mut self, _: bool) -> usize {
        std::mem::size_of::<u8>()
    }

    #[inline]
    fn write_geo(&mut self, v: &str) -> usize {
        3 + v.len()
    }
}

impl<T: BufMut> Write for T {
    fn write_u8(&mut self, v: u8) -> usize {
        self.put_u8(v);
        Sink.write_u8(v)
    }

    fn write_u16(&mut self, v: u16) -> usize {
        self.put_u16(v);
        Sink.write_u16(v)
    }

    fn write_u32(&mut self, v: u32) -> usize {
        self.put_u32(v);
        Sink.write_u32(v)
    }

    fn write_u64(&mut self, v: u64) -> usize {
        self.put_u64(v);
        Sink.write_u64(v)
    }

    fn write_i8(&mut self, v: i8) -> usize {
        self.put_i8(v);
        Sink.write_i8(v)
    }

    fn write_i16(&mut self, v: i16) -> usize {
        self.put_i16(v);
        Sink.write_i16(v)
    }

    fn write_i32(&mut self, v: i32) -> usize {
        self.put_i32(v);
        Sink.write_i32(v)
    }

    fn write_i64(&mut self, v: i64) -> usize {
        self.put_i64(v);
        Sink.write_i64(v)
    }

    fn write_f32(&mut self, v: f32) -> usize {
        self.put_f32(v);
        Sink.write_f32(v)
    }

    fn write_f64(&mut self, v: f64) -> usize {
        self.put_f64(v);
        Sink.write_f64(v)
    }

    fn write_bytes(&mut self, v: &[u8]) -> usize {
        self.put_slice(v);
        Sink.write_bytes(v)
    }

    fn write_str(&mut self, v: &str) -> usize {
        self.put_slice(v.as_bytes());
        Sink.write_str(v)
    }

    fn write_bool(&mut self, v: bool) -> usize {
        self.put_u8(v.into());
        Sink.write_bool(v)
    }

    fn write_geo(&mut self, v: &str) -> usize {
        self.put_bytes(0, 3);
        self.put_slice(v.as_bytes());
        Sink.write_geo(v)
    }
}

#[derive(Clone, Copy)]
enum Marker {
    Pfix(u8),
    FixMap(u8),
    FixArray(u8),
    FixStr(u8),
    Nil,
    Reserved,
    False,
    True,
    Bin8,
    Bin16,
    Bin32,
    Ext8,
    Ext16,
    Ext32,
    F32,
    F64,
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    FixExt1,
    FixExt2,
    FixExt4,
    FixExt8,
    FixExt16,
    Str8,
    Str16,
    Str32,
    Array16,
    Array32,
    Map16,
    Map32,
    Nfix(i8),
}

impl From<u8> for Marker {
    fn from(value: u8) -> Self {
        match value {
            0x00..=0x7f => Self::Pfix(value),
            0x80..=0x8f => Self::FixMap(value & 0x0f),
            0x90..=0x9f => Self::FixArray(value & 0x0f),
            0xa0..=0xbf => Self::FixStr(value & 0x1f),
            0xc0 => Self::Nil,
            0xc1 => Self::Reserved,
            0xc2 => Self::False,
            0xc3 => Self::True,
            0xc4 => Self::Bin8,
            0xc5 => Self::Bin16,
            0xc6 => Self::Bin32,
            0xc7 => Self::Ext8,
            0xc8 => Self::Ext16,
            0xc9 => Self::Ext32,
            0xca => Self::F32,
            0xcb => Self::F64,
            0xcc => Self::U8,
            0xcd => Self::U16,
            0xce => Self::U32,
            0xcf => Self::U64,
            0xd0 => Self::I8,
            0xd1 => Self::I16,
            0xd2 => Self::I32,
            0xd3 => Self::I64,
            0xd4 => Self::FixExt1,
            0xd5 => Self::FixExt2,
            0xd6 => Self::FixExt4,
            0xd7 => Self::FixExt8,
            0xd8 => Self::FixExt16,
            0xd9 => Self::Str8,
            0xda => Self::Str16,
            0xdb => Self::Str32,
            0xdc => Self::Array16,
            0xdd => Self::Array32,
            0xde => Self::Map16,
            0xdf => Self::Map32,
            0xe0..=0xff => Self::Nfix(value as i8),
        }
    }
}

impl From<Marker> for u8 {
    fn from(value: Marker) -> Self {
        match value {
            Marker::Pfix(p) => p & 0x7f,
            Marker::FixMap(len) => 0x80 | (len & 0x0f),
            Marker::FixArray(len) => 0x90 | (len & 0x0f),
            Marker::FixStr(len) => 0xa0 | (len & 0x1f),
            Marker::Nil => 0xc0,
            Marker::Reserved => 0xc1,
            Marker::False => 0xc2,
            Marker::True => 0xc3,
            Marker::Bin8 => 0xc4,
            Marker::Bin16 => 0xc5,
            Marker::Bin32 => 0xc6,
            Marker::Ext8 => 0xc7,
            Marker::Ext16 => 0xc8,
            Marker::Ext32 => 0xc9,
            Marker::F32 => 0xca,
            Marker::F64 => 0xcb,
            Marker::U8 => 0xcc,
            Marker::U16 => 0xcd,
            Marker::U32 => 0xce,
            Marker::U64 => 0xcf,
            Marker::I8 => 0xd0,
            Marker::I16 => 0xd1,
            Marker::I32 => 0xd2,
            Marker::I64 => 0xd3,
            Marker::FixExt1 => 0xd4,
            Marker::FixExt2 => 0xd5,
            Marker::FixExt4 => 0xd6,
            Marker::FixExt8 => 0xd7,
            Marker::FixExt16 => 0xd8,
            Marker::Str8 => 0xd9,
            Marker::Str16 => 0xda,
            Marker::Str32 => 0xdb,
            Marker::Array16 => 0xdc,
            Marker::Array32 => 0xdd,
            Marker::Map16 => 0xde,
            Marker::Map32 => 0xdf,
            Marker::Nfix(p) => 0xe0 | ((p + 32) as u8),
        }
    }
}
